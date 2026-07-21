"""Sinks that turn Singer records into Linear mutations."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from singer_sdk.sinks import BatchSink

from target_linear.client import LinearAPIError, LinearAuthError, LinearFatalError
from target_linear.mutations import (
    build_customer_need_create_document,
    build_customer_update_document,
    build_customer_upsert_document,
)

if TYPE_CHECKING:
    from target_linear.client import LinearClient

logger = logging.getLogger(__name__)

# Two-pass camel splitting. A naive `(?<!^)(?=[A-Z])` turns ALL-CAPS names such as
# Snowflake's PROD_CUSTOMERS into "p_r_o_d__c_u_s_t_o_m_e_r_s".
_CAMEL_WORD = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_TAIL = re.compile(r"([a-z0-9])([A-Z])")

#: Alternate spellings accepted for customer fields, keyed by snake_cased input.
CUSTOMER_FIELD_ALIASES = {
    "customer_name": "name",
    "external_id": "external_ids",
    "externalid": "external_ids",
    "owner_email": "owner",
    "owner_id": "owner",
    "owner_name": "owner",
    "status_id": "status",
    "status_name": "status",
    "tier_id": "tier",
    "tier_name": "tier",
    "logo": "logo_url",
}

#: Alternate spellings accepted for customer need fields.
CUSTOMER_NEED_FIELD_ALIASES = {
    "customer_externalid": "customer_external_id",
    "external_id": "customer_external_id",
    "issue": "issue_id",
    "issue_identifier": "issue_id",
    "project": "project_id",
    "description": "body",
}


def _to_snake(name: str) -> str:
    """Convert a camelCase, PascalCase, or ALL_CAPS key to snake_case."""
    spaced = _CAMEL_WORD.sub(r"\1_\2", name)
    return _CAMEL_TAIL.sub(r"\1_\2", spaced).lower()


def normalize_keys(record: dict[str, Any], aliases: dict[str, str]) -> dict[str, Any]:
    """Normalize record keys to canonical snake_case names.

    Warehouse columns are almost always snake_case while Linear's API is camelCase,
    so both are accepted.

    Args:
        record: The raw Singer record.
        aliases: Extra accepted spellings, keyed by snake_cased name.

    Returns:
        A new dict with canonical keys. Null values are dropped.
    """
    out: dict[str, Any] = {}
    for raw_key, value in record.items():
        if value is None:
            continue
        key = _to_snake(str(raw_key))
        out[aliases.get(key, key)] = value
    return out


def _as_list(value: Any) -> list[str]:  # noqa: ANN401
    """Coerce a scalar, comma-separated string, or list into a list of strings."""
    if isinstance(value, list):
        return [str(v).strip() for v in value if v is not None and str(v).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def _as_int(value: Any) -> int | None:  # noqa: ANN401
    """Coerce a numeric-ish value to int, or None if it isn't one."""
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _scalar_fields(data: dict[str, Any]) -> dict[str, Any]:
    """Coerce the plain scalar customer fields into their Linear input names."""
    out: dict[str, Any] = {}
    for source in ("revenue", "size"):
        if data.get(source) is not None:
            coerced = _as_int(data[source])
            if coerced is not None:
                out[source] = coerced
    for source, target_key in (
        ("slack_channel_id", "slackChannelId"),
        ("logo_url", "logoUrl"),
    ):
        if data.get(source):
            out[target_key] = str(data[source])
    return out


def _record_identity(record: dict[str, Any]) -> str:
    """Compact human-readable identity of a record for failure logs."""
    data = normalize_keys(record, CUSTOMER_FIELD_ALIASES)
    parts = []
    for key in (
        "name",
        "external_ids",
        "domains",
        "customer_external_id",
        "customer_id",
        "issue_id",
        "project_id",
    ):
        value = data.get(key)
        if value is None or value == "":
            continue
        if isinstance(value, list):
            rendered = ", ".join(str(v) for v in value)
        else:
            rendered = value
        parts.append(f"{key}={rendered!r}")
    return "(" + "; ".join(parts) + ")" if parts else "(unidentifiable)"


def _first_match(
    item: PreparedCustomer,
    by_external_id: dict[str, dict[str, Any]],
    by_domain: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """Find the existing customer a prepared record would upsert onto.

    External ID wins over domain, mirroring Linear's own upsert match precedence.
    """
    for external_id in item.external_ids:
        if external_id in by_external_id:
            return by_external_id[external_id]
    for domain in item.domains:
        if domain in by_domain:
            return by_domain[domain]
    return None


class RecordError(Exception):
    """A record cannot be written and should be reported as a failure."""


@dataclass
class PreparedCustomer:
    """A customer record resolved into its Linear mutation inputs.

    ``domains`` is held here rather than written straight into ``upsert_input``
    because the domain policy needs the batch's current state in Linear before it
    can decide what to send. See :meth:`CustomerSink.apply_domain_policy`.
    """

    record: dict[str, Any]
    upsert_input: dict[str, Any]
    external_ids: list[str] = field(default_factory=list)
    domains: list[str] = field(default_factory=list)
    main_source_id: str | None = None


class LinearSink(BatchSink):
    """Base sink: shared client, key normalization, reference resolution, failures."""

    #: Accepted alternate spellings for this entity's fields.
    field_aliases: ClassVar[dict[str, str]] = {}

    def __init__(self, target: Any, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        """Initialize the sink with the target's shared Linear client."""
        super().__init__(target, *args, **kwargs)
        self.client: LinearClient = target.client
        self._target = target
        self._written = 0
        self._would_write = 0

    @property
    def max_size(self) -> int:
        """Number of records accumulated before ``process_batch`` runs."""
        return int(self.config.get("batch_size", 25))

    @property
    def dry_run(self) -> bool:
        """Whether mutations should be logged instead of executed."""
        return bool(self.config.get("dry_run", False))

    # -- Reference resolution -----------------------------------------------------

    def resolve(self, kind: str, value: Any) -> str | None:  # noqa: ANN401
        """Resolve a human-readable reference to a Linear UUID.

        Args:
            kind: One of ``status``, ``tier``, or ``owner``.
            value: The name, email, or UUID to resolve.

        Returns:
            The resolved UUID, or ``None`` if it could not be resolved.

        Raises:
            RecordError: ``fail_on_unresolved_reference`` is set and lookup failed.
        """
        if value is None or str(value).strip() == "":
            return None
        text = str(value).strip()

        resolver = {
            "status": self.client.resolve_status,
            "tier": self.client.resolve_tier,
            "owner": self.client.resolve_user,
        }[kind]
        resolved = resolver(text)

        if resolved is None:
            if self.config.get("fail_on_unresolved_reference", False):
                msg = f"Could not resolve {kind} {text!r} in this Linear workspace"
                raise RecordError(msg)
            self._target.note_reference_miss(kind, text)
        return resolved

    # -- Failure accounting -------------------------------------------------------

    def fail_record(self, record: dict[str, Any], reason: object) -> None:
        """Record a per-record failure, honoring ``stop_on_error``.

        Args:
            record: The record that could not be written.
            reason: The exception or message explaining the failure.

        Raises:
            Exception: Re-raises when ``stop_on_error`` is enabled.
        """
        self._target.note_record_failure(self.stream_name, record, reason)
        identity = _record_identity(record)
        if self.config.get("stop_on_error", True):
            logger.error(
                "[%s] Failing record %s: %s", self.stream_name, identity, reason
            )
            if isinstance(reason, BaseException):
                raise reason
            raise RecordError(str(reason))
        logger.error("[%s] Skipping record %s: %s", self.stream_name, identity, reason)

    def clean_up(self) -> None:
        """Log a per-stream summary when the stream finishes."""
        super().clean_up()
        if self.dry_run:
            logger.info(
                "[%s] DRY RUN: %d records would have been written.",
                self.stream_name,
                self._would_write,
            )
        else:
            logger.info(
                "[%s] Wrote %d records to Linear.",
                self.stream_name,
                self._written,
            )

    def log_dry_run(self, operation: str, variables: dict[str, Any]) -> None:
        """Log a mutation that would have been sent."""
        self._would_write += 1
        logger.info(
            "[%s] DRY RUN %s: %s",
            self.stream_name,
            operation,
            json.dumps(variables, default=str, sort_keys=True),
        )


class CustomerSink(LinearSink):
    """Upserts customers, batching mutations and replaying on failure."""

    field_aliases: ClassVar[dict[str, str]] = CUSTOMER_FIELD_ALIASES

    def process_batch(self, context: dict) -> None:
        """Write a batch of customer records.

        Args:
            context: The batch context holding accumulated records.
        """
        prepared: list[PreparedCustomer] = []
        for record in context.get("records", []):
            try:
                prepared.append(self.prepare(record))
            except RecordError as exc:
                self.fail_record(record, exc)

        if not prepared:
            return

        self.apply_domain_policy(prepared)

        if self.dry_run:
            for item in prepared:
                self.log_dry_run("customerUpsert", item.upsert_input)
            return

        self._write_batch(prepared)

    def prepare(self, record: dict[str, Any]) -> PreparedCustomer:
        """Turn a record into a ``CustomerUpsertInput``.

        Args:
            record: The raw Singer record.

        Returns:
            The prepared customer.

        Raises:
            RecordError: The record is missing required fields.
        """
        data = normalize_keys(record, self.field_aliases)

        external_ids = _as_list(data.get("external_ids", []))
        domains = _as_list(data.get("domains", []))
        name = str(data.get("name", "")).strip()

        if not name and not data.get("id"):
            msg = "Customer records require a 'name' (or an existing 'id')"
            raise RecordError(msg)

        upsert: dict[str, Any] = {}
        if name:
            upsert["name"] = name
        if data.get("id"):
            upsert["id"] = str(data["id"])
        if external_ids:
            # CustomerUpsertInput takes a single externalId; the rest are added by a
            # follow-up customerUpdate.
            upsert["externalId"] = external_ids[0]

        upsert.update(self._resolved_fields(data))
        upsert.update(_scalar_fields(data))

        main_source_id = data.get("main_source_id")
        return PreparedCustomer(
            record=record,
            upsert_input=upsert,
            external_ids=external_ids,
            domains=domains,
            main_source_id=str(main_source_id) if main_source_id else None,
        )

    def apply_domain_policy(self, prepared: list[PreparedCustomer]) -> None:
        """Decide what to put in each record's ``domains`` field, if anything.

        Linear REPLACES the domain list on both ``customerUpsert`` and
        ``customerUpdate`` -- despite the docs claiming upsert merges, it does not.
        Omitting the key is the only way to leave existing domains alone. So the
        mode governs a genuine data-loss tradeoff:

        * ``merge`` (default) -- union what Linear has with what the warehouse
          sent. Never destroys a domain added by a human or another integration,
          but also never removes one.
        * ``replace`` -- the warehouse is the sole authority. Correct only if the
          source query holds the complete domain set for every customer it emits.
        * ``create_only`` -- set domains when creating, never touch them after.

        Args:
            prepared: The batch's prepared customers, mutated in place.
        """
        mode = self.config.get("domain_sync_mode", "merge")

        if mode == "replace":
            for item in prepared:
                if item.domains:
                    item.upsert_input["domains"] = item.domains
            return

        candidates = [item for item in prepared if item.domains]
        if not candidates:
            return

        # A failed lookup must never silently degrade into replace, so this is
        # allowed to propagate and fail the batch.
        existing = self.client.lookup_customers(
            [e for item in prepared for e in item.external_ids],
            [d for item in candidates for d in item.domains],
        )

        by_external_id: dict[str, dict[str, Any]] = {}
        by_domain: dict[str, dict[str, Any]] = {}
        for node in existing:
            for external_id in node.get("externalIds") or []:
                by_external_id.setdefault(external_id, node)
            for domain in node.get("domains") or []:
                by_domain.setdefault(domain, node)

        for item in candidates:
            match = _first_match(item, by_external_id, by_domain)

            if match is None:
                # No existing customer, so nothing can be destroyed.
                item.upsert_input["domains"] = item.domains
                continue

            if mode == "create_only":
                continue

            current = list(match.get("domains") or [])
            union = list(dict.fromkeys([*current, *item.domains]))
            if union != current:
                item.upsert_input["domains"] = union
            # Identical to what Linear already has -- omit the key so the write
            # cannot touch domains at all.

    def _resolved_fields(self, data: dict[str, Any]) -> dict[str, Any]:
        """Resolve name-or-UUID references into their Linear ID fields."""
        resolved: dict[str, Any] = {}
        for source, target_key in (
            ("status", "statusId"),
            ("tier", "tierId"),
            ("owner", "ownerId"),
        ):
            if data.get(source) is not None:
                value = self.resolve(source, data[source])
                if value:
                    resolved[target_key] = value
        return resolved

    def _write_batch(self, prepared: list[PreparedCustomer]) -> None:
        """Upsert a batch in one request, replaying per-record if anything fails.

        Linear's ``customerUpsert`` returns a NON-NULL ``CustomerPayload``, so a
        single failing alias makes GraphQL null-propagation wipe the entire ``data``
        object -- the successful siblings' IDs are lost even though they committed.
        The replay recovers exact attribution, and is safe only because
        ``customerUpsert`` is idempotent.
        """
        if len(prepared) == 1:
            self._write_single(prepared[0])
            return

        document = build_customer_upsert_document(len(prepared))
        variables = {f"input_{i}": item.upsert_input for i, item in enumerate(prepared)}

        try:
            data = self.client.execute(document, variables)
        except LinearAuthError:
            raise
        except LinearAPIError as exc:
            logger.warning(
                "[%s] Batch of %d failed (%s); replaying record-by-record to "
                "identify the offending record(s).",
                self.stream_name,
                len(prepared),
                exc,
            )
            for item in prepared:
                self._write_single(item)
            return

        for i, item in enumerate(prepared):
            self._finish(item, data.get(f"m{i}") or {})

    def _write_single(self, item: PreparedCustomer) -> None:
        """Upsert one customer in its own request."""
        try:
            data = self.client.execute(
                build_customer_upsert_document(1),
                {"input_0": item.upsert_input},
            )
        except LinearAuthError:
            raise
        except LinearAPIError as exc:
            if self._is_domain_rejection(item, exc):
                self._retry_without_domains(item, exc)
                return
            self.fail_record(item.record, exc)
            return
        self._finish(item, data.get("m0") or {})

    @staticmethod
    def _is_domain_rejection(item: PreparedCustomer, exc: LinearAPIError) -> bool:
        """Whether a fatal rejection is about this record's domain list.

        Linear validates domains server-side against rules this target cannot
        replicate ("public domain not allowed", "invalid domain", "customer
        domain already exists"), so any fatal error mentioning domains on a
        record that sent some is treated as a domain rejection.
        """
        return (
            isinstance(exc, LinearFatalError)
            and not isinstance(exc, LinearAuthError)
            and "domain" in str(exc).lower()
            and bool(item.upsert_input.get("domains") or item.domains)
        )

    def _retry_without_domains(
        self,
        item: PreparedCustomer,
        cause: LinearAPIError,
    ) -> None:
        """Rewrite a domain-rejected record without domains so it still syncs.

        The customer still upserts and matches by external id; only the domain
        list is dropped, and the drop is logged and counted like an unresolved
        reference so the run summary surfaces exactly which domains Linear
        refused.
        """
        domains = item.domains or list(item.upsert_input.get("domains") or [])
        stripped = {k: v for k, v in item.upsert_input.items() if k != "domains"}
        try:
            data = self.client.execute(
                build_customer_upsert_document(1),
                {"input_0": stripped},
            )
        except LinearAuthError:
            raise
        except LinearAPIError as exc:
            self.fail_record(item.record, exc)
            return
        logger.warning(
            "[%s] Linear refused the domains of record %s (%s); synced it "
            "without domains.",
            self.stream_name,
            _record_identity(item.record),
            cause,
        )
        self._target.note_reference_miss("domain", ", ".join(domains) or "<unknown>")
        self._finish(item, data.get("m0") or {})

    def _finish(self, item: PreparedCustomer, payload: dict[str, Any]) -> None:
        """Count the write and issue a follow-up update when one is needed."""
        self._written += 1
        customer = payload.get("customer") or {}
        customer_id = customer.get("id")
        if not customer_id:
            return

        existing = set(customer.get("externalIds") or [])
        missing = [e for e in item.external_ids if e not in existing]

        follow_up: dict[str, Any] = {}
        if missing:
            # customerUpdate APPENDS externalIds, so only send the ones Linear does
            # not already have -- otherwise re-runs would pile up duplicates.
            follow_up["externalIds"] = missing
        if item.main_source_id:
            follow_up["mainSourceId"] = item.main_source_id

        if not follow_up:
            return

        # NOTE: never put `domains` in this follow-up. Domains are decided once, by
        # apply_domain_policy, and written by the upsert. This follow-up fires only
        # for the subset of records carrying extra external IDs, so writing domains
        # here too would apply the domain policy under a second, unrelated trigger
        # condition -- and customerUpdate REPLACES the list, so getting it wrong is
        # destructive.
        try:
            self.client.execute(
                build_customer_update_document(1),
                {"id_0": customer_id, "input_0": follow_up},
            )
        except LinearAuthError:
            raise
        except LinearAPIError as exc:
            self.fail_record(item.record, exc)


class CustomerNeedSink(LinearSink):
    """Creates customer needs, linking customers to issues.

    Deliberately unbatched: ``customerNeedCreate`` has no upsert form, so the
    batch-replay strategy used for customers would create duplicates here.
    """

    field_aliases: ClassVar[dict[str, str]] = CUSTOMER_NEED_FIELD_ALIASES

    @property
    def max_size(self) -> int:
        """Always 1 -- see the class docstring."""
        return 1

    def process_batch(self, context: dict) -> None:
        """Write customer need records one at a time.

        Args:
            context: The batch context holding accumulated records.
        """
        for record in context.get("records", []):
            try:
                need_input = self.prepare(record)
            except RecordError as exc:
                self.fail_record(record, exc)
                continue

            if self.dry_run:
                self.log_dry_run("customerNeedCreate", need_input)
                continue

            try:
                self.client.execute(
                    build_customer_need_create_document(1),
                    {"input_0": need_input},
                )
            except LinearAuthError:
                raise
            except LinearAPIError as exc:
                self.fail_record(record, exc)
            else:
                self._written += 1

    def prepare(self, record: dict[str, Any]) -> dict[str, Any]:
        """Turn a record into a ``CustomerNeedCreateInput``.

        Args:
            record: The raw Singer record.

        Returns:
            The mutation input.

        Raises:
            RecordError: The record is missing or double-specifies required links.
        """
        data = normalize_keys(record, self.field_aliases)

        customer_id = data.get("customer_id")
        customer_external_id = data.get("customer_external_id")
        if bool(customer_id) == bool(customer_external_id):
            msg = (
                "Customer need records require exactly one of 'customer_id' or "
                "'customer_external_id'"
            )
            raise RecordError(msg)

        issue_id = data.get("issue_id")
        project_id = data.get("project_id")
        if bool(issue_id) == bool(project_id):
            msg = (
                "Customer need records require exactly one of 'issue_id' or "
                "'project_id'"
            )
            raise RecordError(msg)

        need: dict[str, Any] = {}
        if customer_id:
            need["customerId"] = str(customer_id)
        else:
            need["customerExternalId"] = str(customer_external_id)
        if issue_id:
            need["issueId"] = str(issue_id)
        else:
            need["projectId"] = str(project_id)

        if data.get("id"):
            need["id"] = str(data["id"])
        if data.get("body"):
            need["body"] = str(data["body"])
        if data.get("priority") is not None:
            need["priority"] = _as_int(data["priority"]) or 0
        for source, target_key in (
            ("attachment_url", "attachmentUrl"),
            ("attachment_id", "attachmentId"),
            ("comment_id", "commentId"),
        ):
            if data.get(source):
                need[target_key] = str(data[source])

        return need


class NoOpSink(LinearSink):
    """Discards records for streams this target doesn't know how to write.

    Targets routinely receive streams they don't handle, so skipping is the correct
    default -- but silent drops are how people lose data without noticing, hence the
    count and the summary line.
    """

    _discarded = 0

    @property
    def max_size(self) -> int:
        """Drain often; nothing is buffered for real work."""
        return 1000

    def process_batch(self, context: dict) -> None:
        """Count and discard.

        Args:
            context: The batch context holding accumulated records.
        """
        self._discarded += len(context.get("records", []))

    def clean_up(self) -> None:
        """Report how many records were discarded."""
        logger.warning(
            "[%s] Discarded %d records: stream does not map to a Linear entity. "
            "Map it explicitly with the 'stream_entity_map' setting if it should "
            "be written.",
            self.stream_name,
            self._discarded,
        )


#: Stream entity name -> sink class. Adding an entity is one line plus a sink.
ENTITY_SINKS: dict[str, type[LinearSink]] = {
    "customers": CustomerSink,
    "customer_needs": CustomerNeedSink,
}

#: Modeling-layer and environment prefixes stripped before matching a stream name.
#: A wrong guess is harmless -- the stream falls through to NoOpSink and can be
#: mapped explicitly with 'stream_entity_map'.
STREAM_NAME_PREFIXES: tuple[str, ...] = (
    "dim_",
    "fct_",
    "fact_",
    "stg_",
    "raw_",
    "prod_",
    "dev_",
    "staging_",
    "public_",
    "analytics_",
)

#: Accepted stream-name spellings, normalized, mapped onto entity names.
STREAM_ALIASES: dict[str, str] = {
    "customer": "customers",
    "customers": "customers",
    "account": "customers",
    "accounts": "customers",
    "customer_need": "customer_needs",
    "customer_needs": "customer_needs",
    "need": "customer_needs",
    "needs": "customer_needs",
    "customer_request": "customer_needs",
    "customer_requests": "customer_needs",
}


def normalize_stream_name(stream_name: str) -> str:
    """Reduce a tap's stream name to a bare, comparable entity name.

    Taps emit things like ``public-customers``, ``analytics.dim_customers``, and
    ``PROD_CUSTOMERS``; all should land on ``customers``.

    Args:
        stream_name: The raw Singer stream name.

    Returns:
        The normalized name.
    """
    last_segment = re.split(r"[-.]", stream_name)[-1]
    normalized = _to_snake(last_segment).strip("_")
    for prefix in STREAM_NAME_PREFIXES:
        normalized = normalized.removeprefix(prefix)
    return normalized


def resolve_entity(stream_name: str, overrides: dict[str, str]) -> str | None:
    """Map a stream name onto a known entity name.

    Args:
        stream_name: The raw Singer stream name.
        overrides: Explicit ``stream_entity_map`` config.

    Returns:
        The entity name, or ``None`` if the stream is unknown.
    """
    if stream_name in overrides:
        return overrides[stream_name]

    normalized = normalize_stream_name(stream_name)
    if normalized in overrides:
        return overrides[normalized]
    if normalized in ENTITY_SINKS:
        return normalized
    return STREAM_ALIASES.get(normalized)
