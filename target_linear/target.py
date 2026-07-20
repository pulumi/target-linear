"""Singer target for Linear."""

from __future__ import annotations

import logging
from collections import Counter
from typing import IO, TYPE_CHECKING, Any

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_linear import __version__
from target_linear.client import DEFAULT_API_URL, LinearClient
from target_linear.sinks import ENTITY_SINKS, LinearSink, NoOpSink, resolve_entity

if TYPE_CHECKING:
    from singer_sdk.sinks import Sink

logger = logging.getLogger(__name__)


class TargetLinearError(Exception):
    """Raised at end of run when one or more records could not be written."""


class TargetLinear(Target):
    """Loads records into Linear via its GraphQL API."""

    name = "target-linear"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,
            description=(
                "Linear API key. Sent as a raw Authorization header with no "
                "'Bearer' prefix, per Linear's personal API key scheme."
            ),
        ),
        th.Property(
            "api_url",
            th.StringType,
            default=DEFAULT_API_URL,
            description="The Linear GraphQL endpoint.",
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            default=25,
            description=(
                "Customer mutations aliased into a single GraphQL request. Linear "
                "allows 2500 requests/hour, so batching matters on large loads."
            ),
        ),
        th.Property(
            "dry_run",
            th.BooleanType,
            default=False,
            description=(
                "Resolve references and build mutations, but never write. Read "
                "queries still execute, so reference resolution is genuinely "
                "validated against the workspace."
            ),
        ),
        th.Property(
            "domain_sync_mode",
            th.StringType,
            default="merge",
            allowed_values=["merge", "replace", "create_only"],
            description=(
                "How to reconcile a record's domains with what Linear already "
                "holds. Linear REPLACES the domain list on every write, so this "
                "governs a real data-loss tradeoff. 'merge' (default) unions both "
                "sets and never destroys a domain added by a human or another "
                "integration. 'replace' makes the source data the sole authority. "
                "'create_only' sets domains on creation and never touches them "
                "again."
            ),
        ),
        th.Property(
            "fail_on_unresolved_reference",
            th.BooleanType,
            default=False,
            description=(
                "When a status/tier/owner name cannot be resolved: false warns and "
                "drops the field, true fails the record."
            ),
        ),
        th.Property(
            "fail_on_unknown_stream",
            th.BooleanType,
            default=False,
            description=(
                "When a stream does not map to a Linear entity: false warns and "
                "skips, true raises."
            ),
        ),
        th.Property(
            "stop_on_error",
            th.BooleanType,
            default=True,
            description=(
                "Whether a record-level API error aborts the run immediately. When "
                "false, failures are collected and raised at the end of the run."
            ),
        ),
        th.Property(
            "stream_entity_map",
            th.ObjectType(additional_properties=th.StringType),
            default={},
            description=(
                'Explicit stream to entity mapping, e.g. {"dim_sfdc_accounts": '
                '"customers"}.'
            ),
        ),
        th.Property(
            "max_retries",
            th.IntegerType,
            default=5,
            description="Retry attempts for rate-limited and transient failures.",
        ),
        th.Property(
            "rate_limit_buffer",
            th.IntegerType,
            default=50,
            description=(
                "Pause until the rate-limit window resets once fewer than this many "
                "requests remain in the hour."
            ),
        ),
        th.Property(
            "user_agent",
            th.StringType,
            default=f"target-linear/{__version__}",
            description="User-Agent header sent with every request.",
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType,
            default=False,
            description=(
                "Add _sdc_* metadata fields to records. Off by default: Linear has "
                "nowhere to put them."
            ),
        ),
    ).to_dict()

    default_sink_class = NoOpSink

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        """Initialize the target."""
        super().__init__(*args, **kwargs)
        self._client: LinearClient | None = None
        self._reference_misses: Counter[tuple[str, str]] = Counter()
        self._record_failures: list[dict[str, Any]] = []

    @property
    def max_parallelism(self) -> int:
        """Write serially.

        Parallel sinks would race for a shared hourly rate-limit budget, making
        backoff accounting incoherent.
        """
        return 1

    @property
    def client(self) -> LinearClient:
        """The shared Linear client, created on first use.

        Sharing one instance across sinks means one HTTP session, one rate-limit
        view, and one set of lookup caches.
        """
        if self._client is None:
            self._client = LinearClient(self.config)
        return self._client

    def get_sink_class(self, stream_name: str) -> type[Sink]:
        """Map a stream name to the sink that writes it.

        Args:
            stream_name: The Singer stream name.

        Returns:
            The sink class to use.

        Raises:
            TargetLinearError: The stream is unknown and ``fail_on_unknown_stream``
                is enabled.
        """
        overrides = self.config.get("stream_entity_map") or {}
        entity = resolve_entity(stream_name, dict(overrides))

        if entity and entity in ENTITY_SINKS:
            return ENTITY_SINKS[entity]

        if self.config.get("fail_on_unknown_stream", False):
            msg = (
                f"Stream {stream_name!r} does not map to a Linear entity. Known "
                f"entities: {sorted(ENTITY_SINKS)}. Map it with 'stream_entity_map'."
            )
            raise TargetLinearError(msg)

        return NoOpSink

    # -- Run-level accounting -----------------------------------------------------

    def note_reference_miss(self, kind: str, value: str) -> None:
        """Record an unresolvable reference.

        Counted rather than logged per occurrence: a bad owner email on 5,000 rows
        should produce one summary line, not 5,000 warnings.

        Args:
            kind: One of ``status``, ``tier``, or ``owner``.
            value: The value that could not be resolved.
        """
        key = (kind, value)
        if key not in self._reference_misses:
            logger.warning(
                "Could not resolve %s %r in this Linear workspace; dropping the "
                "field. Further occurrences will be counted, not logged.",
                kind,
                value,
            )
        self._reference_misses[key] += 1

    def note_record_failure(
        self,
        stream_name: str,
        record: dict[str, Any],
        reason: object,
    ) -> None:
        """Record a per-record write failure.

        Args:
            stream_name: The stream the record came from.
            record: The record that failed.
            reason: The exception or message explaining the failure.
        """
        self._record_failures.append(
            {"stream": stream_name, "record": record, "reason": str(reason)},
        )

    def listen(self, file_input: IO[str] | None = None) -> None:
        """Read the Singer stream, then report on what happened.

        Args:
            file_input: Input stream; defaults to stdin.

        Raises:
            TargetLinearError: One or more records could not be written.
        """
        super().listen(file_input)
        self._report_run()

    def _report_run(self) -> None:
        """Emit the end-of-run summary and fail if any record failed."""
        if self._reference_misses:
            summary = ", ".join(
                f"{kind}={value!r} ({count} record{'s' if count != 1 else ''})"
                for (kind, value), count in self._reference_misses.most_common()
            )
            logger.warning(
                "%d unresolved reference value(s): %s",
                len(self._reference_misses),
                summary,
            )

        if not self._record_failures:
            return

        for failure in self._record_failures[:20]:
            logger.error(
                "Failed record on stream %s: %s",
                failure["stream"],
                failure["reason"],
            )

        # Exiting 0 after dropping records would make Meltano report a partially
        # failed load as a success.
        msg = (
            f"{len(self._record_failures)} record(s) could not be written to Linear. "
            "See the errors above."
        )
        raise TargetLinearError(msg)


__all__ = ["LinearSink", "TargetLinear", "TargetLinearError"]
