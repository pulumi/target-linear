"""HTTP client for the Linear GraphQL API.

This module owns everything network-facing: authentication, error classification,
retry, and the workspace lookup caches used to turn human-readable names
(``"Active"``, ``"someone@example.com"``) into the UUIDs Linear's mutations expect.

A single :class:`LinearClient` is created per target run and shared by every sink so
lookup caches are populated at most once.
"""

from __future__ import annotations

import logging
import random
import time
import uuid
from typing import TYPE_CHECKING, Any

import requests

from target_linear import __version__
from target_linear.mutations import (
    CUSTOMER_BY_EXTERNAL_ID_QUERY,
    CUSTOMER_STATUSES_QUERY,
    CUSTOMER_TIERS_QUERY,
    USERS_QUERY,
    build_customer_lookup_document,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

DEFAULT_API_URL = "https://api.linear.app/graphql"

#: Error codes that mean "try again later" rather than "this will never work".
RETRIABLE_CODES = frozenset(
    {"RATELIMITED", "INTERNAL_SERVER_ERROR", "SERVICE_UNAVAILABLE", "TIMEOUT"},
)

#: Error codes that mean the credentials are wrong -- fatal for the whole run.
AUTH_CODES = frozenset({"AUTHENTICATION_ERROR", "FORBIDDEN", "UNAUTHENTICATED"})

#: Upper bound on how long we will sleep waiting for a rate-limit window to reset.
MAX_RATE_LIMIT_SLEEP_SECONDS = 300.0

#: Pagination safety valve for the users lookup.
MAX_USER_PAGES = 40

#: Slow down once fewer than this many requests remain in the hourly window.
DEFAULT_RATE_LIMIT_BUFFER = 50

#: ``first:`` value used by the bulk customer lookup; mirrors the GraphQL document.
CUSTOMER_LOOKUP_PAGE_SIZE = 250

logger = logging.getLogger(__name__)


class LinearAPIError(Exception):
    """Base class for Linear API failures."""

    def __init__(
        self,
        message: str,
        *,
        errors: list[dict[str, Any]] | None = None,
        status_code: int | None = None,
    ) -> None:
        """Initialize the error.

        Args:
            message: Human-readable summary.
            errors: The raw GraphQL ``errors`` array, when there was one.
            status_code: HTTP status code, when there was one.
        """
        super().__init__(message)
        self.errors = errors or []
        self.status_code = status_code

    @property
    def failed_aliases(self) -> set[str]:
        """Aliases (``m0``, ``m1``, ...) that GraphQL attributed errors to."""
        return {
            str(error["path"][0])
            for error in self.errors
            if isinstance(error.get("path"), list) and error["path"]
        }


class LinearFatalError(LinearAPIError):
    """A request that will never succeed if retried (validation, bad input)."""


class LinearAuthError(LinearFatalError):
    """Credentials are missing, invalid, or lack permission."""


class LinearRetriableError(LinearAPIError):
    """A transient failure worth retrying (rate limit, 5xx, connection reset)."""


class LinearClient:
    """Thin, retrying GraphQL client for Linear with workspace lookup caches."""

    def __init__(self, config: Mapping[str, Any]) -> None:
        """Initialize the client.

        Args:
            config: The target's config mapping.
        """
        self.config = config
        self.api_url: str = config.get("api_url") or DEFAULT_API_URL
        self.max_retries: int = int(config.get("max_retries", 5))
        self.dry_run: bool = bool(config.get("dry_run", False))
        self.rate_limit_buffer: int = int(
            config.get("rate_limit_buffer", DEFAULT_RATE_LIMIT_BUFFER),
        )

        user_agent = config.get("user_agent") or f"target-linear/{__version__}"
        self.session = requests.Session()
        self.session.headers.update(
            {
                # Linear personal API keys go in a bare Authorization header.
                # There is deliberately no "Bearer " prefix here.
                "Authorization": str(config.get("auth_token", "")),
                "Content-Type": "application/json",
                "User-Agent": user_agent,
            },
        )

        self._statuses: dict[str, str] | None = None
        self._tiers: dict[str, str] | None = None
        self._users: dict[str, str] | None = None

    # -- Request plumbing ---------------------------------------------------------

    def execute(
        self,
        document: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL document, retrying transient failures.

        Args:
            document: The GraphQL query or mutation document.
            variables: Variable values for the document.

        Returns:
            The ``data`` object from the response.

        Raises:
            LinearAuthError: Credentials are invalid or insufficient.
            LinearFatalError: The request will never succeed as written.
            LinearRetriableError: Retries were exhausted.
            RuntimeError: A mutation was attempted while ``dry_run`` is enabled.
        """
        if self.dry_run and document.lstrip().startswith("mutation"):
            # Defense in depth: sinks are expected to short-circuit before this.
            msg = "Refusing to execute a mutation while dry_run is enabled"
            raise RuntimeError(msg)

        payload = {"query": document, "variables": variables or {}}
        last_error: LinearRetriableError | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.post(self.api_url, json=payload, timeout=60)
            except requests.RequestException as exc:
                last_error = LinearRetriableError(f"Request to Linear failed: {exc}")
            else:
                try:
                    return self._handle_response(response)
                except LinearRetriableError as exc:
                    last_error = exc

            if attempt < self.max_retries:
                delay = self._retry_delay(attempt, last_error)
                logger.warning(
                    "Linear request failed (attempt %d/%d): %s -- retrying in %.1fs",
                    attempt + 1,
                    self.max_retries + 1,
                    last_error,
                    delay,
                )
                time.sleep(delay)

        msg = f"Giving up after {self.max_retries + 1} attempts: {last_error}"
        raise LinearRetriableError(
            msg,
            errors=last_error.errors if last_error else None,
            status_code=last_error.status_code if last_error else None,
        )

    def _handle_response(self, response: requests.Response) -> dict[str, Any]:
        """Classify a Linear response and return its ``data`` payload."""
        self._last_response = response
        self._observe_rate_limit(response)

        try:
            body = response.json()
        except ValueError:
            body = {}

        errors = body.get("errors") or []

        if errors:
            raise self._classify_errors(errors, response)

        if response.status_code >= requests.codes.bad_request:
            msg = f"Linear returned HTTP {response.status_code}: {response.text[:500]}"
            if response.status_code in (
                requests.codes.unauthorized,
                requests.codes.forbidden,
            ):
                raise LinearAuthError(msg, status_code=response.status_code)
            if response.status_code >= requests.codes.internal_server_error or (
                response.status_code == requests.codes.too_many_requests
            ):
                raise LinearRetriableError(msg, status_code=response.status_code)
            raise LinearFatalError(msg, status_code=response.status_code)

        data = body.get("data")
        if data is None:
            msg = "Linear returned no data and no errors"
            raise LinearFatalError(msg, status_code=response.status_code)
        return data

    @staticmethod
    def _classify_errors(
        errors: list[dict[str, Any]],
        response: requests.Response,
    ) -> LinearAPIError:
        """Turn a GraphQL ``errors`` array into the right exception type.

        Linear does not map error kinds onto HTTP status codes: application errors
        arrive as HTTP 200 with an ``errors`` array, validation errors as HTTP 400,
        and rate limiting *also* as HTTP 400. So classification reads
        ``extensions.code`` rather than the status.
        """
        codes = {
            str((error.get("extensions") or {}).get("code", "")) for error in errors
        }
        summary = "; ".join(
            str(error.get("message", "unknown error")) for error in errors
        )[:1000]

        if codes & AUTH_CODES:
            return LinearAuthError(
                f"Linear authentication failed: {summary}",
                errors=errors,
                status_code=response.status_code,
            )
        if codes & RETRIABLE_CODES:
            return LinearRetriableError(
                f"Linear transient error: {summary}",
                errors=errors,
                status_code=response.status_code,
            )
        return LinearFatalError(
            f"Linear rejected the request: {summary}",
            errors=errors,
            status_code=response.status_code,
        )

    def _retry_delay(self, attempt: int, error: LinearRetriableError | None) -> float:
        """Compute how long to wait before the next attempt."""
        reset_delay = self._rate_limit_reset_delay()
        if (
            reset_delay is not None
            and error is not None
            and self._is_rate_limited(
                error,
            )
        ):
            return reset_delay
        # Exponential backoff with full jitter.
        return random.uniform(0, min(2**attempt, 32))  # noqa: S311

    @staticmethod
    def _is_rate_limited(error: LinearRetriableError) -> bool:
        """Whether an error was specifically a rate limit."""
        return (
            any(
                (e.get("extensions") or {}).get("code") == "RATELIMITED"
                for e in error.errors
            )
            or error.status_code == requests.codes.too_many_requests
        )

    def _observe_rate_limit(self, response: requests.Response) -> None:
        """Sleep before hitting the wall if the hourly budget is nearly spent.

        Reacting only to 429s means sprinting into the limit and then stalling for
        the remainder of the window. Slowing down while a small reserve is left
        keeps a long load moving.
        """
        raw_remaining = response.headers.get("x-ratelimit-requests-remaining")
        if not raw_remaining:
            return
        try:
            remaining = int(raw_remaining)
        except ValueError:
            return

        if remaining > self.rate_limit_buffer:
            return

        delay = self._rate_limit_reset_delay()
        if delay:
            logger.warning(
                "Only %d Linear requests remain this hour; pausing %.0fs until the "
                "window resets.",
                remaining,
                delay,
            )
            time.sleep(delay)

    def _rate_limit_reset_delay(self) -> float | None:
        """Seconds until the request rate-limit window resets, if known.

        ``x-ratelimit-requests-reset`` is a **millisecond epoch timestamp**, not a
        delta and not seconds -- converting it wrong produces either a no-op sleep or
        a multi-decade one.
        """
        response = getattr(self, "_last_response", None)
        if response is None:
            return None

        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(0.0, min(float(retry_after), MAX_RATE_LIMIT_SLEEP_SECONDS))
            except ValueError:
                pass

        raw = response.headers.get("x-ratelimit-requests-reset")
        if not raw:
            return None
        try:
            reset_at = float(raw) / 1000.0
        except ValueError:
            return None

        delay = reset_at - time.time()
        if delay <= 0:
            return None
        return min(delay, MAX_RATE_LIMIT_SLEEP_SECONDS)

    # -- Lookup caches ------------------------------------------------------------

    def resolve_status(self, value: str) -> str | None:
        """Resolve a customer status name (or UUID) to a status ID.

        Args:
            value: A status name such as ``"Active"``, or an existing UUID.

        Returns:
            The status ID, or ``None`` if no status matches.
        """
        return self._resolve(value, self._status_map)

    def resolve_tier(self, value: str) -> str | None:
        """Resolve a customer tier name (or UUID) to a tier ID.

        Args:
            value: A tier name such as ``"Enterprise"``, or an existing UUID.

        Returns:
            The tier ID, or ``None`` if no tier matches.
        """
        return self._resolve(value, self._tier_map)

    def resolve_user(self, value: str) -> str | None:
        """Resolve a user email, name, or display name (or UUID) to a user ID.

        Args:
            value: An email, name, display name, or existing UUID.

        Returns:
            The user ID, or ``None`` if no user matches.
        """
        return self._resolve(value, self._user_map)

    def _resolve(self, value: str, loader: Any) -> str | None:  # noqa: ANN401
        """Pass UUIDs through untouched, otherwise look the value up by name."""
        if not value:
            return None
        text = str(value).strip()
        if _is_uuid(text):
            return text
        return loader().get(text.casefold())

    def _status_map(self) -> dict[str, str]:
        if self._statuses is None:
            data = self.execute(CUSTOMER_STATUSES_QUERY)
            self._statuses = _name_map(data["customerStatuses"]["nodes"])
            logger.info(
                "Loaded %d customer status lookup keys from Linear",
                len(self._statuses),
            )
        return self._statuses

    def _tier_map(self) -> dict[str, str]:
        if self._tiers is None:
            data = self.execute(CUSTOMER_TIERS_QUERY)
            self._tiers = _name_map(data["customerTiers"]["nodes"])
            logger.info(
                "Loaded %d customer tier lookup keys from Linear",
                len(self._tiers),
            )
        return self._tiers

    def _user_map(self) -> dict[str, str]:
        """Build an email/displayName/name -> id map, email taking precedence."""
        if self._users is None:
            by_email: dict[str, str] = {}
            by_name: dict[str, str] = {}
            cursor: str | None = None

            for _ in range(MAX_USER_PAGES):
                data = self.execute(USERS_QUERY, {"after": cursor})
                page = data["users"]
                for node in page["nodes"]:
                    if node.get("email"):
                        by_email.setdefault(node["email"].casefold(), node["id"])
                    for key in ("displayName", "name"):
                        if node.get(key):
                            by_name.setdefault(node[key].casefold(), node["id"])
                if not page["pageInfo"]["hasNextPage"]:
                    break
                cursor = page["pageInfo"]["endCursor"]
            else:
                logger.warning(
                    "Stopped paginating users after %d pages; some users may not "
                    "resolve by name.",
                    MAX_USER_PAGES,
                )

            # Names must not shadow emails.
            self._users = {**by_name, **by_email}
            logger.info("Loaded %d user lookup keys from Linear", len(self._users))
        return self._users

    # -- Customer lookups ---------------------------------------------------------

    def lookup_customers(
        self,
        external_ids: list[str],
        domains: list[str],
    ) -> list[dict[str, Any]]:
        """Bulk-fetch customers matching any of the given external IDs or domains.

        One request covers a whole batch, which is what makes the ``merge`` domain
        mode affordable.

        Args:
            external_ids: External identifiers to match.
            domains: Domains to match.

        Returns:
            Deduplicated customer nodes with their current domains and externalIds.
        """
        unique_ids = sorted({e for e in external_ids if e})
        unique_domains = sorted({d for d in domains if d})
        if not unique_ids and not unique_domains:
            return []

        document = build_customer_lookup_document(
            by_external_ids=bool(unique_ids),
            by_domains=bool(unique_domains),
        )
        variables: dict[str, Any] = {}
        if unique_ids:
            variables["externalIds"] = unique_ids
        if unique_domains:
            variables["domains"] = unique_domains

        data = self.execute(document, variables)

        nodes: list[dict[str, Any]] = []
        seen: set[str] = set()
        for alias in ("byExternalId", "byDomain"):
            connection = data.get(alias)
            if not connection:
                continue
            page = connection["nodes"]
            if len(page) >= CUSTOMER_LOOKUP_PAGE_SIZE:
                logger.warning(
                    "Customer lookup '%s' returned the full page of %d results; "
                    "some matches may have been truncated. Lower 'batch_size'.",
                    alias,
                    CUSTOMER_LOOKUP_PAGE_SIZE,
                )
            for node in page:
                if node["id"] not in seen:
                    seen.add(node["id"])
                    nodes.append(node)
        return nodes

    def find_customer_by_external_id(self, external_id: str) -> dict[str, Any] | None:
        """Find a customer carrying the given external ID.

        Args:
            external_id: The external identifier, e.g. a Salesforce account ID.

        Returns:
            The customer node, or ``None`` if nothing matched.
        """
        data = self.execute(
            CUSTOMER_BY_EXTERNAL_ID_QUERY,
            {"externalId": external_id},
        )
        nodes = data["customers"]["nodes"]
        return nodes[0] if nodes else None


def _name_map(nodes: list[dict[str, Any]]) -> dict[str, str]:
    """Build a casefolded ``name``/``displayName`` -> id map for lookup entities.

    Linear's statuses and tiers each carry two names and they frequently differ --
    the API's ``name`` is the canonical one, while the UI renders ``displayName``,
    which is what a human building a warehouse mapping will copy. Both are accepted.

    ``name`` wins when one entity's ``name`` collides with another's
    ``displayName``, since ``name`` is canonical.

    Args:
        nodes: Nodes carrying ``id``, ``name``, and optionally ``displayName``.

    Returns:
        Lookup keys mapped to entity IDs.
    """
    by_display: dict[str, str] = {}
    by_name: dict[str, str] = {}
    for node in nodes:
        if node.get("displayName"):
            by_display.setdefault(node["displayName"].strip().casefold(), node["id"])
        if node.get("name"):
            by_name.setdefault(node["name"].strip().casefold(), node["id"])
    return {**by_display, **by_name}


def _is_uuid(value: str) -> bool:
    """Whether a string is a well-formed UUID."""
    try:
        uuid.UUID(value)
    except (ValueError, AttributeError, TypeError):
        return False
    return True
