"""Test configuration and the Linear API mock.

Every HTTP interaction in this suite is stubbed. Nothing here ever contacts the real
Linear API -- not opt-in, not behind an environment variable. Stub payloads mirror
real response shapes (verified ``CustomerPayload``, error ``extensions``, and
``x-ratelimit-*`` headers) so the mocks stay faithful without calling out.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from typing import Any, ClassVar

import pytest
import responses

from target_linear.client import DEFAULT_API_URL

# NOTE: singer-sdk >=0.54 registers its pytest plugin through an entry point, so
# declaring `pytest_plugins = ("singer_sdk.testing.pytest_plugin",)` here -- as older
# targets such as target-mysql do -- double-registers it and pytest aborts.

SAMPLE_CONFIG: dict[str, Any] = {
    "auth_token": "lin_api_test_token",
    "api_url": DEFAULT_API_URL,
}

_OPERATION_RE = re.compile(r"^\s*(?:query|mutation)\s+(\w+)")

STATUSES = [
    {"id": "11111111-1111-1111-1111-111111111111", "name": "Active"},
    {"id": "22222222-2222-2222-2222-222222222222", "name": "Prospect"},
    {"id": "33333333-3333-3333-3333-333333333333", "name": "Churned"},
]

TIERS = [
    {"id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "name": "Enterprise"},
    {"id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "name": "Team Growth"},
]

USERS = [
    {
        "id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
        "name": "Ada Lovelace",
        "displayName": "ada",
        "email": "ada@example.com",
    },
    {
        "id": "dddddddd-dddd-dddd-dddd-dddddddddddd",
        "name": "Grace Hopper",
        "displayName": "grace",
        "email": "grace@example.com",
    },
]


def customer_id_for(name: str) -> str:
    """Deterministic fake customer UUID so assertions can predict IDs."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"customer:{name}"))


def graphql_error(
    message: str,
    *,
    code: str = "INPUT_ERROR",
    path: list[str] | None = None,
    user_error: bool = True,
) -> dict[str, Any]:
    """Build an error entry shaped like a real Linear GraphQL error."""
    error: dict[str, Any] = {
        "message": message,
        "extensions": {
            "code": code,
            "type": "graphql error",
            "userError": user_error,
            "http": {"status": 400, "headers": {}},
        },
    }
    if path:
        error["path"] = path
    return error


class LinearRouter:
    """Routes mocked POSTs by GraphQL operation name.

    Every request goes to the same URL with the same method, so dispatch has to key
    on the parsed request body rather than the URL.
    """

    def __init__(self) -> None:
        """Set up empty queues and the default call log."""
        self.calls: list[dict[str, Any]] = []
        self._queues: dict[str, list[dict[str, Any]]] = {}

    # -- Registration -------------------------------------------------------------

    def on(
        self,
        operation: str,
        *,
        data: Any = None,
        errors: list[dict[str, Any]] | None = None,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> LinearRouter:
        """Queue one response for the given operation.

        Queued responses are consumed in order; once the queue is empty the default
        behavior for that operation resumes.
        """
        self._queues.setdefault(operation, []).append(
            {"data": data, "errors": errors, "status": status, "headers": headers},
        )
        return self

    def calls_for(self, operation: str) -> list[dict[str, Any]]:
        """All recorded calls for an operation, in order."""
        return [call for call in self.calls if call["operation"] == operation]

    # -- Dispatch -----------------------------------------------------------------

    def __call__(self, request: Any) -> tuple[int, dict[str, str], str]:
        """Handle a mocked request."""
        body = json.loads(request.body)
        document = body.get("query", "")
        variables = body.get("variables") or {}
        match = _OPERATION_RE.match(document)
        operation = match.group(1) if match else "Unknown"

        self.calls.append(
            {"operation": operation, "variables": variables, "document": document},
        )

        queued = self._queues.get(operation)
        if queued:
            spec = queued.pop(0)
            payload: dict[str, Any] = {}
            if spec["errors"] is not None:
                payload["errors"] = spec["errors"]
                payload["data"] = spec["data"]
            else:
                payload["data"] = spec["data"]
            return (
                spec["status"],
                self._headers(spec["headers"]),
                json.dumps(payload),
            )

        return (
            200,
            self._headers(None),
            json.dumps({"data": self._default(operation, variables)}),
        )

    @staticmethod
    def _headers(extra: dict[str, str] | None) -> dict[str, str]:
        """Realistic rate-limit headers; reset is a millisecond epoch."""
        headers = {
            "Content-Type": "application/json",
            "x-ratelimit-requests-limit": "2500",
            "x-ratelimit-requests-remaining": "2499",
            "x-ratelimit-requests-reset": str(int((time.time() + 3600) * 1000)),
        }
        if extra:
            headers.update(extra)
        return headers

    def _default(self, operation: str, variables: dict[str, Any]) -> Any:
        """Successful default payloads, built from the request's own variables."""
        builder = self._DEFAULTS.get(operation)
        return builder(variables) if builder else {}

    @staticmethod
    def _statuses_payload(_: dict[str, Any]) -> dict[str, Any]:
        return {"customerStatuses": {"nodes": STATUSES}}

    @staticmethod
    def _tiers_payload(_: dict[str, Any]) -> dict[str, Any]:
        return {"customerTiers": {"nodes": TIERS}}

    @staticmethod
    def _users_payload(_: dict[str, Any]) -> dict[str, Any]:
        return {
            "users": {
                "pageInfo": {"hasNextPage": False, "endCursor": None},
                "nodes": USERS,
            },
        }

    @staticmethod
    def _no_customers_payload(_: dict[str, Any]) -> dict[str, Any]:
        return {"customers": {"nodes": []}}

    @staticmethod
    def _empty_lookup_payload(variables: dict[str, Any]) -> dict[str, Any]:
        """No pre-existing customers, i.e. everything in the batch is a create."""
        payload: dict[str, Any] = {}
        if "externalIds" in variables:
            payload["byExternalId"] = {"nodes": []}
        if "domains" in variables:
            payload["byDomain"] = {"nodes": []}
        return payload

    @staticmethod
    def _need_payload(variables: dict[str, Any]) -> dict[str, Any]:
        return {
            alias: {"success": True, "need": {"id": str(uuid.uuid4())}}
            for alias in _aliases(variables, "input_")
        }

    @staticmethod
    def _upsert_payload(variables: dict[str, Any]) -> dict[str, Any]:
        """Echo each input back as a committed customer."""
        payload = {}
        for index, key in enumerate(_sorted_inputs(variables, "input_")):
            data = variables[key]
            name = data.get("name", "unnamed")
            external_ids = [data["externalId"]] if data.get("externalId") else []
            payload[f"m{index}"] = {
                "success": True,
                "customer": {
                    "id": data.get("id") or customer_id_for(name),
                    "name": name,
                    "externalIds": external_ids,
                    "domains": data.get("domains", []),
                },
            }
        return payload

    @staticmethod
    def _update_payload(variables: dict[str, Any]) -> dict[str, Any]:
        """Echo each update back as applied."""
        payload = {}
        for index, key in enumerate(_sorted_inputs(variables, "input_")):
            data = variables[key]
            payload[f"m{index}"] = {
                "success": True,
                "customer": {
                    "id": variables.get(f"id_{index}", str(uuid.uuid4())),
                    "name": data.get("name", "unnamed"),
                    "externalIds": data.get("externalIds", []),
                    "domains": data.get("domains", []),
                },
            }
        return payload

    #: Operation name -> default payload builder. Declared last so the staticmethods
    #: above are already bound.
    _DEFAULTS: ClassVar[dict[str, Any]] = {
        "CustomerStatuses": _statuses_payload,
        "CustomerTiers": _tiers_payload,
        "Users": _users_payload,
        "CustomerByExternalId": _no_customers_payload,
        "CustomerLookup": _empty_lookup_payload,
        "BatchCustomerUpsert": _upsert_payload,
        "BatchCustomerUpdate": _update_payload,
        "BatchCustomerNeedCreate": _need_payload,
    }


def _sorted_inputs(variables: dict[str, Any], prefix: str) -> list[str]:
    """Input variable keys ordered by their numeric suffix."""
    keys = [k for k in variables if k.startswith(prefix)]
    return sorted(keys, key=lambda k: int(k.removeprefix(prefix)))


def _aliases(variables: dict[str, Any], prefix: str) -> list[str]:
    """Alias names (``m0``, ``m1``, ...) implied by the input variables."""
    return [f"m{i}" for i, _ in enumerate(_sorted_inputs(variables, prefix))]


@pytest.fixture
def linear_api() -> Any:
    """A mocked Linear API that routes on GraphQL operation name."""
    router = LinearRouter()
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        mock.add_callback(
            responses.POST,
            DEFAULT_API_URL,
            callback=router,
            content_type="application/json",
        )
        yield router


@pytest.fixture
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Capture retry sleeps instead of actually waiting."""
    slept: list[float] = []

    def _sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr("target_linear.client.time.sleep", _sleep)
    return slept
