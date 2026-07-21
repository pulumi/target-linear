"""Behavioral tests for target-linear. Fully mocked -- no network access."""

from __future__ import annotations

import io
import json
import time
from pathlib import Path
from typing import Any

import pytest
import requests
import responses

from target_linear.client import (
    DEFAULT_API_URL,
    LinearAuthError,
    LinearClient,
    LinearFatalError,
    LinearRetriableError,
    _name_map,
)
from target_linear.sinks import (
    CustomerNeedSink,
    CustomerSink,
    NoOpSink,
    normalize_keys,
    normalize_stream_name,
    resolve_entity,
)
from target_linear.target import TargetLinear, TargetLinearError
from tests.conftest import SAMPLE_CONFIG, graphql_error

# ---------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------


def singer_file_to_target(file_name: str, target: TargetLinear) -> None:
    """Feed a .singer fixture through the target, emulating a tap run."""
    path = Path(__file__).parent / "data_files" / file_name
    with path.open() as handle:
        target.listen(io.StringIO(handle.read()))


def singer_lines(stream: str, schema: dict, records: list[dict]) -> io.StringIO:
    """Build an in-memory Singer message stream."""
    lines = [
        json.dumps(
            {"type": "SCHEMA", "stream": stream, "key_properties": [], "schema": schema}
        ),
    ]
    lines.extend(
        json.dumps({"type": "RECORD", "stream": stream, "record": record})
        for record in records
    )
    return io.StringIO("\n".join(lines) + "\n")


CUSTOMER_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "external_ids": {"type": "array", "items": {"type": "string"}},
        "domains": {"type": "array", "items": {"type": "string"}},
        "status": {"type": "string"},
        "tier": {"type": "string"},
        "owner": {"type": "string"},
        "revenue": {"type": "integer"},
        "main_source_id": {"type": "string"},
    },
}


STATUSES_QUERY = "query CustomerStatuses { customerStatuses { nodes { id } } }"


def make_target(**overrides: Any) -> TargetLinear:
    """Build a target with the sample config plus overrides."""
    return TargetLinear(config={**SAMPLE_CONFIG, **overrides})


# ---------------------------------------------------------------------------------
# Stream dispatch
# ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("customers", "customers"),
        ("public-customers", "customers"),
        ("analytics.dim_customers", "customers"),
        ("PROD_CUSTOMERS", "customers"),
        ("Customer", "customers"),
        ("accounts", "customers"),
        ("customer_needs", "customer_needs"),
        ("needs", "customer_needs"),
        ("customerNeeds", "customer_needs"),
        ("page_views", None),
    ],
)
def test_stream_name_resolution(raw: str, expected: str | None) -> None:
    assert resolve_entity(raw, {}) == expected


def test_stream_entity_map_override() -> None:
    assert resolve_entity("weird_name", {"weird_name": "customers"}) == "customers"


def test_normalize_stream_name_strips_warehouse_prefixes() -> None:
    assert normalize_stream_name("analytics.stg_customers") == "customers"


def test_unknown_stream_is_skipped(linear_api: Any) -> None:
    target = make_target()
    singer_file_to_target("unknown_stream.singer", target)
    assert linear_api.calls_for("BatchCustomerUpsert") == []


def test_unknown_stream_raises_when_strict(linear_api: Any) -> None:
    target = make_target(fail_on_unknown_stream=True)
    with pytest.raises(TargetLinearError, match="does not map to a Linear entity"):
        singer_file_to_target("unknown_stream.singer", target)


def test_sink_class_selection() -> None:
    target = make_target()
    assert target.get_sink_class("customers") is CustomerSink
    assert target.get_sink_class("customer_needs") is CustomerNeedSink
    assert target.get_sink_class("page_views") is NoOpSink


# ---------------------------------------------------------------------------------
# Field normalization
# ---------------------------------------------------------------------------------


def test_normalize_keys_handles_camel_and_snake() -> None:
    normalized = normalize_keys(
        {"externalIds": ["a"], "slack_channel_id": "C1", "ownerEmail": "x@y.z"},
        {"owner_email": "owner"},
    )
    assert normalized == {
        "external_ids": ["a"],
        "slack_channel_id": "C1",
        "owner": "x@y.z",
    }


def test_normalize_keys_drops_nulls() -> None:
    assert normalize_keys({"name": "A", "tier": None}, {}) == {"name": "A"}


def test_camelcase_records_are_accepted(linear_api: Any) -> None:
    target = make_target()
    singer_file_to_target("customers_camel.singer", target)

    calls = linear_api.calls_for("BatchCustomerUpsert")
    assert len(calls) == 1
    sent = calls[0]["variables"]["input_0"]
    assert sent["name"] == "Camel Co"
    assert sent["externalId"] == "001PQ00000CAMEL0"
    assert sent["slackChannelId"] == "C0123456789"
    assert sent["logoUrl"] == "https://camel.example/logo.png"
    # ownerEmail -> owner -> resolved ownerId
    assert sent["ownerId"] == "cccccccc-cccc-cccc-cccc-cccccccccccc"
    # statusName -> status -> resolved statusId
    assert sent["statusId"] == "11111111-1111-1111-1111-111111111111"


def test_comma_separated_lists_are_split(linear_api: Any) -> None:
    """Warehouse columns often carry a delimited string rather than an array."""
    string_domains_schema = {
        "type": "object",
        "properties": {"name": {"type": "string"}, "domains": {"type": "string"}},
    }
    target = make_target()
    target.listen(
        singer_lines(
            "customers",
            string_domains_schema,
            [{"name": "CSV Co", "domains": "a.example, b.example"}],
        ),
    )
    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert sent["domains"] == ["a.example", "b.example"]


# ---------------------------------------------------------------------------------
# Batching
# ---------------------------------------------------------------------------------


def test_customers_are_batched_into_one_request(linear_api: Any) -> None:
    target = make_target()
    singer_file_to_target("customers.singer", target)

    calls = linear_api.calls_for("BatchCustomerUpsert")
    assert len(calls) == 1, "three customers should go out in a single request"
    variables = calls[0]["variables"]
    assert sorted(variables) == ["input_0", "input_1", "input_2"]
    assert "m2: customerUpsert" in calls[0]["document"]


def test_batch_size_config_is_respected(linear_api: Any) -> None:
    target = make_target(batch_size=2)
    singer_file_to_target("customers.singer", target)
    assert len(linear_api.calls_for("BatchCustomerUpsert")) == 2


def test_batch_failure_replays_record_by_record(linear_api: Any) -> None:
    """One bad record nulls the whole batch, so the batch is replayed singly.

    This is the behavior that compensates for GraphQL null-propagation over
    non-null CustomerPayload -- see AGENTS.md.
    """
    linear_api.on(
        "BatchCustomerUpsert",
        data=None,
        errors=[graphql_error("Entity not found: Customer", path=["m1"])],
    )
    # Replay: first succeeds (default), second fails, third succeeds.
    linear_api.on("BatchCustomerUpsert", data=None, errors=None, status=200)

    target = make_target(stop_on_error=False)
    with pytest.raises(TargetLinearError):
        singer_file_to_target("customers.singer", target)

    calls = linear_api.calls_for("BatchCustomerUpsert")
    # 1 failed batch + 3 single replays.
    assert len(calls) == 4
    for call in calls[1:]:
        assert sorted(call["variables"]) == ["input_0"]


def test_domain_rejection_syncs_without_domains(linear_api: Any) -> None:
    """A Linear-side domain rejection drops the domains, not the record."""
    linear_api.on(
        "BatchCustomerUpsert",
        data=None,
        errors=[graphql_error("public domain not allowed", path=["m0"])],
    )
    linear_api.on(
        "BatchCustomerUpsert",
        data=None,
        errors=[graphql_error("public domain not allowed", path=["m0"])],
    )

    target = make_target()
    singer_file_to_target("customers.singer", target)

    calls = linear_api.calls_for("BatchCustomerUpsert")
    # Failed batch, offender replay with domains, retry without, two other replays.
    assert len(calls) == 5
    offender = calls[1]["variables"]["input_0"]
    retried = calls[2]["variables"]["input_0"]
    assert offender["domains"] == ["acme.example"]
    assert "domains" not in retried
    assert retried["externalId"] == offender["externalId"]


def test_non_domain_rejection_still_fails(linear_api: Any) -> None:
    """Only domain rejections self-heal; other fatal errors still stop the run."""
    linear_api.on(
        "BatchCustomerUpsert",
        data=None,
        errors=[graphql_error("Entity not found: Customer", path=["m0"])],
    )
    linear_api.on(
        "BatchCustomerUpsert",
        data=None,
        errors=[graphql_error("Entity not found: Customer", path=["m0"])],
    )

    target = make_target()
    with pytest.raises(LinearFatalError):
        singer_file_to_target("customers.singer", target)


def test_successful_batch_is_not_replayed(linear_api: Any) -> None:
    target = make_target()
    singer_file_to_target("customers.singer", target)
    assert len(linear_api.calls_for("BatchCustomerUpsert")) == 1


def test_customer_needs_are_never_batched(linear_api: Any) -> None:
    """customerNeedCreate is not idempotent, so it must go one per request."""
    target = make_target()
    singer_file_to_target("customer_needs.singer", target)

    calls = linear_api.calls_for("BatchCustomerNeedCreate")
    assert len(calls) == 2
    for call in calls:
        assert sorted(call["variables"]) == ["input_0"]


# ---------------------------------------------------------------------------------
# Follow-up updates and the domains data-loss guard
# ---------------------------------------------------------------------------------


def test_single_external_id_needs_no_followup(linear_api: Any) -> None:
    target = make_target()
    singer_file_to_target("customers.singer", target)
    assert linear_api.calls_for("BatchCustomerUpdate") == []


def test_extra_external_ids_trigger_followup_update(linear_api: Any) -> None:
    target = make_target()
    target.listen(
        singer_lines(
            "customers",
            CUSTOMER_SCHEMA,
            [
                {
                    "name": "Multi Co",
                    "external_ids": ["SFDC-1", "NETSUITE-2"],
                    "domains": ["multi.example"],
                    "main_source_id": "SFDC-1",
                },
            ],
        ),
    )

    upsert = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert upsert["externalId"] == "SFDC-1", "first external id is the match key"

    updates = linear_api.calls_for("BatchCustomerUpdate")
    assert len(updates) == 1
    follow_up = updates[0]["variables"]["input_0"]
    # Only the ids Linear did not already echo back, so re-runs can't pile up.
    assert follow_up["externalIds"] == ["NETSUITE-2"]
    assert follow_up["mainSourceId"] == "SFDC-1"


def test_followup_update_never_sends_domains(linear_api: Any) -> None:
    """customerUpdate REPLACES domains; sending them would delete curated ones."""
    target = make_target()
    target.listen(
        singer_lines(
            "customers",
            CUSTOMER_SCHEMA,
            [
                {
                    "name": "Domain Co",
                    "external_ids": ["SFDC-1", "SFDC-2"],
                    "domains": ["only-one.example"],
                },
            ],
        ),
    )

    follow_up = linear_api.calls_for("BatchCustomerUpdate")[0]["variables"]["input_0"]
    assert "domains" not in follow_up


# ---------------------------------------------------------------------------------
# Domain sync policy
#
# Verified against the live API: Linear REPLACES the domain list on BOTH
# customerUpsert and customerUpdate -- its docs claim upsert merges, and it does
# not. Omitting the key is the only non-destructive option. These tests pin that
# down, because getting it wrong silently deletes domains.
# ---------------------------------------------------------------------------------


def _existing(name: str, external_ids: list[str], domains: list[str]) -> dict:
    """A customer node as the bulk lookup would return it."""
    return {
        "id": f"existing-{name}",
        "name": name,
        "externalIds": external_ids,
        "domains": domains,
    }


def _lookup(nodes: list[dict]) -> dict:
    return {"byExternalId": {"nodes": nodes}, "byDomain": {"nodes": nodes}}


ONE_DOMAIN_RECORD = [
    {
        "name": "Acme Corp",
        "external_ids": ["SFDC-ACME-001"],
        "domains": ["acme.example"],
    },
]

CURATED = [
    _existing("Acme Corp", ["SFDC-ACME-001"], ["acme-labs.example", "acme.example"]),
]


def test_merge_mode_preserves_domains_the_warehouse_does_not_know(
    linear_api: Any,
) -> None:
    """The motivating case: a warehouse row with one domain must not delete others."""
    linear_api.on("CustomerLookup", data=_lookup(CURATED))

    target = make_target(domain_sync_mode="merge")
    target.listen(singer_lines("customers", CUSTOMER_SCHEMA, ONE_DOMAIN_RECORD))

    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    # acme-labs.example survives even though the record never mentioned it.
    assert "domains" not in sent or set(sent["domains"]) == {
        "acme-labs.example",
        "acme.example",
    }


def test_merge_mode_adds_new_domains(linear_api: Any) -> None:
    linear_api.on("CustomerLookup", data=_lookup(CURATED))

    target = make_target(domain_sync_mode="merge")
    target.listen(
        singer_lines(
            "customers",
            CUSTOMER_SCHEMA,
            [
                {
                    "name": "Acme Corp",
                    "external_ids": ["SFDC-ACME-001"],
                    "domains": ["acme-new.example"],
                },
            ],
        ),
    )
    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert sent["domains"] == ["acme-labs.example", "acme.example", "acme-new.example"]


def test_merge_mode_omits_domains_when_already_identical(linear_api: Any) -> None:
    """No change means don't send the key at all, so the write can't touch it."""
    linear_api.on(
        "CustomerLookup",
        data=_lookup([_existing("Acme Corp", ["SFDC-ACME-001"], ["acme.example"])]),
    )
    target = make_target(domain_sync_mode="merge")
    target.listen(singer_lines("customers", CUSTOMER_SCHEMA, ONE_DOMAIN_RECORD))

    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert "domains" not in sent


def test_merge_mode_sets_domains_for_brand_new_customers(linear_api: Any) -> None:
    """Nothing exists, so nothing can be destroyed."""
    target = make_target(domain_sync_mode="merge")
    target.listen(singer_lines("customers", CUSTOMER_SCHEMA, ONE_DOMAIN_RECORD))

    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert sent["domains"] == ["acme.example"]


def test_replace_mode_makes_the_warehouse_authoritative(linear_api: Any) -> None:
    linear_api.on("CustomerLookup", data=_lookup(CURATED))

    target = make_target(domain_sync_mode="replace")
    target.listen(singer_lines("customers", CUSTOMER_SCHEMA, ONE_DOMAIN_RECORD))

    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert sent["domains"] == ["acme.example"]


def test_replace_mode_does_not_pay_for_a_lookup(linear_api: Any) -> None:
    target = make_target(domain_sync_mode="replace")
    target.listen(singer_lines("customers", CUSTOMER_SCHEMA, ONE_DOMAIN_RECORD))
    assert linear_api.calls_for("CustomerLookup") == []


def test_create_only_mode_never_touches_existing_domains(linear_api: Any) -> None:
    linear_api.on("CustomerLookup", data=_lookup(CURATED))

    target = make_target(domain_sync_mode="create_only")
    target.listen(singer_lines("customers", CUSTOMER_SCHEMA, ONE_DOMAIN_RECORD))

    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert "domains" not in sent


def test_create_only_mode_still_sets_domains_on_create(linear_api: Any) -> None:
    target = make_target(domain_sync_mode="create_only")
    target.listen(singer_lines("customers", CUSTOMER_SCHEMA, ONE_DOMAIN_RECORD))

    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert sent["domains"] == ["acme.example"]


def test_merge_matches_by_domain_when_no_external_id(linear_api: Any) -> None:
    linear_api.on("CustomerLookup", data=_lookup(CURATED))

    target = make_target(domain_sync_mode="merge")
    target.listen(
        singer_lines(
            "customers",
            CUSTOMER_SCHEMA,
            [{"name": "Acme Corp", "domains": ["acme.example"]}],
        ),
    )
    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert "domains" not in sent or set(sent["domains"]) == {
        "acme-labs.example",
        "acme.example",
    }


def test_lookup_failure_does_not_degrade_into_replace(linear_api: Any) -> None:
    """A failed merge lookup must abort, never silently fall through to replace."""
    linear_api.on(
        "CustomerLookup",
        data=None,
        errors=[graphql_error("lookup exploded", code="INPUT_ERROR")],
    )
    target = make_target(domain_sync_mode="merge", stop_on_error=False)
    with pytest.raises(LinearFatalError):
        target.listen(singer_lines("customers", CUSTOMER_SCHEMA, ONE_DOMAIN_RECORD))

    assert linear_api.calls_for("BatchCustomerUpsert") == []


def test_one_lookup_per_batch(linear_api: Any) -> None:
    """Merge costs one extra request per batch, not per record."""
    target = make_target(domain_sync_mode="merge")
    singer_file_to_target("customers.singer", target)
    assert len(linear_api.calls_for("CustomerLookup")) == 1


def test_records_without_domains_never_trigger_a_lookup(linear_api: Any) -> None:
    target = make_target(domain_sync_mode="merge")
    target.listen(
        singer_lines("customers", CUSTOMER_SCHEMA, [{"name": "No Domains Co"}]),
    )
    assert linear_api.calls_for("CustomerLookup") == []


def test_default_domain_mode_is_merge() -> None:
    prop = TargetLinear.config_jsonschema["properties"]["domain_sync_mode"]
    assert prop["default"] == "merge"


# ---------------------------------------------------------------------------------
# Reference resolution
# ---------------------------------------------------------------------------------


def test_references_are_resolved_to_uuids(linear_api: Any) -> None:
    target = make_target()
    singer_file_to_target("customers.singer", target)

    inputs = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]
    assert inputs["input_0"]["statusId"] == "11111111-1111-1111-1111-111111111111"
    assert inputs["input_0"]["tierId"] == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    assert inputs["input_0"]["ownerId"] == "cccccccc-cccc-cccc-cccc-cccccccccccc"


def test_reference_lookups_are_cached(linear_api: Any) -> None:
    target = make_target()
    singer_file_to_target("customers.singer", target)
    # Three records referencing statuses/tiers/users, one lookup query each.
    assert len(linear_api.calls_for("CustomerStatuses")) == 1
    assert len(linear_api.calls_for("CustomerTiers")) == 1
    assert len(linear_api.calls_for("Users")) == 1


def test_reference_matching_is_case_insensitive(linear_api: Any) -> None:
    target = make_target()
    target.listen(
        singer_lines(
            "customers",
            CUSTOMER_SCHEMA,
            [{"name": "Case Co", "status": "  active  ", "tier": "ENTERPRISE"}],
        ),
    )
    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert sent["statusId"] == "11111111-1111-1111-1111-111111111111"
    assert sent["tierId"] == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


def test_existing_uuids_pass_through_without_lookup(linear_api: Any) -> None:
    target = make_target()
    target.listen(
        singer_lines(
            "customers",
            CUSTOMER_SCHEMA,
            [{"name": "UUID Co", "status": "11111111-1111-1111-1111-111111111111"}],
        ),
    )
    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert sent["statusId"] == "11111111-1111-1111-1111-111111111111"
    assert linear_api.calls_for("CustomerStatuses") == []


def test_display_name_resolves_for_tier(linear_api: Any) -> None:
    """The UI shows displayName, so warehouse mappings are built from it."""
    target = make_target()
    target.listen(
        singer_lines(
            "customers",
            CUSTOMER_SCHEMA,
            [{"name": "Display Co", "tier": "Acme Growth Plan"}],
        ),
    )
    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert sent["tierId"] == "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def test_display_name_resolves_for_status(linear_api: Any) -> None:
    target = make_target()
    target.listen(
        singer_lines(
            "customers",
            CUSTOMER_SCHEMA,
            [{"name": "Display Co", "status": "Past Customer"}],
        ),
    )
    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert sent["statusId"] == "33333333-3333-3333-3333-333333333333"


def test_canonical_name_still_resolves(linear_api: Any) -> None:
    """Both spellings work; adding displayName must not break `name` lookups."""
    target = make_target()
    target.listen(
        singer_lines(
            "customers",
            CUSTOMER_SCHEMA,
            [{"name": "Canon Co", "status": "Active", "tier": "Growth"}],
        ),
    )
    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert sent["statusId"] == "11111111-1111-1111-1111-111111111111"
    assert sent["tierId"] == "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def test_name_wins_over_another_entitys_display_name() -> None:
    """`name` is canonical, so it takes priority on collision."""
    nodes = [
        {"id": "id-a", "name": "Alpha", "displayName": "Beta"},
        {"id": "id-b", "name": "Beta", "displayName": "Gamma"},
    ]
    mapping = _name_map(nodes)
    assert mapping["beta"] == "id-b", "the entity NAMED Beta wins"
    assert mapping["alpha"] == "id-a"
    assert mapping["gamma"] == "id-b"


def test_display_name_matching_is_exact_not_fuzzy(linear_api: Any) -> None:
    """Matching stays exact; a partial name must not silently resolve."""
    target = make_target()
    target.listen(
        singer_lines(
            "customers",
            CUSTOMER_SCHEMA,
            [{"name": "Fuzzy Co", "tier": "Acme Growth"}],
        ),
    )
    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert "tierId" not in sent


def test_unresolved_reference_warns_and_drops_field(linear_api: Any) -> None:
    target = make_target()
    target.listen(
        singer_lines(
            "customers",
            CUSTOMER_SCHEMA,
            [{"name": "Typo Co", "tier": "Does Not Exist"}],
        ),
    )
    sent = linear_api.calls_for("BatchCustomerUpsert")[0]["variables"]["input_0"]
    assert "tierId" not in sent
    assert sent["name"] == "Typo Co", "the record is still written"


def test_unresolved_reference_fails_when_strict(linear_api: Any) -> None:
    target = make_target(fail_on_unresolved_reference=True, stop_on_error=False)
    with pytest.raises(TargetLinearError):
        target.listen(
            singer_lines(
                "customers",
                CUSTOMER_SCHEMA,
                [{"name": "Typo Co", "tier": "Does Not Exist"}],
            ),
        )
    assert linear_api.calls_for("BatchCustomerUpsert") == []


def test_repeated_reference_misses_are_counted_once(
    linear_api: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A bad value on many rows should not produce one warning per row."""
    caplog.set_level("WARNING")
    target = make_target()
    target.listen(
        singer_lines(
            "customers",
            CUSTOMER_SCHEMA,
            [{"name": f"Co {i}", "tier": "Nope"} for i in range(5)],
        ),
    )
    miss_warnings = [
        rec for rec in caplog.records if "Could not resolve tier" in rec.getMessage()
    ]
    assert len(miss_warnings) == 1
    assert any("5 records" in rec.getMessage() for rec in caplog.records)


# ---------------------------------------------------------------------------------
# Customer needs
# ---------------------------------------------------------------------------------


def test_customer_need_uses_external_id(linear_api: Any) -> None:
    target = make_target()
    singer_file_to_target("customer_needs.singer", target)

    sent = linear_api.calls_for("BatchCustomerNeedCreate")[0]["variables"]["input_0"]
    assert sent["customerExternalId"] == "001PQ00000ACME01"
    assert sent["issueId"] == "ENG-123"
    assert sent["priority"] == 1


NEED_SCHEMA = {
    "type": "object",
    "properties": {
        "customer_id": {"type": "string"},
        "customer_external_id": {"type": "string"},
        "issue_id": {"type": "string"},
        "project_id": {"type": "string"},
    },
}


@pytest.mark.parametrize(
    "record",
    [
        pytest.param({"issue_id": "ENG-1"}, id="no-customer-ref"),
        pytest.param(
            {"customer_id": "x", "customer_external_id": "y", "issue_id": "ENG-1"},
            id="both-customer-refs",
        ),
        pytest.param({"customer_external_id": "y"}, id="no-target"),
        pytest.param(
            {"customer_external_id": "y", "issue_id": "ENG-1", "project_id": "p"},
            id="both-targets",
        ),
    ],
)
def test_customer_need_requires_exactly_one_of_each_link(
    linear_api: Any,
    record: dict,
) -> None:
    target = make_target(stop_on_error=False)
    with pytest.raises(TargetLinearError):
        target.listen(singer_lines("customer_needs", NEED_SCHEMA, [record]))
    assert linear_api.calls_for("BatchCustomerNeedCreate") == []


# ---------------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------------


def test_dry_run_issues_no_mutations(linear_api: Any) -> None:
    target = make_target(dry_run=True)
    singer_file_to_target("customers.singer", target)

    assert linear_api.calls_for("BatchCustomerUpsert") == []
    assert linear_api.calls_for("BatchCustomerUpdate") == []


def test_dry_run_still_resolves_references(linear_api: Any) -> None:
    """A dry run that stubbed reads would validate nothing but JSON encoding."""
    target = make_target(dry_run=True)
    singer_file_to_target("customers.singer", target)

    assert len(linear_api.calls_for("CustomerStatuses")) == 1
    assert len(linear_api.calls_for("Users")) == 1


def test_client_refuses_mutations_in_dry_run(linear_api: Any) -> None:
    client = LinearClient({**SAMPLE_CONFIG, "dry_run": True})
    with pytest.raises(RuntimeError, match="dry_run"):
        client.execute("mutation Foo { bar }", {})


# ---------------------------------------------------------------------------------
# Error classification and retry
# ---------------------------------------------------------------------------------


def test_graphql_errors_over_http_200_are_detected(linear_api: Any) -> None:
    """Linear returns application errors with a 200 status."""
    linear_api.on(
        "CustomerStatuses",
        data=None,
        errors=[graphql_error("nope", code="INPUT_ERROR")],
        status=200,
    )
    client = LinearClient(SAMPLE_CONFIG)
    with pytest.raises(LinearFatalError, match="nope"):
        client.execute(STATUSES_QUERY)


def test_validation_errors_are_fatal_and_not_retried(
    linear_api: Any,
    no_sleep: list[float],
) -> None:
    linear_api.on(
        "CustomerStatuses",
        data=None,
        errors=[graphql_error("bad field", code="GRAPHQL_VALIDATION_FAILED")],
        status=400,
    )
    client = LinearClient(SAMPLE_CONFIG)
    with pytest.raises(LinearFatalError):
        client.execute(STATUSES_QUERY)

    assert no_sleep == [], "validation errors must not be retried"
    assert len(linear_api.calls_for("CustomerStatuses")) == 1


def test_auth_errors_are_fatal(linear_api: Any) -> None:
    linear_api.on(
        "CustomerStatuses",
        data=None,
        errors=[graphql_error("bad token", code="AUTHENTICATION_ERROR")],
        status=400,
    )
    client = LinearClient(SAMPLE_CONFIG)
    with pytest.raises(LinearAuthError):
        client.execute(STATUSES_QUERY)


def test_rate_limit_is_retried_then_succeeds(
    linear_api: Any,
    no_sleep: list[float],
) -> None:
    """Rate limiting arrives as HTTP 400, so classification reads extensions.code."""
    reset_ms = int((time.time() + 42) * 1000)
    linear_api.on(
        "CustomerStatuses",
        data=None,
        errors=[graphql_error("slow down", code="RATELIMITED", user_error=False)],
        status=400,
        headers={"x-ratelimit-requests-reset": str(reset_ms)},
    )
    client = LinearClient(SAMPLE_CONFIG)
    data = client.execute(STATUSES_QUERY)

    assert data["customerStatuses"]["nodes"], "the retry succeeded"
    assert len(no_sleep) == 1
    # The reset header is a MILLISECOND epoch. Reading it as seconds would sleep for
    # millennia; reading it as a delta would sleep for ~56 million seconds.
    assert 30 < no_sleep[0] <= 45


def test_retries_are_exhausted_and_reported(
    linear_api: Any,
    no_sleep: list[float],
) -> None:
    for _ in range(4):
        linear_api.on(
            "CustomerStatuses",
            data=None,
            errors=[graphql_error("boom", code="INTERNAL_SERVER_ERROR")],
            status=200,
        )
    client = LinearClient({**SAMPLE_CONFIG, "max_retries": 3})
    with pytest.raises(LinearRetriableError, match="Giving up after 4 attempts"):
        client.execute(STATUSES_QUERY)
    assert len(no_sleep) == 3


def test_transport_error_without_errors_array_is_fatal() -> None:
    """Pre-GraphQL failures return a singular `error` key and no `errors` array."""
    with responses.RequestsMock() as mock:
        mock.add(
            responses.POST,
            DEFAULT_API_URL,
            json={"error": "Bad escaped character in JSON", "code": 400},
            status=400,
        )
        client = LinearClient(SAMPLE_CONFIG)
        with pytest.raises(LinearFatalError):
            client.execute("query Foo { bar }")


def test_http_401_is_an_auth_error() -> None:
    with responses.RequestsMock() as mock:
        mock.add(responses.POST, DEFAULT_API_URL, json={}, status=401)
        client = LinearClient(SAMPLE_CONFIG)
        with pytest.raises(LinearAuthError):
            client.execute("query Foo { bar }")


def test_auth_header_has_no_bearer_prefix() -> None:
    """Linear personal API keys go in a bare Authorization header."""
    client = LinearClient(SAMPLE_CONFIG)
    assert client.session.headers["Authorization"] == "lin_api_test_token"


def test_proactive_throttle_when_budget_nearly_spent(
    linear_api: Any,
    no_sleep: list[float],
) -> None:
    reset_ms = int((time.time() + 10) * 1000)
    linear_api.on(
        "CustomerStatuses",
        data={"customerStatuses": {"nodes": []}},
        headers={
            "x-ratelimit-requests-remaining": "3",
            "x-ratelimit-requests-reset": str(reset_ms),
        },
    )
    client = LinearClient(SAMPLE_CONFIG)
    client.execute(STATUSES_QUERY)
    assert len(no_sleep) == 1
    assert 0 < no_sleep[0] <= 11


# ---------------------------------------------------------------------------------
# Failure reporting
# ---------------------------------------------------------------------------------


def test_stop_on_error_aborts_immediately(linear_api: Any) -> None:
    linear_api.on(
        "BatchCustomerUpsert",
        data=None,
        errors=[graphql_error("rejected", path=["m0"])],
    )
    linear_api.on(
        "BatchCustomerUpsert",
        data=None,
        errors=[graphql_error("rejected", path=["m0"])],
    )
    target = make_target(stop_on_error=True)
    with pytest.raises(LinearFatalError):
        target.listen(
            singer_lines("customers", CUSTOMER_SCHEMA, [{"name": "Bad Co"}]),
        )


def test_failures_raise_at_end_of_run_when_not_stopping(linear_api: Any) -> None:
    """Exiting 0 after dropping records would make Meltano report a false success."""
    for _ in range(2):
        linear_api.on(
            "BatchCustomerUpsert",
            data=None,
            errors=[graphql_error("rejected", path=["m0"])],
        )
    target = make_target(stop_on_error=False)
    with pytest.raises(TargetLinearError, match="record\\(s\\) could not be written"):
        target.listen(
            singer_lines("customers", CUSTOMER_SCHEMA, [{"name": "Bad Co"}]),
        )


def test_record_without_name_is_rejected(linear_api: Any) -> None:
    target = make_target(stop_on_error=False)
    with pytest.raises(TargetLinearError):
        target.listen(
            singer_lines("customers", CUSTOMER_SCHEMA, [{"domains": ["x.example"]}]),
        )
    assert linear_api.calls_for("BatchCustomerUpsert") == []


def test_clean_run_does_not_raise(linear_api: Any) -> None:
    target = make_target()
    singer_file_to_target("customers.singer", target)


# ---------------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------------


def test_auth_token_is_marked_secret() -> None:
    prop = TargetLinear.config_jsonschema["properties"]["auth_token"]
    assert prop.get("secret") is True
    assert "auth_token" in TargetLinear.config_jsonschema["required"]


def test_target_writes_serially() -> None:
    assert make_target().max_parallelism == 1


def test_client_is_shared_across_sinks(linear_api: Any) -> None:
    target = make_target()
    first = target.client
    assert target.client is first


def test_no_network_without_records(linear_api: Any) -> None:
    """Lookup caches are lazy: a run that references nothing queries nothing."""
    target = make_target()
    target.listen(singer_lines("customers", CUSTOMER_SCHEMA, []))
    assert linear_api.calls == []


def test_default_api_url() -> None:
    assert DEFAULT_API_URL == "https://api.linear.app/graphql"


def test_session_sets_json_content_type() -> None:
    assert LinearClient(SAMPLE_CONFIG).session.headers["Content-Type"] == (
        "application/json"
    )


def test_requests_is_importable() -> None:
    assert requests.codes.too_many_requests == 429
