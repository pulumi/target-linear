# AGENTS.md

This file provides guidance to coding agents (Claude Code, etc.) when working with code
in this repository. `CLAUDE.md` is a symlink to this file.

## Project Overview

`target-linear` is a Singer target that writes records **into** Linear via its GraphQL
API. It receives Singer SCHEMA/RECORD/STATE messages and turns them into Linear
mutations.

The driving use case is a Salesforce → warehouse → Linear customer sync: a deduplicated
account list is loaded into Linear with the Salesforce ID in `externalIds`, so customer
requests ("needs") can be linked to issues.

## Development Commands

### Setup

```bash
uv tool install poetry   # pipx also works
poetry install
```

### Testing

```bash
poetry run pytest
poetry run pytest tests/test_core.py::test_name
```

The suite is **fully mocked** — see [Testing](#testing) below.

### Linting and Formatting

```bash
poetry run ruff check target_linear tests
poetry run ruff format target_linear tests
poetry run mypy target_linear
```

### Running the Target

```bash
poetry run target-linear --help
poetry run target-linear --about --format=markdown
cat tests/data_files/customers.singer | poetry run target-linear --config .secrets/config.json
```

## Architecture

**`target_linear/target.py` (`TargetLinear`)**
- Entry point. Declares `config_jsonschema` and owns the single shared `LinearClient`.
- Maps an incoming stream name to a sink via `ENTITY_SINKS`: explicit `stream_entity_map`
  config first, then a normalized name match (lowercase, strip non-alphanumerics,
  singular/plural). No match → warn and skip (or raise if `fail_on_unknown_stream`).

**`target_linear/client.py` (`LinearClient`)**
- All HTTP. Auth, error classification, retry, and the workspace lookup caches.
- One instance per run, shared by every sink so caches are populated at most once.
- Lookup caches (`customerStatuses`, `customerTiers`, `users`) are lazy — a run that
  never references a tier never queries tiers.

**`target_linear/sinks.py`**
- `LinearSink` — base: field normalization, reference resolution, error policy.
- `CustomerSink` — `BatchSink`. Aliases N `customerUpsert` mutations per request, with a
  per-record replay fallback (see below). `apply_domain_policy()` decides the `domains`
  field for the whole batch before any write, using one bulk lookup; domains are
  deliberately written in exactly one place.
- `CustomerNeedSink` — `max_size = 1`, deliberately unbatched.

**`target_linear/mutations.py`** — GraphQL document constants.
**`target_linear/schemas.py`** — per-entity record schemas (`th.PropertiesList`).

## Linear API gotchas

These were verified empirically against the live API and are the reason several design
choices look the way they do. Do not "simplify" them away.

1. **Auth is a raw `Authorization: <api_key>` header — no `Bearer` prefix.**
2. **Rate limit is 2500 req/hr**, not the 5000 the docs claim (`x-ratelimit-requests-limit`).
3. **`x-ratelimit-*-reset` is a millisecond epoch timestamp**, not a delta and not seconds.
4. **Errors do not map to HTTP status.** Application errors come back as **HTTP 200 with a
   top-level `errors` array**. Validation errors come back as **HTTP 400**. Rate limiting
   *also* surfaces as 400 on GraphQL requests. Always classify on `extensions.code`, never
   on status alone.
5. **The customers filter is `filter:{externalIds:{some:{eq:$id}}}`.** `StringArrayComparator`
   has only `every`/`some`/`length` — there is no `in`.
6. **Aliased-batch responses are all-or-nothing.** `customerUpsert` returns a *non-null*
   `CustomerPayload`, so if one alias errors, GraphQL null-propagation nulls the **entire**
   `data` object. You keep `errors[].path` (which alias failed) but lose every sibling's
   returned data. This is why `CustomerSink` replays the whole batch one-record-per-request
   on any error — safe only because `customerUpsert` is idempotent.
7. **`customerNeedCreate` is not idempotent**, which is why needs are never batched (the
   replay fallback would duplicate them).
8. **Domains are REPLACED on every write, including by `customerUpsert`.** Linear's
   docs claim upsert merges domains. **They are wrong.** Verified live: a customer with
   `[a, b]` upserted with `[a]` becomes `[a]`; upserted with `[c]` it becomes `[c]`.
   `customerUpdate` replaces too. **Omitting the key is the only non-destructive
   option.** This is why `apply_domain_policy` exists and why `merge` is the default —
   see `domain_sync_mode`. Do not "simplify" by writing `domains` straight into the
   upsert input.
9. **`externalIds` genuinely appends** on both upsert and update (verified). So the
   follow-up update sends only the IDs Linear doesn't already have, or re-runs would
   accumulate duplicates.
10. **Never use `tierName`.** It auto-creates missing tiers, which conflicts with the
    warn-and-drop policy for unresolved references.
11. **A failed merge lookup must abort, never fall back to `replace`.** Silently
    degrading from "preserve domains" to "overwrite domains" on a transient read
    failure would be the worst possible failure mode.

## Testing

**The test suite never contacts the live Linear API — not even opt-in, not even behind an
env var.** All HTTP is stubbed with `responses`. Do not add integration tests that hit the
real API; if end-to-end verification is needed, it is a manual step a human runs
deliberately.

Stub payloads mirror real response shapes (verified `CustomerPayload`, error `extensions`,
`x-ratelimit-*` headers) so mocks stay faithful without calling out.

Test data lives in `tests/data_files/*.singer` and is fed through `target.listen()` via the
`singer_file_to_target()` helper in `tests/test_core.py`.

## Python Compatibility

Python 3.13+ only. This is a deliberate pin to the interpreter we run on rather than a
technical constraint — nothing in the codebase is version-specific.
