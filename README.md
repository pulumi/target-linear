# target-linear

`target-linear` is a Singer target for [Linear](https://linear.app), built with the
[Meltano Target SDK](https://sdk.meltano.com).

It loads records **into** Linear via the GraphQL API. The primary use case is syncing
customers from a data warehouse — for example, a deduplicated Salesforce account list —
into Linear's [Customers](https://linear.app/developers/managing-customers) feature,
carrying the Salesforce ID in `externalIds` so that customer requests can be linked to
issues and back again.

Supported entities:

| Entity | Stream name | Linear mutation | Idempotent |
|---|---|---|---|
| Customers | `customers` | `customerUpsert` (+ `customerUpdate` when needed) | Yes |
| Customer needs | `customer_needs` | `customerNeedCreate` | **No** — see [caveats](#caveats) |

## Installation

```bash
pipx install git+https://github.com/pulumi/target-linear.git@main
```

Requires Python 3.10+.

## Configuration

### Accepted Config Options

| Setting | Required | Default | Description |
|---|---|---|---|
| `auth_token` | Yes | — | Linear API key. Create one at **Settings → Security & access → Personal API keys**. |
| `api_url` | No | `https://api.linear.app/graphql` | Linear GraphQL endpoint. |
| `batch_size` | No | `25` | Customer mutations aliased into a single GraphQL request. |
| `domain_sync_mode` | No | `merge` | `merge`, `replace`, or `create_only`. See [Domain sync modes](#domain-sync-modes) — Linear replaces the domain list on every write, so this is a data-loss decision. |
| `dry_run` | No | `false` | Resolve references and build mutations, but never write. Logs what *would* be sent. |
| `fail_on_unresolved_reference` | No | `false` | If a `status`/`tier`/`owner` name can't be resolved: `false` warns and drops the field, `true` raises. |
| `fail_on_unknown_stream` | No | `false` | If a stream doesn't map to a known entity: `false` warns and skips, `true` raises. |
| `stop_on_error` | No | `true` | Whether a record-level API error aborts the run. |
| `stream_entity_map` | No | `{}` | Explicit stream → entity mapping, e.g. `{"dim_sfdc_accounts": "customers"}`. |
| `max_retries` | No | `5` | Retry attempts for rate-limited and transient failures. |
| `user_agent` | No | `target-linear/<version>` | Sent as the `User-Agent` header. |
| `stream_maps` | No | — | Inline stream maps. See [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| `stream_map_config` | No | — | User-defined config values for stream maps. |
| `flattening_enabled` | No | — | Enable schema flattening. |
| `flattening_max_depth` | No | — | Max flattening depth. |

A full list of supported settings is available by running `target-linear --about`.

### Configure using environment variables

This target will automatically import any environment variables within the working
directory's `.env` if the `--config=ENV` option is specified. Settings map to
`TARGET_LINEAR_<SETTING>`, e.g. `TARGET_LINEAR_AUTH_TOKEN`.

## Record format

Field names are accepted in **either** `snake_case` or `camelCase`, so warehouse
columns generally need no renaming.

The authoritative field lists live in
[`target_linear/schemas.py`](target_linear/schemas.py), and
[`tests/data_files/`](tests/data_files/) holds runnable Singer message fixtures for
each stream.

### `customers`

| Field | Notes |
|---|---|
| `name` | Required, unless `id` identifies an existing customer. |
| `external_ids` | Identifiers from your source system. |
| `main_source_id` | Primary external ID; must also appear in `external_ids`. |
| `domains` | See [Domain sync modes](#domain-sync-modes). |
| `status`, `tier` | Name or UUID. |
| `owner` | Email, name, display name, or UUID. |
| `revenue`, `size` | Integers. |
| `slack_channel_id`, `logo_url`, `id` | Passed through as-is. |

`status`, `tier`, and `owner` accept **either** a Linear UUID or a human-readable
value; the target resolves names to IDs against your workspace.

> **Statuses and tiers have two names.** Linear stores a canonical `name` plus a
> `displayName`, and a workspace can relabel the latter independently. **The Linear UI
> shows `displayName`**, so that is usually what your source mapping will contain. The
> target matches either one (exact, case-insensitive), preferring `name` if the two ever
> collide. If a value looks right in the UI but won't resolve, query `customerTiers` or
> `customerStatuses` and compare against both fields.

Matching for upsert is by, in order: `id`, the first `external_ids` entry,
`slack_channel_id`, then `domains`. A record with no match creates a new customer.

### `customer_needs`

| Field | Notes |
|---|---|
| `customer_id` *or* `customer_external_id` | Exactly one. |
| `issue_id` *or* `project_id` | Exactly one. `issue_id` takes a UUID or an issue identifier. |
| `body` | Markdown. |
| `priority` | `0` normal, `1` important. |
| `attachment_url`, `attachment_id`, `comment_id`, `id` | Optional. |

## Usage

### Executing the Target Directly

```bash
target-linear --version
target-linear --help
target-linear --about --format=markdown
```

The target reads Singer messages on stdin, so any tap can be piped into
`target-linear --config config.json`.

Set `dry_run` to `true` the first time you point it at a workspace. Reference lookups
still run, so the resolved mutations are logged and validated without anything being
written.

## Caveats

Linear's customer API has some field semantics that are easy to get wrong:

- **Domains are replaced, not merged — on every write.** Linear's documentation states
  that `customerUpsert` merges `domains`; **it does not**. Verified against the live
  API: the submitted list wholly replaces the stored one, on both `customerUpsert` and
  `customerUpdate`. Omitting the key is the only way to leave existing domains
  untouched. This is what `domain_sync_mode` exists to manage — see
  [Domain sync modes](#domain-sync-modes).
- **`externalIds` genuinely does append.** Upserting a customer under a second external
  ID adds it rather than replacing, so a customer can accumulate IDs from several source
  systems. The target only issues a follow-up `customerUpdate` when a record supplies
  multiple `external_ids` or a `main_source_id`, since `CustomerUpsertInput` accepts
  only a single `externalId` — and it sends only the IDs Linear doesn't already have, so
  re-runs can't pile up duplicates.
- **`customer_needs` are not idempotent.** `customerNeedCreate` has no upsert form, so
  re-running the same records creates duplicate needs. Sync needs incrementally.
- **Tiers are never auto-created.** Linear's `tierName` input would create a missing
  tier on the fly; this target deliberately does not use it, so a typo in your warehouse
  can't proliferate tiers. Unknown tier names are warned about and dropped (or raise, if
  `fail_on_unresolved_reference` is set).

### Domain sync modes

Because Linear replaces the domain list on every write, `domain_sync_mode` decides who
owns that field:

| Mode | Behavior | Use when |
|---|---|---|
| `merge` (default) | Reads current domains once per batch and sends the union. Never deletes a domain; also never removes one. | Other sources also write domains — people in the Linear UI, or the Intercom/Zendesk/Front integrations. |
| `replace` | Sends exactly what the record carries. The source system becomes the sole authority. | Your source query holds the *complete* domain set for every customer it emits. |
| `create_only` | Sets domains when the customer is created, never touches them again. | Linear is the authority after initial seeding. |

`merge` is the default because the failure it prevents is silent: a dropped domain
doesn't error, it just quietly stops Linear attributing inbound customer requests to
that customer, which you'd notice weeks later. It costs one extra read per batch. If
a merge lookup fails, the batch aborts rather than falling back to replace.

### Rate limits

Linear allows **2500 requests/hour** and 3,000,000 complexity points/hour per API key.
(The published docs say 5000 requests; the response headers say 2500.) The target
batches customer upserts to stay well inside this — a 5,000-record load at the default
`batch_size` of 25 is roughly 200 requests.

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry   # or: uv tool install poetry
poetry install
```

### Create and Run Tests

```bash
poetry run pytest
poetry run ruff check target_linear tests
poetry run target-linear --help
```

The test suite is **fully mocked** and never contacts the Linear API, so it runs
without credentials.

### Testing with [Meltano](https://meltano.com/)

```bash
export LINEAR_TOKEN=lin_api_...
meltano install
meltano run tap-smoke-test target-linear
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more
instructions on how to use the Meltano Singer SDK to develop your own Singer taps and
targets.
