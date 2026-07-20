"""Record schemas for the entities target-linear can write.

These are permissive on purpose: a target should accept whatever the warehouse
produces and fail on the parts it genuinely cannot use, rather than rejecting rows
for carrying extra columns.
"""

from __future__ import annotations

from singer_sdk import typing as th

CUSTOMER_SCHEMA = th.PropertiesList(
    th.Property("id", th.StringType, description="Linear customer UUID, if known."),
    th.Property("name", th.StringType, description="Customer display name."),
    th.Property(
        "external_ids",
        th.ArrayType(th.StringType),
        description="External identifiers, e.g. Salesforce account IDs.",
    ),
    th.Property(
        "main_source_id",
        th.StringType,
        description="Primary external ID; must also appear in external_ids.",
    ),
    th.Property("domains", th.ArrayType(th.StringType)),
    th.Property("status", th.StringType, description="Status name or UUID."),
    th.Property("tier", th.StringType, description="Tier name or UUID."),
    th.Property("owner", th.StringType, description="Owner email, name, or UUID."),
    th.Property("revenue", th.IntegerType),
    th.Property("size", th.IntegerType),
    th.Property("slack_channel_id", th.StringType),
    th.Property("logo_url", th.StringType),
).to_dict()

CUSTOMER_NEED_SCHEMA = th.PropertiesList(
    th.Property("id", th.StringType),
    th.Property("customer_id", th.StringType),
    th.Property("customer_external_id", th.StringType),
    th.Property(
        "issue_id",
        th.StringType,
        description="Issue UUID or identifier such as ENG-123.",
    ),
    th.Property("project_id", th.StringType),
    th.Property("body", th.StringType, description="Markdown body."),
    th.Property("priority", th.NumberType, description="0 = normal, 1 = important."),
    th.Property("attachment_url", th.StringType),
    th.Property("attachment_id", th.StringType),
    th.Property("comment_id", th.StringType),
).to_dict()
