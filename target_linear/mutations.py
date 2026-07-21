"""GraphQL documents used by target-linear.

Mutation documents are built dynamically because a batch aliases N mutations into a
single request (``m0``, ``m1``, ...). See :mod:`target_linear.sinks` for why.
"""

from __future__ import annotations

# --- Lookup queries -------------------------------------------------------------------

# NOTE: statuses and tiers carry BOTH `name` and `displayName`, and a workspace can
# rename the displayed label independently of the canonical name. Linear's UI shows
# `displayName`, so that is what a human building a source mapping will copy. Both
# must be selected here, or those mappings silently fail to resolve.
CUSTOMER_STATUSES_QUERY = """
query CustomerStatuses {
  customerStatuses(first: 250) {
    nodes { id name displayName }
  }
}
"""

CUSTOMER_TIERS_QUERY = """
query CustomerTiers {
  customerTiers(first: 250) {
    nodes { id name displayName }
  }
}
"""

USERS_QUERY = """
query Users($after: String) {
  users(first: 250, after: $after, includeDisabled: true) {
    pageInfo { hasNextPage endCursor }
    nodes { id name displayName email }
  }
}
"""

# NOTE: ``StringArrayComparator`` exposes only every/some/length -- there is no ``in``.
# ``filter:{externalIds:{in:[...]}}`` is rejected by the API.
CUSTOMER_BY_EXTERNAL_ID_QUERY = """
query CustomerByExternalId($externalId: String!) {
  customers(filter: { externalIds: { some: { eq: $externalId } } }, first: 1) {
    nodes { id name externalIds domains }
  }
}
"""

#: Fields needed to reconcile a batch's domains against what Linear already holds.
_LOOKUP_SELECTION = "{ nodes { id name domains externalIds } }"


def build_customer_lookup_document(
    *,
    by_external_ids: bool,
    by_domains: bool,
) -> str:
    """Build a bulk lookup for the customers a batch might already match.

    Used by the ``merge`` and ``create_only`` domain modes, which need to know a
    customer's current domains before writing. Only the aliases that have values
    are included -- an empty ``in: []`` would match nothing and quietly defeat the
    lookup.

    Args:
        by_external_ids: Include the lookup keyed on external IDs.
        by_domains: Include the lookup keyed on domains.

    Returns:
        A GraphQL query document.

    Raises:
        ValueError: Neither lookup was requested.
    """
    if not (by_external_ids or by_domains):
        msg = "build_customer_lookup_document needs at least one lookup key"
        raise ValueError(msg)

    params: list[str] = []
    selections: list[str] = []
    if by_external_ids:
        params.append("$externalIds: [String!]")
        selections.append(
            "  byExternalId: customers("
            "filter: { externalIds: { some: { in: $externalIds } } }, first: 250"
            f") {_LOOKUP_SELECTION}",
        )
    if by_domains:
        params.append("$domains: [String!]")
        selections.append(
            "  byDomain: customers("
            "filter: { domains: { some: { in: $domains } } }, first: 250"
            f") {_LOOKUP_SELECTION}",
        )

    signature = ", ".join(params)
    body = "\n".join(selections)
    return f"query CustomerLookup({signature}) {{\n{body}\n}}"


# --- Mutation fragments ---------------------------------------------------------------

#: Selection set returned by customerCreate/customerUpdate/customerUpsert.
CUSTOMER_PAYLOAD_SELECTION = "{ success customer { id name externalIds domains } }"

#: Selection set returned by customerNeedCreate.
CUSTOMER_NEED_PAYLOAD_SELECTION = "{ success need { id } }"


def build_customer_upsert_document(count: int) -> str:
    """Build a document aliasing ``count`` customerUpsert mutations.

    Each mutation takes its own ``$input_N`` variable so a single request can carry a
    whole batch.

    Args:
        count: Number of aliased mutations. Must be at least 1.

    Returns:
        A GraphQL mutation document.
    """
    return _build_aliased_document(
        count=count,
        mutation="customerUpsert",
        input_type="CustomerUpsertInput!",
        selection=CUSTOMER_PAYLOAD_SELECTION,
    )


def build_customer_update_document(count: int) -> str:
    """Build a document aliasing ``count`` customerUpdate mutations.

    Args:
        count: Number of aliased mutations. Must be at least 1.

    Returns:
        A GraphQL mutation document.
    """
    signature = ", ".join(
        f"$id_{i}: String!, $input_{i}: CustomerUpdateInput!" for i in range(count)
    )
    body = "\n".join(
        f"  m{i}: customerUpdate(id: $id_{i}, input: $input_{i}) "
        f"{CUSTOMER_PAYLOAD_SELECTION}"
        for i in range(count)
    )
    return f"mutation BatchCustomerUpdate({signature}) {{\n{body}\n}}"


def build_customer_need_create_document(count: int) -> str:
    """Build a document aliasing ``count`` customerNeedCreate mutations.

    Args:
        count: Number of aliased mutations. Must be at least 1.

    Returns:
        A GraphQL mutation document.
    """
    return _build_aliased_document(
        count=count,
        mutation="customerNeedCreate",
        input_type="CustomerNeedCreateInput!",
        selection=CUSTOMER_NEED_PAYLOAD_SELECTION,
    )


def _build_aliased_document(
    *,
    count: int,
    mutation: str,
    input_type: str,
    selection: str,
) -> str:
    """Build a GraphQL document aliasing one mutation ``count`` times."""
    if count < 1:
        msg = f"count must be >= 1, got {count}"
        raise ValueError(msg)
    signature = ", ".join(f"$input_{i}: {input_type}" for i in range(count))
    body = "\n".join(
        f"  m{i}: {mutation}(input: $input_{i}) {selection}" for i in range(count)
    )
    name = f"Batch{mutation[0].upper()}{mutation[1:]}"
    return f"mutation {name}({signature}) {{\n{body}\n}}"
