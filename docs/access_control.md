# Access control and tagging

## Current authorization boundary

Interactive access is already limited by SCIM group membership. The
`BLOBFORGE_SERVER_ROLE_GROUPS` JSON object maps exact SCIM group display names
to `viewer`, `operator`, or `admin`. An OIDC identity is rejected unless its
SCIM user is active and belongs to at least one mapped group.

To make one group the only interactive management group, configure for example:

```dotenv
BLOBFORGE_SERVER_ROLE_GROUPS={"BlobForge Administrators":"admin"}
```

Gandalf should also bind the Authentik application to that same group. This
gives two gates: Authentik refuses the OIDC flow for other users, and BlobForge
independently refuses users absent from the provisioned SCIM group. Management
routes must require the `admin` role; ingest and conversion requests require
`operator`; read-only routes require `viewer`.

The current static client token is intentionally an unrestricted automation
credential and therefore bypasses SCIM. It should be replaced before broader
multi-user use with revocable service-account tokens carrying explicit scopes.
Worker tokens are machine identities, not user identities. The compatibility
API currently permits them on some read routes; the management/API hardening
phase should restrict them to registration, claim, lease, and transfer calls.

## Tags are not ACLs

Jobs already store free-form tags, and the legacy migration assigns
`legacy-import` to every imported source plus `metadata-unavailable` to the 431
raw-only sources. These tags are descriptive metadata. They are not normalized,
reserved, or checked by authorization and must not be used as a security
boundary.

The recommended model separates discovery from authority:

- **Tags** are many-to-many labels for filtering and workflows, with normalized
  names, optional namespaces, descriptions, and audit history.
- **Collections** group sources and their artifacts into an authorization
  boundary. Artifacts inherit access from their source/collection so a new
  conversion cannot accidentally become more public than its input.
- **Policy bindings** grant a SCIM group or service account a role on a
  collection. Global administrators remain separate from collection roles.
- **Visibility** is an explicit collection property (`private`, `authenticated`,
  or deliberately `public`), never inferred from a tag.

A fitting first schema is `tags`, `source_tags`, `collections`,
`collection_sources`, and `collection_group_roles`, with foreign keys and audit
records. A source may be in multiple collections only if effective access is
defined as the union of grants and deletion/retention rules are explicit.
Until that policy is approved, default every migrated source to one private
administrative collection and keep all API-token access trusted-operator only.

## Required follow-up

Before enabling multi-user management:

1. add scoped, revocable service-account tokens and narrow worker tokens;
2. add collection-aware authorization to every source, job, artifact, transfer,
   search, and status query;
3. normalize tag CRUD and filtering independently of policy evaluation;
4. add deny-by-default tests proving list/count/search endpoints do not leak
   inaccessible object existence;
5. audit every authorization decision with principal, role, collection,
   operation, and outcome.

