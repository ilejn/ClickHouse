---
slug: /en/operations/external-authenticators/oauth
title: "OAuth 2.0"
---
import SelfManaged from '@site/docs/en/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

OAuth 2.0 access tokens can be used to authenticate ClickHouse users. This works in two ways:

- Existing users (defined in `users.xml` or in local access control paths) can be authenticated with access token if this user can be `IDENTIFIED WITH jwt`. 
- Use Identity Provider (IdP) as an external user directory and allow locally undefined users to be authenticated with a token if it is valid and recognized by the provider.

Though this authentication method is different from JWT authentication, it works under the same authentication method to maintain better compatibility. 

For both of these approaches a definition of `token_processors` is mandatory.

## Access Token Processors

To define an access token processor, add `token_processors` section to `config.xml`. Example:
```xml
<clickhouse>
    <token_processors>
        <azuure>
            <provider>azure</provider>
            <username_claim>claim_name</username_claim>
            <client_id>CLIENT_ID</client_id>
            <tenant_id>TENANT_ID</tenant_id>
        </azuure>
    </token_processors>
</clickhouse>
```

:::note
Different providers have different sets of parameters.
:::

**Parameters**

- `provider` -- name of identity provider. Mandatory, case-insensitive. Supported options: "Google", "Azure", "OpenID".
- `username_claim` -- name of claim (field) that will be treated as ClickHouse user name. Optional, default: "sub".
- `cache_lifetime` --  maximum lifetime of cached token (in seconds). Optional, default: 3600.
- `email_filter` -- Regex for validation of user emails. Optional parameter, only for Google IdP.
- `client_id` -- Azure AD (Entra ID) client ID. Optional parameter, used only for Azure IdP.
- `tenant_id` -- Azure AD (Entra ID) tenant ID. Optional parameter, used only for Azure IdP.
- `groups_claim` -- Name of claim (field) that contains list of groups user belongs to. This claim will be looked up in the token itself (in case token is a valid JWT, e.g. in Keycloak) or in response from `/userinfo`. Optional parameter.
- `configuration_endpoint` -- URI of `.well-known/openid-configuration`. Optional parameter, useful only for OIDC-compliant providers (e.g. Keycloak).
- `userinfo_endpoint` -- URI of userinfo endpoint. Optional parameter.
- `token_introspection_endpoint` -- URI of token introspection endpoint. Optional parameter.
  
:::note
Either `configuration_endpoint` or both `userinfo_endpoint` and `token_introspection_endpoint` shall be set. If none of them are set or all three are set, this is invalid configuration, it will not be parsed.
:::


### Tokens cache
To reduce number of requests to IdP, tokens are cached internally for no longer then `cache_lifetime` seconds.
If token expires sooner than `cache_lifetime`, then cache entry for this token will only be valid while token is valid.
If token lifetime is longer than `cache_lifetime`, cache entry for this token will be valid for `cache_lifetime`. 

## IdP as External Authenticator {#idp-external-authenticator}

Locally defined users can be authenticated with an access token. To allow this, `jwt` must be specified as user's authentication method. Example:

```xml
<clickhouse>
    <users>
        <my_user>
            <jwt>
                <allowed_processors>
                    <azuure />
                </allowed_processors>
            </jwt>
        </my_user>
    </users>
</clickhouse>
```

Inside `jwt` one or more specific access token processors names can be specified -- only those processors will be tried when authenticating. If no processors are specified, _all_ processors will be tried.

At each login attempt, ClickHouse will attempt to validate token and get user info against every defined access token provider.

When SQL-driven [Access Control and Account Management](/docs/en/guides/sre/user-management/index.md#access-control) is enabled, users that are authenticated with tokens can also be created using the [CREATE USER](/docs/en/sql-reference/statements/create/user.md#create-user-statement) statement.

Query:

```sql
CREATE USER my_user IDENTIFIED WITH jwt;
```

## Identity Provider as an External User Directory {#idp-external-user-directory}

If there is no suitable user pre-defined in ClickHouse, authentication is still possible: Identity Provider can be used as source of user information.
To allow this, add `token` section to the `users_directories` section of the `config.xml` file. 

At each login attempt, ClickHouse tries to find the user definition locally and authenticate it as usual.
If the user is not defined, ClickHouse will treat the user as externally defined and will try to validate the token and get user information from the specified processor.
If validated successfully, the user will be considered existing and authenticated. The user will be assigned roles from the list specified in the `roles` section. 
All this implies that the SQL-driven [Access Control and Account Management](/docs/en/guides/sre/user-management/index.md#access-control) is enabled and roles are created using the [CREATE ROLE](/docs/en/sql-reference/statements/create/role.md#create-role-statement) statement.

**Example**

```xml
<clickhouse>
    <user_directories>
        <token>
            <processor>processor_name</processor>
            <common_roles>
                <token_test_role_1 />
            </common_roles>
            <roles_filter></roles_filter>
        </token>
    </user_directories>
</clickhouse>
```

:::note
For now, no more than one `token` section can be defined inside `user_directories`. This _may_ change in future.
:::

**Parameters**

- `processor` — Name of one of processors defined in `token_processors` config section described above. This parameter is mandatory and cannot be empty.
- `common_roles` — Section with a list of locally defined roles that will be assigned to each user retrieved from the IdP. Optional.
- `roles_filter` — Regex string for groups filtering. Only groups matching this regex will be mapped to roles. Optional.
