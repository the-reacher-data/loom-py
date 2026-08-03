# 🚀 Release 0.18.1 ([#103](https://github.com/the-reacher-data/loom-py/pull/103)) ([`c15f1b5`](https://github.com/the-reacher-data/loom-py/commit/c15f1b55439a272d09ecfbd17a78bfa353f29d1b))



## 🐛 Fixes
### tests
- **tests:** assert what the tautological tests meant to assert<br>
  > Six assertions compared an expression with itself, so they verified


### lineage,tests
- **lineage,tests:** drop the two defects the cleanup introduced<br>
  > for_process built its copy with dataclasses.replace, which analysers
  > still model as returning an untyped dataclass; constructing the context
  > explicitly states the type the signature promises.
  > The determinism test compared two textually identical expressions again,
  > which is the very shape it was meant to leave behind.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  > --------
  > Co-authored-by: Claude Fable 5 <noreply@anthropic.com>





## ♻️ Refactor
### rest,core,etl,release
- **rest,core,etl,release:** cut cognitive complexity and fix real type/shape smells<br>
  > Cognitive complexity (SonarCloud python:S3776, limit 15):
  > rest/fastapi/router_runtime._make_handler 27 -> 0: the per-route facts resolved
  > at startup move into a frozen _RouteRuntime, and profile resolution, parameter
  > building, payload decoding, execution, failure mapping and signature building
  > become named module-level functions instead of closures.
  > core/model/introspection.get_column_fields 22 -> 4: the three ways a column can
  > be described (declared, Annotated, inferred) move to _resolve_column_field.
  > scripts/release/checkout_merged_release._wait_for_merge 22 -> 10: state and
  > merge-state refusals become dispatch maps behind two named guards.
  > core/backend/sqlalchemy._configure_relationships 16 -> 3: relationship building
  > splits into _attach_relations and _relationship_kwargs.
  > Other src smells:
  > python:S8495: _parse_sort and _field_tuple now return one tuple shape.
  > msgspec.NODEFAULT as the third element keeps defstruct fields required.
  > python:S5890/S5655: RunContext.for_process replaces the bare dataclasses.replace,
  > so the process context is typed as RunContext at every call site.
  > pythonenterprise:S7181: the dedup_priority window states its frame explicitly.
  > It is the frame Spark already used (verified identical on a live session); an
  > orderBy added later would otherwise silently turn it into a running max.
  > python:S7504: neither loop mutates the container it iterates, so the defensive
  > list() copies go; the metaclass now collects defaults and applies them after.
  > python:S7500: pass the iterable straight to tuple().




## ✅ Tests
- import pytest as a module, split composite asserts, drop empty decorator parens<br>
  > Addresses SonarCloud python:S9084 (19), python:S9073 (19) and python:S9083 (15).
  > Assertion splits keep the exact same conditions, one per statement.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>

- keep a single throwing invocation inside every pytest.raises block<br>
  > Hoists construction of subjects, arguments and context managers out of the
  > `with pytest.raises(...)` body so the block contains only the call under test.
  > Where the setup must run inside a patch context it is hoisted to a plain local
  > before the combined `with`, since none of those constructors touch the patched
  > module attribute. Addresses SonarCloud python:S5778 (217 of 218).
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>




# 🚀 Release 0.18.0 ([#99](https://github.com/the-reacher-data/loom-py/pull/99)) ([`90d0bd9`](https://github.com/the-reacher-data/loom-py/commit/90d0bd95845e1b7c67b4e6d9476f9450faa401a6))


## ✨ Features
### etl,prefect
- **etl,prefect:** warn on empty writes and group processes as tasks









# 🚀 Release 0.17.1 ([#97](https://github.com/the-reacher-data/loom-py/pull/97)) ([`a69930f`](https://github.com/the-reacher-data/loom-py/commit/a69930f61d3960250b9a22af8c3d292def8459b2))



## 🐛 Fixes
### ci
- **ci:** lock and verify every dependency installed by workflows<br>
  > Close the SonarCloud supply-chain findings raised on the workflow files:
  > Pin uv to 0.10.2 in ci-pr, ci-main and docs (release.yml already did) and
  > install it with --only-binary :all: so no sdist setup script can run.
  > Pin every ephemeral tool installed by uv: pytest-cov==7.1.0, build==1.3.0,
  > twine==6.2.0 and the Sphinx documentation toolchain.
  > Run the pytest and docs environments with --frozen so the committed uv.lock
  > resolution is used verbatim.
  > Export a hashed twin of every Snyk manifest and install it with
  > pip --require-hashes, so downloaded artifacts are verified against uv.lock
  > while Snyk keeps reading the plain manifest it can parse.
  > -no-build cannot be enabled for uv: omegaconf, a runtime dependency, requires
  > antlr4-python3-runtime 4.9.3, which publishes no wheel on PyPI. The reason is
  > documented next to each affected command.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>


### release
- **release:** merge the version bump PR when its checks pass<br>
  > The release PR never reached mergeStateStatus CLEAN, so the prepare job always
  > timed out waiting for a merge that could not happen. GitHub reports CLEAN only
  > when the head commit carries a successful legacy commit status, and a release
  > PR has none: no workflow runs on a branch pushed with GITHUB_TOKEN, so no
  > coverage is uploaded and Codecov (which now reports through the Checks API)
  > never posts anything. Verified on PR #96: combined status "pending" with zero
  > statuses, while the check rollup was fully successful.
  > The helper now merges on CLEAN, or on UNSTABLE when every check reported on the
  > head commit has passed. A pending, failing or missing check still blocks the
  > merge, so the protection added in #90 is preserved rather than relaxed.
  > The release job also passes an explicit merge timeout, overridable through the
  > RELEASE_MERGE_TIMEOUT_SECONDS repository variable, as a safety net.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  > --------
  > Co-authored-by: Claude Fable 5 <noreply@anthropic.com>







## ✅ Tests
### ci
- **ci:** guard the workflow supply-chain contract<br>
  > Add contract tests over every workflow so the locking rules cannot regress:
  > pip installs must be hash-verified or pinned and wheels-only, uv --with specs
  > must be exact, and every workflow must install the same pinned uv.
  > Also run the TestPyPI build and upload with --frozen. uv treats uv.lock as the
  > source of truth under --frozen, so the version rewritten in pyproject.toml
  > before the build does not invalidate the locked resolution.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>




# 🚀 Release 0.17.0 ([#95](https://github.com/the-reacher-data/loom-py/pull/95)) ([`308c2b6`](https://github.com/the-reacher-data/loom-py/commit/308c2b6307bc681b16e5da12b1462a5bf1c7e783))


## ✨ Features
### identity
- **identity:** add the Identity value object, its context guard and 401 mapping<br>
  > The framework had no way to name the caller of an execution: authorization
  > decisions were taken from raw JWT claims left in the ASGI scope, a
  > transport-specific shape that only the HTTP layer could produce.
  > `loom.core.identity` introduces the domain-level answer to "who is running
  > this?": an immutable `Identity` (subject, roles, verified string attributes,
  > mechanism), the explicit `ANONYMOUS` instead of `None`, and a contextvar
  > guard mirroring `loom.core.tracing.context`. `require_subject()` and
  > `require_attribute()` fail closed, and `__repr__` exposes attribute *names*
  > but never their values, which are personal data.
  > `Unauthenticated` completes the error taxonomy — the caller can fix it by
  > authenticating, unlike `Forbidden` — and maps to 401 with the
  > `WWW-Authenticate: Bearer` challenge RFC 9110 §11.6.1 requires.
  > Additive only: no existing behaviour changes.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>


### engine
- **engine:** declare the caller identity with the Caller() marker<br>
  > Reading the identity from a global inside `execute()` is hidden state, and
  > taking it in `__init__` breaks the moment the use case runs behind a broker:
  > use cases are built per request, but a contextvar does not cross Celery.
  > `Caller()` joins `Input()`/`LoadById()` as a declarative marker: the compiler
  > turns it into a `CallerBinding` and the executor injects the identity the
  > transport handed it for that single execution. Being its own binding, it
  > never falls through to `ParamBinding`, so the caller can neither supply nor
  > forge it from the request.
  > Binding is fail-closed: a plan declaring `Caller()` with no identity raises
  > `Unauthenticated` naming the use case and the parameter, instead of quietly
  > substituting ANONYMOUS. A transport that wants an anonymous execution must
  > say so explicitly.
  > `router_runtime` becomes the single ambient identity read of the REST layer.
  > Two contained refactors keep the added binding from inflating signatures:
  > `_SignatureBindings` in the compiler and `_ExecutionInputs` in the executor
  > replace the growing positional accumulator lists.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>


### rest
- **rest:** declarative route authorization and per-connection role binding<br>
  > Two things land together because the review found the second while the first
  > was being wired.
  > `requires_roles` on `RestRoute` and `RestInterface` states which roles reach a
  > route, resolved at compile time with the route → interface precedence the
  > other policy fields already use. The router enforces it before `factory.build`,
  > so a denied caller never causes a use case — or the repositories its
  > constructor resolves — to exist. Holding any declared role is enough; no
  > identity is a 403 like any other denial, because whether authenticating would
  > have helped is part of the route's policy and the response must not say.
  > Per-connection role binding fixes a real deployment trap: the authentication
  > mechanism is application-wide but `allowed_roles` is per connection, so a
  > single-role connection (empty allowlist plus `default_role`) sitting next to a
  > multi-role one was intersecting against an empty allowlist and returning 403
  > forever. The binding is now decided per connection, and the audit span names
  > the effective roles including the `default_role` actually applied.
  > The ClickHouse multi-role workaround no longer mutates the driver namespace at
  > import time — a process-wide side effect hitting consumers that never touch
  > Loom. The registry enables it explicitly, only for a connection that can apply
  > more than one role, and the capability probe now checks the encoder seam still
  > exists instead of re-reading what it just assigned.
  > The duplicated "allowlist without a binding" predicate becomes one shared
  > `roles_need_identity_binding`; both guards stay, since they run at different
  > moments and the binder one is load-bearing.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>


### jobs
- **jobs:** propagate the caller identity through the job envelope<br>
  > A context variable does not cross a broker, so a job dispatched by an
  > authenticated caller arrived at the worker with no idea who asked for it. The
  > identity now travels inside the envelope, as an explicit part of the wire
  > contract — designed now, because adding it later would be a second break.
  > `encode_identity`/`decode_identity` speak plain JSON types so any broker
  > serializer carries them. Decoding treats the envelope as untrusted: a blank or
  > non-string subject, a tampered role list or a non-string attribute yields no
  > identity rather than a partially decoded one.
  > `CeleryJobService.dispatch` captures the caller at registration time, when it
  > is still known, and the worker publishes it for the whole task — so a job that
  > dispatches another job propagates the same caller onward — resetting it in a
  > `finally` because worker processes reuse their threads.
  > Old envelopes stay compatible: without the field the job simply runs with no
  > caller, and a job declaring `Caller()` fails closed with a message naming what
  > is missing.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>



## 🐛 Fixes
### sql
- **sql:** enforce the identity binding in the binder, not only in create_app<br>
  > Moving the rule to the startup gate left the unsafe shape representable
  > through the public bind_sql_endpoints: a connection with several allowed
  > roles and no claim binding them would mount an endpoint where the caller
  > picks their own privilege. The binder is where the route becomes
  > reachable, so the invariant is enforced there too — create_app still
  > reports it earlier and with more context.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>


### identity
- **identity:** use default_factory for the empty attribute mapping<br>
  > Python 3.11 dataclasses reject an unhashable default and a mappingproxy
  > is unhashable, so importing loom.core.identity crashed on the minimum
  > supported interpreter — caught by the strict docs build in CI, which runs
  > 3.11 while the local venv is 3.12. The factory returns the same frozen
  > empty mapping, so nothing is allocated per instance.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>



## 📖 Documentation
### identity
- **identity:** document the caller contract and teach the harnesses about it<br>
  > `UseCaseTest.with_caller` and `GoldenHarness.run(identity=...)` let an
  > authorization test state whose request it is, and the plan snapshot records the
  > caller binding so a golden pins that a use case reads its caller. Neither
  > harness softens the fail-closed rule: omitting the caller on a use case that
  > declares `Caller()` raises, because an authorization test that forgot to say
  > who is calling would be vacuous.
  > `docs/rest/identity.md` walks the whole contract — reading an attribute from a
  > use case, narrowing a QuerySpec to the caller's own rows, declarative
  > `requires_roles`, and a non-JWT authenticator — plus the `jwt_claims` migration
  > table. `docs/rest/sql.md` follows the rename to `auth: identity` and stops
  > promising raw claims.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>


### reference
- **reference:** publish the identity wire codec and the request-edge modules<br>
  > The job envelope codec and the middlewares that now guard the request edge are
  > part of the public contract: an application implementing its own transport
  > needs `encode_identity`/`decode_identity`, and an operator configuring CORS or
  > the trace-id header needs their reference pages.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>




## ♻️ Refactor
### sql
- **sql:** move roles_claim to the auth section, out of sql_endpoint<br>
  > The claim name is a property of the authentication mechanism, not of a SQL




## ✅ Tests
### sql
- **sql:** hoist fixture setup out of the pytest.raises block<br>
  > Sonar S5915 wants a single throwing invocation inside the block; the app
  > and service construction are setup, not the behaviour under test.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>


### identity
- **identity:** clear the sonar smells on the new code<br>
  > Split composite assertions so a failure names the condition that broke,
  > hoist setup out of pytest.raises blocks (S5915), and compare the route
  > match with != instead of the identity operator.
  > The remaining four issues are deliberate: the three coroutines without
  > await are required by their interfaces (Authenticator.authenticate is
  > async by protocol; sync Starlette exception handlers would run in a
  > threadpool), and Caller() keeps the PascalCase of the other markers.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  > --------
  > Co-authored-by: Claude Fable 5 <noreply@anthropic.com>




## 🔖 Other
- feat(sql)!: derive SQL roles from verified JWT claims and allow several per query<br>
  > The SQL endpoint took the role from the request BODY and validated it only
  > against the connection allowlist, so any bearer of a valid token could pick any
  > allowlisted role: authentication proved an identity but never bound it to a
  > privilege. Measured on ClickHouse 25.3 with one credential and only `role`

- feat(rest)!: make authentication pluggable and drop the jwt_claims scope key<br>
  > Authorization was reading raw JWT claims out of `scope["state"]["jwt_claims"]`,
  > which nailed every downstream rule to one mechanism and kept a second,
  > transport-shaped source of truth for a security decision next to the identity
  > context. Two sources of truth for who the caller is was the actual defect.
  > The REST layer now speaks one contract: an `Authenticator` turns
  > `RequestCredentials` (headers, path, peer) into an `Identity` or refuses.
  > `AuthenticationMiddleware` owns the request-scoped concerns — exclusions, the
  > 401 with its challenge, and the set/reset of the identity context in a
  > `finally` so a reused task cannot inherit the previous caller. `JwtAuthenticator`
  > verifies the token and projects its claims; `JwtAuthMiddleware` stays as a thin
  > composition with the same public signature.
  > `_sql_roles` now consumes an `Identity`: it no longer knows what a claim is,
  > while every invariant holds unchanged — intersection with the allowlist, body
  > narrowing only, no `default_role` fallback, 403 fail-closed, and an audit
  > WARNING now carrying the mechanism as well as the subject. A malformed roles
  > claim still grants nothing rather than a filtered subset.
  > `sql_endpoint.auth: jwt` becomes `identity` — the contract was never "a JWT",
  > it was "the framework knows the caller"; `jwt` keeps working as a deprecated
  > alias. `create_app(authenticator=...)` wires any other mechanism and is
  > mutually exclusive with `app.rest.auth.jwt`.
  > BREAKING CHANGE: `scope["state"]["jwt_claims"]` is gone. Read the caller with
  > `loom.core.identity.current_identity()`:
  > `jwt_claims["email"]` becomes `current_identity().attribute("email")`.
  > The 401 error code is now `unauthenticated`, matching `ErrorCode`.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>

- fix(rest)!: close the request-edge holes found in review<br>
  > Five findings, all reproduced, all at the boundary where a request first
  > reaches the application.

- Authentication exclusions were matched by string while Starlette routes by<br>
  > template.** A route declared `/{tenant}` answers `/metrics` and `/openapi.json`
  > too, so any application with a first-segment path parameter was serving a
  > business route with no credentials at all. `create_app` now asks the router
  > itself which routes each exclusion reaches, and refuses to boot when one is
  > captured. The exclusion list also follows the effective `docs_url`/`redoc_url`/
  > `openapi_url`/metrics paths instead of a hardcoded tuple that goes stale as
  > soon as an operator moves Swagger.

- Request bodies were unbounded.** uvicorn has no body-size option, so an<br>
  > endless chunked POST was an out-of-memory away. A middleware caps every route —
  > including ones the application mounted by hand — refusing a declared
  > Content-Length outright and cutting a lying or chunked one as it arrives. Since
  > FastAPI rewrites body-parsing failures into its own 400, the middleware
  > replaces whatever the application answers with the 413 once the cap is crossed.

- Three narrower ones.** The router's catch-all returned a trace id with no<br>
  > counterpart in the logs, so a 500 was untraceable and triggering one left no
  > record; `?limit=` and `?page=` reached the query unbounded and unvalidated,
  > turning `?limit=100000000` into a full scan and `?limit=abc` into a 500; and
  > the trace id header was echoed back and written to every log line exactly as
  > the caller wrote it.
  > `openapi_url` becomes configurable — disabling the docs no longer leaves the
  > full schema published — and an authenticated application serving it anonymously
  > now says so at startup. CORS becomes config-driven for one reason: Starlette
  > does not reject `allow_origins: ["*"]` with `allow_credentials: true`, it
  > starts reflecting the caller's Origin, so the wildcard quietly becomes "any
  > site, with cookies". That shape now fails at config parse.
  > Error bodies are finally uniform: validation failures used to answer FastAPI's
  > own shape, without a trace id, echoing the rejected input and linking to the
  > Pydantic docs.
  > BREAKING CHANGE: an application whose routes capture a default exclusion path
  > (for example `GET /{tenant}` at the root) no longer starts; narrow the route or
  > the exclusion list. `?limit=` above `RestApiDefaults.max_limit` is clamped, and
  > non-positive or non-numeric pagination values answer 400 instead of 500.
  > Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>


# 🚀 Release 0.16.1 ([#92](https://github.com/the-reacher-data/loom-py/pull/92)) ([`91e73c5`](https://github.com/the-reacher-data/loom-py/commit/91e73c5adfdc2646c6f6180286130ab0282f2a19))



## 🐛 Fixes
### ci
- **ci:** allow CHANGELOG.md as a known release action side effect








# 🚀 Release 0.16.0 ([#88](https://github.com/the-reacher-data/loom-py/pull/88)) ([`c04d61e`](https://github.com/the-reacher-data/loom-py/commit/c04d61ed88d9d46f954e9b851290e7cce22fbf46))


## ✨ Features
- add DynamoDB repository backend behind persistence.backend<br>
  > Add a DynamoDB backend for the Repository contract, selectable via
  > persistence.backend: dynamodb. Apps using it start with no database:
  > section and without SQLAlchemy — credentials come from boto3's default
  > chain (task role on ECS; endpoint_url + dummy creds locally), never
  > from config.
  > RepositoryDynamoDB implements key-scoped ops (get_by_id, create,
  > update, delete, key-only get_by/exists_by); scan-only ops (count,
  > list_paginated, list_with_query, non-key get_by/exists_by) raise an
  > explicit DynamoCapabilityError instead of degrading silently.
  > DynamoUnitOfWork/Factory: no-op UoW (writes autocommit via PutItem).
  > build_dynamodb_repository_registration_module mirrors the SQLAlchemy
  > registry; every model maps to the single configured table.
  > auto.py: new _dynamodb_wiring + elif branch in _resolve_persistence;
  > _PersistenceWiring structure unchanged.
  > Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>



## 🐛 Fixes
### dynamodb
- **dynamodb:** thread-safe client, insert/update conditions, resource-leak guard<br>
  > Address review feedback on the pluggable DynamoDB repository backend.
  > Thread-safety: migrate from boto3 `resource`/`Table` (not thread-safe under
  > `asyncio.to_thread`) to the low-level `client("dynamodb")`. Items now cross
  > the wire as AttributeValue dicts; `TypeSerializer`/`TypeDeserializer` wrap the
  > existing float<->Decimal / Decimal->int/float conversion. boto3/botocore and
  > the serializers are imported lazily to preserve the `loom[dynamodb]` optional
  > dependency. `RepositoryDynamoDB(client, table_name, model)`; registry and
  > `_dynamodb_wiring` pass the shared client.
  > Connection pool: add `_DynamoDBConfig.max_pool_connections` (default 32,
  > mirroring `_DatabaseConfig.pool_size`), applied via `botocore.config.Config`.
  > `create` is now an insert, not an upsert: `ConditionExpression`
  > `attribute_not_exists(pk)`; a failed condition maps to `Conflict`.
  > `update` carries `attribute_exists(pk)` to prevent resurrecting a
  > concurrently-deleted item; a failed condition maps to `None`, matching the
  > contract's "does not exist" return.
  > Resource leak: enforce the "No BaseModel classes discovered" guard before
  > `_resolve_persistence` allocates a SQLAlchemy engine, so the error path no
  > longer leaks an engine.
  > Docs: correct the obsolete `_resolve_persistence` docstring (the two-branch
  > if/elif is deliberate; promote to a dispatch dict at a third backend) and
  > document that DynamoDB `count`/`list_paginated`/`list_with_query` raise
  > `DynamoCapabilityError`, so REST listing endpoints error on this backend.
  > Tests: `FakeClient` replaces `FakeTable`/`FakeResource` with the client-shaped
  > API and honours the conditions by raising a real botocore `ClientError`; adds
  > coverage for create-on-existing-id (Conflict) and update losing the
  > conditional race (None).
  > Note (from commit 1): discovery now runs before the `database` `ConfigError`,
  > changing error precedence when both discovery and database config are invalid.
  > Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>





## ♻️ Refactor
- extract persistence wiring selection in create_app; keep sqlalchemy default; sanitize DIP/docs<br>
  > Extract the REST auto-bootstrap persistence choice into a single symmetric




## ✅ Tests
### dynamodb
- **dynamodb:** keep a single raising call inside pytest.raises blocks<br>
  > Move repo/argument construction out of the pytest.raises context so each
  > exception test has exactly one invocation that may throw (SonarQube S5915).
  > Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  > --------
  > Co-authored-by: Claude Opus 4.8 (1M context) <noreply@anthropic.com>






# 🚀 Release 0.15.1 ([#86](https://github.com/the-reacher-data/loom-py/pull/86)) ([`72f562c`](https://github.com/the-reacher-data/loom-py/commit/72f562cb57b16c7857e89844853faa6baa0d9768))


## ✨ Features
### prefect
- **prefect:** add chunked backfill_flow orchestration primitive<br>
  > Add backfill_flow() alongside etl_flow()/maintenance_flow(): a generic
  > @flow that runs an ETLPipeline chunk-by-chunk over a time window instead
  > of in a single pass. It slices [window_start_field, window_end_field) into
  > chunk-aligned partitions (hour/day/month, default day), runs
  > per_chunk_processes once per chunk oldest->newest, then runs
  > finalize_processes once with window_end pinned to the start of the current
  > chunk (not now) so batch-sequenced refreshes get a stable boundary.
  > Reuses the existing machinery for parity: ETLRunner.from_yaml, the _body
  > observer/manifest helpers, deploy metadata and synthesised signature. Each
  > chunk gets its own correlation id so manifests never leak across chunks;
  > resume is chunk-level via the start_from flow parameter.
  > Chunk granularity, window field names and process lists are all
  > parameterised so the primitive stays generic.
  > Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  > Claude-Session: https://claude.ai/code/session_01HNJydUX2yBKkiDEeh4uxtQ



## 🐛 Fixes
### deps
- **deps:** floor deltalake >=1.6.2 (MERGE correctness regression)<br>
  > deltalake (delta-rs) 1.6.0/1.6.1 raise "matched a target row with multiple source
  > rows that satisfy duplicate relevant WHEN MATCHED clauses" on clean, unique data
  > once a target file is read in more than one DataFusion batch — breaking the only
  > MERGE path in loom (IntoTable.upsert / _writer._upsert). Upstream
  > delta-io/delta-rs #4471/#4475/#4572, fixed in 1.6.2.
  > Raise the etl-polars / etl extras floor from >=1.5 to >=1.6.2 so no consumer
  > resolves a MERGE-broken delta-rs, and re-lock (deltalake 1.5.0 -> 1.6.2).


### historify
- **historify:** clamp LOG boundaries to zero-width on same-instant events<br>
  > Two events sharing an effective instant (e.g. a task created and flipped to
  > true in the same snapshot millisecond, task_created_at == task_updated_at)
  > land on the same effective_ts. build_log_boundaries then computes
  > valid_to = next_eff - offset < valid_from, an inverted (negative) interval.
  > Clamp valid_to to never fall below valid_from via
  > greatest(valid_from, next_eff - offset) in both backends, collapsing such
  > collisions to a zero-width [T, T] vector. The last event per entity has a
  > null next_eff and must stay open: max_horizontal/greatest skip nulls, so an
  > explicit null guard preserves the open row. Normal boundaries
  > (next_eff - offset > valid_from) are unchanged.
  > Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  > Claude-Session: https://claude.ai/code/session_01HNJydUX2yBKkiDEeh4uxtQ





## ♻️ Refactor
### prefect
- **prefect:** deduplicate factory boilerplate and split backfill body<br>
  > Every flow factory (etl_flow, maintenance_flow, backfill_flow) duplicated
  > the same deploy-time boilerplate: per-flow YAML settings, safe-name +
  > __signature__ attachment, prefect.flow decoration and ETLFlowMeta wiring.
  > Extract it into flow._assemble (load_flow_settings + assemble_flow) so a
  > change to the decoration or metadata contract touches one place, and unify
  > the three per-factory _synthesise_signature copies into
  > _signature.synthesise_flow_signature(params_type, extra_parameters=...).
  > Promote the manifest/observer helpers backfill imported privately from
  > _body into flow._runtime with public names (load_or_init_manifest,
  > build_observers, maybe_delete_manifest), and process-name validation into
  > flow._stages with a parameterised field label. Type flow_run_id as
  > uuid.UUID | None and observers as list[LifecycleObserver].
  > Backfill hardening, in parity with etl_flow:
  > split the runtime body out of the factory into _backfill_body, with the
  > per-chunk floor/advance/label rules in one _ChunkAlgebra table so adding
  > a granularity touches a single dict
  > validate per_chunk_processes AND finalize_processes against the compiled
  > pipeline at build time (a typo no longer explodes after hours of chunks)
  > rename Chunk to BackfillChunk and re-export it from loom.prefect and
  > loom.prefect.flow so consumers can type chunk=
  > make window_start_field/window_end_field required (they were consumer
  > conventions leaking into the library defaults)
  > log the accepted env parameter and document that it does not route;
  > document that manifests are observability-only (resume is chunk-granular
  > via start_from), that finalize runs once after all chunks, and that the
  > flow deliberately registers without Prefect retries
  > drop the redundant _as_utc double-coercion in _chunk_windows




## ✅ Tests
### historify
- **historify:** cover DATE-precision clamp and document zero-width vectors<br>
  > Add same-day collision tests for build_log_boundaries with
  > date_type=DATE on both backends (the clamp fix was only exercised at
  > TIMESTAMP precision), and document the zero-width [T, T] semantics in
  > the public IntoHistory/HistorifySpec docstrings: boundaries are
  > inclusive, so a point-in-time query at the collision instant matches
  > both the zero-width row and the row opening at it.
  > Also hoist the datetime import in the Spark historify test module.
  > Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  > Claude-Session: https://claude.ai/code/session_01HNJydUX2yBKkiDEeh4uxtQ
  > --------
  > Co-authored-by: Claude Opus 4.8 (1M context) <noreply@anthropic.com>






# 🚀 Release 0.15.0 ([#84](https://github.com/the-reacher-data/loom-py/pull/84)) ([`c3581bd`](https://github.com/the-reacher-data/loom-py/commit/c3581bdc28255935c5658a15b9b64c035cf3cc96))


## ✨ Features
### dynamodb
- **dynamodb:** add FromDynamoDb source connector for the polars backend











# 🚀 Release 0.14.3 ([#82](https://github.com/the-reacher-data/loom-py/pull/82)) ([`02bdbc4`](https://github.com/the-reacher-data/loom-py/commit/02bdbc4345805748dc2bb57f8f7c54c0517fb160))



## 🐛 Fixes
### mongo
- **mongo:** preserve sparse fields beyond schema sample










# 🚀 Release 0.14.2 ([#80](https://github.com/the-reacher-data/loom-py/pull/80)) ([`74e626d`](https://github.com/the-reacher-data/loom-py/commit/74e626d9d58ed703d54449ad078d652dc2779ce2))


## ✨ Features
### mongo
- **mongo:** opt-in flattening of literal Extended JSON wrappers<br>
  > Legacy writers sometimes store serialized Extended JSON back as data:
  > a field holds the dict {"$date": "..."} instead of a BSON date. The
  > BSON normalizer is blind to plain dicts, so the wrapper reaches the
  > lake verbatim and typed decoders downstream either null the value or
  > abort. In production this first dropped business subdocuments silently
  > and later crashed a nightly load on a single poisoned document.
  > FromMongo(...).normalize_extended_json() flattens single-key wrapper
  > dicts to plain values inside the existing normalization walk — an O(1)
  > probe per dict node, no extra traversal. Off by default: reinterpreting
  > stored data is a consumer decision. Mongo forbids $-prefixed field
  > names in stored documents, so no legitimate data matches the probe;
  > malformed wrappers degrade per-value.






## ♻️ Refactor
### mongo
- **mongo:** flatten _normalize dispatch below the cognitive-complexity gate







# 🚀 Release 0.14.1 ([#78](https://github.com/the-reacher-data/loom-py/pull/78)) ([`b616857`](https://github.com/the-reacher-data/loom-py/commit/b6168572dd2bcd2789992369bb715798bdd97dfa))



## 🐛 Fixes
### mongo
- **mongo:** count and surface serialization failures, never abort a scan on a bad document










# 🚀 Release 0.14.0 ([#76](https://github.com/the-reacher-data/loom-py/pull/76)) ([`18c274d`](https://github.com/the-reacher-data/loom-py/commit/18c274d20bb5da0f15b1f00b642aad950c22cf3e))


## ✨ Features
### historify
- **historify:** idempotent reruns and rewind-based backfill<br>
  > Replace rollback_same_day_run with rewind_to: discard rows with
  > valid_from >= eff_date and reopen rows closed at prev(eff_date),
  > clearing valid_to and deleted_at. Covers same-day reruns and makes
  > SNAPSHOT backfills with allow_temporal_rerun=True safe (rewind +
  > replay) instead of corrupting intervals.
  > Materialize HistorifyRepairReport on backfill (affected keys, dates
  > requiring downstream rerun) and log it from both writers.
  > Make LOG-mode corrections converge: dedup by keys + effective_date
  > with explicit origin priority (incoming wins), replacing the
  > non-deterministic Spark dedup ordering.
  > Document the idempotency contract in IntoHistory.






## ♻️ Refactor
### historify
- **historify:** drop unused join_key parameter from _first_run







# 🚀 Release 0.13.0 ([#73](https://github.com/the-reacher-data/loom-py/pull/73)) ([`bd19527`](https://github.com/the-reacher-data/loom-py/commit/bd195275a7395fcf512177bce6c80b1f6cd1c750))


## ✨ Features
### historify
- **historify:** honor `overwrite` in LOG mode (same-track run collapse)











# 🚀 Release 0.12.3 ([#70](https://github.com/the-reacher-data/loom-py/pull/70)) ([`1964d8b`](https://github.com/the-reacher-data/loom-py/commit/1964d8b36e1402a63d7e46ff70d41d460a8e9812))



## 🐛 Fixes
### prefect
- **prefect:** configure maintenance observability










# 🚀 Release 0.12.2 ([#68](https://github.com/the-reacher-data/loom-py/pull/68)) ([`4f067d0`](https://github.com/the-reacher-data/loom-py/commit/4f067d060347334128455503d6a4975c1bcc6285))



## 🐛 Fixes
### prefect
- **prefect:** observe maintenance logs in flow runs










# 🚀 Release 0.12.1 ([#66](https://github.com/the-reacher-data/loom-py/pull/66)) ([`f3c045e`](https://github.com/the-reacher-data/loom-py/commit/f3c045ec6b9e915e5d2c9f55d7134a7be37c1475))



## 🐛 Fixes
### runner
- **runner:** wire ClickHouseClientExecutor in from_config for ClientStep










# 🚀 Release 0.12.0 ([#64](https://github.com/the-reacher-data/loom-py/pull/64)) ([`a471a17`](https://github.com/the-reacher-data/loom-py/commit/a471a17337d2d63b66fad0f74c81b199b943e588))


## ✨ Features
### etl
- **etl:** add ClientStep for side-effect engine commands<br>
  > Introduces a new step type for executing ClickHouse DDL, OPTIMIZE,
  > ALTER TABLE DROP PARTITION, and similar commands that need the native
  > engine client but produce no DataFrame output.
  > New public API:
  > `ClientStep[ParamsT]` — base class; implement `execute(params, *, client)`
  > `IntoClient` / `ClientSpec` — target sentinel that bypasses read-write path
  > `ClientCommandExecutor` protocol — injected capability; method is `command(fn)`
  > `ClickHouseClientExecutor` — standalone executor with lazy thread-safe client
  > `ETLRunner(client_executor=...)` / `ETLExecutor(client_executor=...)` params
  > Architecture decisions:
  > `ETLExecutor.run_step` short-circuits to `client_executor.command(fn)` before
  > any source reads when `target_binding.spec` is `ClientSpec`
  > Compiler skips execute-signature validation for `ClientStep` subclasses
  > `_write_policy` and `ClickHouseTargetWriter.write()` raise on `ClientSpec`
  > as a defence-in-depth guard
  > `ETLStep._log` class variable set by `__init_subclass__` gives every step
  > a structured logger named after the concrete step class
  > Module-level `try/except` imports replace lazy function-level imports;
  > `threading.Lock` guards lazy client init under parallel step execution
  > Also fixes a pre-existing return type annotation in `_historify.py`:
  > `pl.Datetime("us", "UTC")` is an instance, not `type[pl.Datetime]`.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **etl:** ClickHouseSourceReader implements ClientCommandExecutor<br>
  > Allows passing the existing ClickHouseSourceReader as client_executor
  > to ETLRunner / ETLExecutor, reusing the same connection for both
  > DataFrame reads and ClientStep side-effect commands — no second
  > ClickHouse connection opened.
  > Changes:
  > `ClickHouseSourceReader` now inherits `ClientCommandExecutor` and
  > exposes `command(fn)` which lazily resolves the client via the same
  > `_get_client()` path used by `read()` and `read_streaming()`
  > `threading.Lock` and double-check pattern protect the shared lazy
  > client init against concurrent access under `ParallelStepGroup`
  > Module-level `try/except` imports for `clickhouse_connect` and
  > `pyarrow` replace lazy function-level imports
  > Usage:
  > ch_reader = ClickHouseSourceReader(url="clickhouse://...")
  > runner = ETLRunner(
  > reader=ch_reader,
  > writer=ch_writer,
  > client_executor=ch_reader,  # reuses the same connection
  > )
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>


### prefect
- **prefect:** wrap ETLProcess instances as Prefect subflows<br>
  > Each ETLProcess now runs inside a nested @prefect.flow, giving the Prefect
  > UI a process-level hierarchy instead of a flat list of 300 tasks.
  > PrefectTaskRunObserver resolves the active flow_run_id at task-creation
  > time via prefect.runtime, so task runs attach to the correct subflow
  > automatically. Fallback to the stored flow_run_id keeps unit tests intact.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>



## 🐛 Fixes
### tests
- **tests:** resolve Sonar collection-access issues in test_client_step<br>
  > Replace [0] index access with tuple unpacking to eliminate potential
  > IndexError; replace == [] comparisons with `not collection` idiom.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
  > --------
  > Co-authored-by: Claude Sonnet 4.6 <noreply@anthropic.com>











# 🚀 Release 0.11.4 ([#62](https://github.com/the-reacher-data/loom-py/pull/62)) ([`92904c2`](https://github.com/the-reacher-data/loom-py/commit/92904c20b99b7a4659a6658f3c29af46ac3887f3))



## 🐛 Fixes
### historify
- **historify:** tz-aware UTC SCD2 boundaries










# 🚀 Release 0.11.3 ([#60](https://github.com/the-reacher-data/loom-py/pull/60)) ([`ae40236`](https://github.com/the-reacher-data/loom-py/commit/ae40236b2706ccd30924267ea87294de6a76ad1b))



## 🐛 Fixes
### historify
- **historify:** align tz-aware Delta read-back in LOG-mode union










# 🚀 Release 0.11.2 ([#58](https://github.com/the-reacher-data/loom-py/pull/58)) ([`54b6069`](https://github.com/the-reacher-data/loom-py/commit/54b6069e4b012e1ad64e8da3baaf18afe735afc9))



## 🐛 Fixes
### historify
- **historify:** allow recurring tracked values in LOG mode










# 🚀 Release 0.11.1 ([#56](https://github.com/the-reacher-data/loom-py/pull/56)) ([`817a6c4`](https://github.com/the-reacher-data/loom-py/commit/817a6c48c6d377ca21bf60cede1c16126a8fdcfb))



## 🐛 Fixes
### config
- **config:** resolve interpolations in includes paths<br>
  > `_load_local_file` was calling `omega_conf.to_container(raw_includes)` without
  > `resolve=True`, so paths like `includes: [pool.${oc.env:VARIANT,local}.yaml]`
  > were treated literally and raised ConfigError on the next load attempt.
  > Aligns the call with `loader.py:325`, which already passes `resolve=True`
  > when materialising the resolved config. The facade default stays
  > `resolve=False` to preserve other callers that need unresolved data.
  > Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>


### polars
- **polars:** skip create on empty frame missing partition cols<br>
  > `replace_partitions` already skips writing when the frame is empty on the
  > update path (existing table). The create path (target does not exist yet)
  > did not, so a fallback that produced an empty frame without the partition
  > columns surfaced a cryptic delta-rs `SchemaMismatchError: Unable to get
  > field named "<col>"`.
  > Two changes in `PolarsTargetWriter._create`:
  > 1. If partition cols are missing AND the frame is empty: skip with a
  > warning. Mirrors `_replace_partitions` / `_streaming_replace_partitions`
  > so create and update behave consistently.
  > 2. If partition cols are missing AND the frame has rows: raise a clear
  > loom `ValueError` instead of letting delta-rs fail with an opaque
  > schema error. The message lists the missing cols and the frame schema
  > so the caller can diagnose without inspecting tracebacks.
  > Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  > --------
  > Co-authored-by: Claude Opus 4.7 (1M context) <noreply@anthropic.com>











# 🚀 Release 0.11.0 ([#54](https://github.com/the-reacher-data/loom-py/pull/54)) ([`23bff3d`](https://github.com/the-reacher-data/loom-py/commit/23bff3df20607bc12268de0d941ce46df61e16bf))


## ✨ Features
### maintenance
- **maintenance:** add loom.etl.maintenance — agnostic Delta vacuum/optimize<br>
  > New module providing a declarative, backend-agnostic API for running
  > Delta table maintenance operations (vacuum, compact, z-order) via delta-rs.
  > Core abstractions:
  > MaintenanceStep[ParamsT] base class (mirrors ETLStep pattern)
  > MaintainTable fluent builder for explicit per-table ops
  > MaintainSchema for autodiscovery by schema prefix from StorageConfig
  > MaintenanceRunner.from_config() wired to StorageConfig.to_path_locator()
  > MaintenanceReport with per-table error isolation + raise_if_errors()
  > operations_for(params) hook enabling Prefect-parameter-driven operations
  > Storage config:
  > MaintenanceConfig + MaintenanceVacuumConfig added to StorageConfig
  > run_from_config() for config-only maintenance without any Python class
  > Prefect integration:
  > maintenance_flow() factory in loom.prefect.flow (mirrors etl_flow)
  > _common.coerce_tags extracted from _factory — no more private cross-import
  > Deployable via same YAML pattern as ETL pipelines (step_class key)
  > Safety defaults:
  > vacuum dry_run=True everywhere; production must opt-in with dry_run=False
  > continue-on-error per table; MaintenanceReport.raise_if_errors() for gates
  > missing_table_policy="skip" by default
  > 42 unit tests covering builder, step, runner (StubMaintainer), and
  > DeltaRsMaintainer backend against real local Delta tables.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>


### notify
- **notify:** enrich NotifyEvent with duration + env; fix loggers<br>
  > NotifyEvent:
  > duration_seconds: float | None — wall-clock time from Prefect's
  > flow_run.total_run_time. Displayed as '2m 03s', '1h 05m 12s', etc.
  > env: str | None — forwarded from the flow's 'env' parameter so ops
  > teams know which environment triggered the alert.
  > _hooks.py:
  > _duration_from_run() extracts total_run_time.total_seconds()
  > _env_from_params() reads the 'env' param (present in all loom flows)
  > correlation_id stays empty for maintenance flows (no business key) —
  > the run name already carries the timestamp identifier
  > Replace stdlib logging with loom get_logger; convert %s → kwargs
  > SlackNotifier._render:
  > Header: icon + flow_name + state
  > Meta line: run name  env (when set)  correlation (when set)  duration
  > Omits empty fields — maintenance messages stay clean without correlation
  > _fmt_duration() helper: 45s / 2m 03s / 1h 05m 12s
  > dry_run indicator is in the run name for maintenance (Prefect shows
  > params in the UI); a future iteration can surface it in the message
  > field via MaintenanceReport summary.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **notify:** add run summary to Slack — ETL steps + maintenance table count


## 🐛 Fixes
### maintenance
- **maintenance:** use loom StructLogger + fix logging.exception + Sonar smells<br>
  > _runner.py, backends/_delta_rs.py: replace stdlib `import logging` with
  > loom's `from loom.core.logger import get_logger`. StructLogger only accepts
  > (event: str, **kwargs) — convert all %s positional format calls to keyword


### test
- **test:** use type() instead of class statement to avoid Sonar S1118<br>
  > type() triggers __init_subclass__ identically to a class statement but
  > returns a value — static analyzers see a function call, not an unused
  > class declaration. Resolves Sonar 'Remove this unused class declaration'
  > on test_step.py:28 and :34.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **test:** revert to class statement with NOSONAR — type() introduced Bug Major<br>
  > Using type() to avoid Sonar S1118 introduced a new Bug Major ('return
  > value of type() must be used'). The class statement IS the correct pattern
  > for testing __init_subclass__ — the class definition is the act under test.
  > Add # NOSONAR on the class line: SonarQube's official suppression mechanism
  > for confirmed false positives. Tells Sonar the unused class is intentional.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>





## ♻️ Refactor
### maintenance
- **maintenance:** move imports to module level + fix noqa:ARG002<br>
  > _delta_rs.py: move deltalake + protocol types to module level; this module
  > is only loaded when DeltaRsMaintainer is used so the [delta] extra is always
  > present. Removes lazy imports from all three method bodies.
  > _runner.py: move _ops types (CompactSpec, MaintenanceSpec, VacuumSpec,
  > ZOrderSpec) and TableRef to module level. Keep DeltaRsMaintainer import
  > lazy inside from_config() with noqa:PLC0415 — avoids forcing [delta] on
  > users who import MaintenanceRunner for type annotations only.
  > Remove incorrect noqa:ARG002 on run(params): params IS used via
  > operations_for(params) in _resolve_specs.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **maintenance:** iter-1 SOLID — OperationDeclaration protocol (LSP/OCP)<br>
  > Introduce OperationDeclaration as a @runtime_checkable Protocol that both
  > MaintainTable and MaintainSchema implement, replacing the isinstance branch
  > in _resolve_specs with a uniform op.resolve(config) call.
  > _protocol.py: add OperationDeclaration Protocol
  > _builder.py: add resolve() to MaintainTable (returns [_to_spec()]) and
  > MaintainSchema (delegates to _expand)
  > _step.py: operations ClassVar typed as list[OperationDeclaration];
  > __init_subclass__ validates via isinstance(op, OperationDeclaration)
  > _runner.py: _resolve_specs simplified to a single list comprehension
  > __init__.py: export OperationDeclaration
  > Fixes LSP (MaintainTable/MaintainSchema now substitutable) and OCP partial
  > (new operation declaration types no longer require touching the runner).
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **maintenance:** iter-2 SOLID — centralize duplication + reduce CC (SRP/DRY)<br>
  > _builder.py: extract _expand_for_schemas() pure function — single source of
  > truth for schema-prefix filtering. MaintainSchema._expand delegates to it.
  > _runner.py: run_from_config calls _expand_for_schemas instead of reimplementing
  > the loop; extract _resolve_location(table_ref) -> TableLocation | None to
  > encapsulate the try/except + missing_table_policy logic. _run_one CC drops
  > from 7 to 3 (early-return on None + single try block).
  > backends/_delta_rs.py: extract _open(uri, location) to consolidate DeltaTable
  > + WriterProperties + CommitProperties construction repeated in all three methods.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **maintenance:** iter-3 SOLID — polymorphic OpSpec dispatch (OCP)<br>
  > Eliminate the if-chain in _run_one and make the module extensible to new
  > operation types (RESTORE, CHECKPOINT…) without touching runner or builders.
  > Design:
  > _ops.py: VacuumSpec/CompactSpec/ZOrderSpec each gain a ClassVar name and
  > execute(maintainer, uri, location) method — self-dispatching Strategy pattern.
  > MaintenanceSpec replaces three nullable fields with ops: tuple[OpSpec, ...].
  > _protocol.py: add OpSpec @runtime_checkable Protocol (name + execute).
  > TableMaintenanceResult switches to op_results: dict[str, result] with
  > @property vacuum/compact/z_order accessors for backwards compatibility.
  > _builder.py: internal _ops list replaces three separate fields; _to_spec /
  > _expand build the tuple. _expand_for_schemas takes an ops tuple instead of
  > three params — one call site, one contract.
  > _runner.py: _run_one becomes a single loop:
  > for op in spec.ops: op_results[op.name] = op.execute(maintainer, uri, loc)
  > Zero if-chain; adding a new OpSpec type requires zero changes here.
  > To add a new operation (e.g. RESTORE):
  > 1. Create RestoreSpec(name="restore") with execute() → RestoreResult
  > 2. Add .restore() fluent method to MaintainTable / MaintainSchema
  > 3. Done — runner, protocol, and tests need no changes.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **maintenance:** post-iter cleanup — OpSpec ClassVar + deduplicate validation<br>
  > _ops.py: MaintenanceSpec.ops typed as tuple[OpSpec, ...] for consistency
  > with the runner loop; import OpSpec under TYPE_CHECKING.
  > _protocol.py: OpSpec.name declared as ClassVar[str] so mypy correctly
  > validates VacuumSpec/CompactSpec/ZOrderSpec as structural subtypes.
  > _builder.py: extract _assert_no_conflicting_ops() pure function — single
  > source of truth for the compact⊕z_order invariant, called from _to_spec
  > and _expand (was duplicated in 3 places).
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>








# 🚀 Release 0.10.4 ([#52](https://github.com/the-reacher-data/loom-py/pull/52)) ([`3bf88be`](https://github.com/the-reacher-data/loom-py/commit/3bf88bee9210893dbf9522c1cf5c0424c069e43e))


## ✨ Features
### storage
- **storage:** add MissingTablePolicy.ERROR to block auto-creation<br>
  > Tables must be pre-created via the catalog process. With schema_mode,
  > steps using SchemaMode.OVERWRITE could silently create tables bypassing
  > catalog governance. ERROR unconditionally blocks creation regardless of
  > the step's schema_mode.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>



## 🐛 Fixes
### runner
- **runner:** cleanup RUN-scope checkpoints in ETLRunner.run() finally block<br>
  > CheckpointScope.RUN promises cleanup 'in the finally block of every
  > pipeline run' but ETLRunner.run() never called cleanup_run(). RUN-scope
  > checkpoint files accumulated in S3 indefinitely after every run.
  > Wrap flush_runner in its own try/finally so cleanup always executes
  > even if the observability flush raises. Cleanup errors are caught and
  > logged as warnings to avoid masking the original pipeline exception.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
  > --------
  > Co-authored-by: Claude Sonnet 4.6 <noreply@anthropic.com>











# 🚀 Release 0.10.3 ([#48](https://github.com/the-reacher-data/loom-py/pull/48)) ([`15c54d0`](https://github.com/the-reacher-data/loom-py/commit/15c54d09d74297bac63508a11b23ec5048b8128a))


## ✨ Features
### polars
- **polars:** streaming Arrow writes for replace_partitions<br>
  > Adds a target-level streaming flag on AppendSpec/ReplaceSpec/
  > ReplacePartitionsSpec/ReplaceWhereSpec (UpsertSpec excluded — MERGE has
  > no streaming source path in delta-rs). The Polars backend honours the
  > flag today for replace_partitions against existing tables: the
  > LazyFrame is sunk to a lz4-compressed IPC spool and re-opened as a
  > pyarrow.RecordBatchReader passed straight to write_deltalake, bounding
  > peak RAM to roughly one batch + delta-rs internals.
  > Partition predicate is computed via a projection-pushdown scan over the
  > same spool. Schema alignment and the Null-dtype guard run lazily on the
  > LazyFrame before the sink. Other modes accept the flag for forward
  > compatibility but still materialise the frame.
  > Spool directory is configurable via LOOM_SPOOL_DIR so container
  > workloads with a tmpfs /tmp (Fargate) can spill to a real disk.
  > Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>



## 🐛 Fixes
### clickhouse
- **clickhouse:** compress streaming IPC spool with lz4<br>
  > The read_streaming path spooled Arrow IPC files without compression,
  > producing ~10-15 GB temporary files for large ClickHouse result sets
  > (e.g. 19M-row CDC tables). Combined with a second lz4-compressed spool
  > from the downstream streaming Delta write, total ephemeral disk usage
  > exceeded 20 GB on Fargate tasks.
  > Adding lz4 compression to the CH spool reduces it ~3x (~4-5 GB),
  > bringing the combined footprint to ~8-10 GB — within the Fargate
  > default ephemeral limit without requiring task definition changes.
  > Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>









## 🔖 Other
- chore(version) Update version<br>
  > --------
  > Co-authored-by: Claude Opus 4.7 (1M context) <noreply@anthropic.com>




# 🚀 Release 0.10.2 ([#46](https://github.com/the-reacher-data/loom-py/pull/46)) ([`0fa17f5`](https://github.com/the-reacher-data/loom-py/commit/0fa17f5735be843e68ba446993bb41f2d107c33b))


## ✨ Features
### polars
- **polars:** streaming Arrow writes for replace_partitions











# 🚀 Release 0.10.1 ([#44](https://github.com/the-reacher-data/loom-py/pull/44)) ([`a55cd27`](https://github.com/the-reacher-data/loom-py/commit/a55cd27020dee60aea6849b510500e7e6f5a83b2))



## 🐛 Fixes
### testing
- **testing:** implement read_streaming on _PolarsStubReader (#44)










# 🚀 Release 0.10.0 ([#41](https://github.com/the-reacher-data/loom-py/pull/41)) ([`099de38`](https://github.com/the-reacher-data/loom-py/commit/099de38b47cd7002a9c36fc56c9c85f673431022))


## ✨ Features
### prefect
- **prefect:** add loom.prefect observer module for ETL observability<br>
  > Introduces loom.prefect as an optional integration layer that exposes each
  > ETLStep as an observable Prefect @task, giving Prefect full step-level
  > visibility into ETL pipelines rather than treating the whole run as a
  > single opaque process.
  > Key additions:
  > FlowCtx: typed operational contract (correlation_id, run_id, processes, force, dry_run)
  > RunManifest + ManifestStore: ephemeral S3-backed retry state — zero S3 writes on happy path
  > S3JsonManifestStore: fsspec-backed implementation with correlation_id path validation
  > PrefectObserver: LifecycleObserver forwarding loom events to Prefect UI logs/artifacts
  > FlowConfig + _load_flow_config: per-flow retry policy from YAML
  > build_etl_flow(): factory that compiles a PipelinePlan once at build time,
  > creates the runner once per Fargate container, and submits each step as @task
  > respecting ParallelStepGroup topology and skipping SUCCESS steps from the manifest
  > ETLRunner.run() now accepts optional run_id so Prefect and loom lineage share
  > the same traceability identifier

- **prefect:** add Fargate/Docker launcher + deploy_etl() for ETL flow generation











# 🚀 Release 0.9.2 ([#39](https://github.com/the-reacher-data/loom-py/pull/39)) ([`f1a482d`](https://github.com/the-reacher-data/loom-py/commit/f1a482d03f8b83f8e0c7725e7af6160466ac6126))


## ✨ Features
### config
- **config:** add SecretsManagerResolver + shared resolver utilities











# 🚀 Release 0.9.1 ([#36](https://github.com/the-reacher-data/loom-py/pull/36)) ([`5c420c9`](https://github.com/the-reacher-data/loom-py/commit/5c420c9dd4efe0f697affb8a921054bc6504358f))


## ✨ Features
### streaming
- **streaming:** add register_sink to StreamingRunner<br>
  > Allows applications to declare sinks once via register_sink(cls) and
  > let the runner resolve them from YAML config at startup, eliminating
  > manual sink wiring in entrypoints.
  > New public API:
  > runner = StreamingRunner()
  > runner.register_sink(ClickHouseErrorTableSink)
  > runner.run(flow=build_flow(), config_path=streaming_config_path())
  > Sink classes satisfy the RegisteredSink protocol (sink_type, config_type,
  > build_binding). The runner matches YAML entries by type field, deserializes
  > each section with config_type, and calls build_binding(cfg, ctx).
  > Adds DuplicateErrorSinkError for conflicting ErrorKind assignments.
  > Adds STREAMING_SINKS to ConfigKey.
  > StreamingRunner.__init__ is now no-arg; factory methods unchanged.
  > prepare_run(error_sinks=...) remains supported for tests and overrides.


### config
- **config:** add SSM dot-notation key navigation and resolve logging<br>
  > _split_ssm_key splits /path/param.key into SSM path + JSON key list
  > _navigate_json parses SSM value as JSON and traverses key path
  > resolve() emits INFO log with expanded SSM path (never the value)
  > ConfigResolver.resolve() widened to -> object (str <= object, LSP-safe)
  > Guard against empty key with explicit ConfigError



## 🐛 Fixes
### tests
- **tests:** guard received[0] index access in test_sink_registry







## 🛠 Chores
### release
- **release:** sync pyproject.toml version to 0.9.0<br>
  > The v0.9.0 tag was created by a hotfix but the version bump PR did not
  > merge cleanly, leaving pyproject.toml stuck at 0.8.0 on master. This
  > aligns the declared version with the existing tag so the next release
  > computes 0.10.0 instead of colliding with v0.9.0.





# 🚀 Release 0.8.0 ([#30](https://github.com/the-reacher-data/loom-py/pull/30)) ([`702b99e`](https://github.com/the-reacher-data/loom-py/commit/702b99ec75a4130f273322f8eb488637a2f98a14))


## ✨ Features
### streaming
- **streaming:** add IntoSink/SinkPartition protocols and Decompose node<br>
  > Introduces the two-level contract that makes storage sinks extensible at
  > the node level. Any frozen dataclass satisfying IntoSink is a first-class
  > terminal node — no registration, no inheritance, no framework coupling.
  > core/schema/: new shared module; SchemaMode promoted from ETL (ETL re-exports for backwards compat)
  > nodes/_sink.py: SinkPartition (contravariant per-worker protocol) and IntoSink (runtime-checkable terminal protocol)
  > nodes/_decompose.py: EntityDecomposer protocol and Decompose transformation node
  > validate.py: _is_leaf_terminal() helper unifies all terminal checks; IntoSink recognised as terminal in shape and output validation; _node_output_shape refactored to dispatch map
  > exports: IntoSink, SinkPartition, Decompose, EntityDecomposer in public API
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **streaming:** add IntoTable sink node and rename EntityDecomposer to PayloadExpander<br>
  > IntoTable: frozen dataclass implementing IntoSink for SQLAlchemy and Delta backends
  > Backend enum: SQLALCHEMY and DELTA variants
  > _SQLAlchemyTablePartition: bulk-insert via engine.begin() per epoch batch
  > _DeltaTablePartition: write via deltalake + polars with validated write mode
  > Rename EntityDecomposer -> PayloadExpander, decompose() -> expand(), targets -> outputs
  > Update both streaming.__init__ and streaming.nodes.__init__ public exports
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>


### streaming/compiler
- **streaming/compiler:** teach compiler to resolve IntoSink nodes<br>
  > Add CompiledStorageSink to _plan: holds the IntoSink node and its
  > pre-fetched streaming.sinks.<name> config section
  > Add terminal_storage_sinks field to CompiledPlan (default empty dict
  > keeps all existing tests green)
  > Refactor _build_terminal_sinks and all branch builders to return
  > (kafka_sinks, storage_sinks) tuple in a single walk — no duplicate
  > traversal
  > Add _build_storage_sink: resolves config by node.name or passes {}
  > when name is empty (self-configured sinks)
  > Add validate_storage_sinks phase: reports a clear error for every
  > named IntoSink whose streaming.sinks.<name> section is absent
  > Wire validate_storage_sinks into the compiler pipeline
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **streaming/compiler:** add Decompose shape and structural validation<br>
  > Import Decompose in validate.py
  > Add RECORD to _node_input_shape for Decompose — enforces that it
  > receives individual events, not batches
  > Add Decompose: StreamShape.RECORD to _FIXED_OUTPUT_SHAPES — its
  > output feeds the Router as per-type records
  > Add structural check in _validate_shape_sequence: Decompose must be
  > immediately followed by a Router; clear error message when it is not
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>


### streaming/bytewax
- **streaming/bytewax:** wire IntoSink nodes to Bytewax output operators<br>
  > Add handlers/storage.py: _StorageSinkPartition wraps loom SinkPartition,
  > extracting payload from Message envelopes; _StorageDynamicSink calls
  > node.build_partition(config, worker_index, worker_count) once per
  > Bytewax worker at startup; _apply_into_sink registers the DynamicSink
  > via bw_output for any IntoSink node found in the compiled plan
  > Register IntoSink in _NODE_HANDLERS dispatch map (dispatcher.py) so
  > _wire_process routes any IntoSink node to _apply_into_sink
  > Add IntoSink pass-through in _execute_router_node (routing.py)
  > consistent with the IntoTopic/Drain pattern for inline Router execution
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>






## ♻️ Refactor
### streaming
- **streaming:** improve _SQLAlchemyTablePartition with typed columns and pool config<br>
  > Extract _require_sa_url, _sa_engine_kwargs, _sa_type_for, _sa_table_from_struct,
  > _structs_to_rows helpers for readability and single responsibility
  > Map Python types to SA column types (_sa_type_for) with Optional unwrapping
  > and collection → JSON fallback; mirrors _SCALAR_TYPE_MAP from introspection.py
  > Mirror SessionManager pool defaults: pool_pre_ping, echo, pool_size,
  > max_overflow, pool_timeout, pool_recycle, connect_args from config
  > Replace msgspec.structs.asdict cast hack with direct call via _structs_to_rows
  > Drop connection_string fallback — config contract is url only
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **streaming:** merge sink config from dsl and yaml
- **streaming:** move table sink into package
- **streaming:** resolve table sink config via context
- **streaming:** enforce typed table sink config



## ✅ Tests
### streaming
- **streaming:** add IntoTable SQLAlchemy integration tests against SQLite<br>
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **streaming:** cover resource manager session cache



## 🔖 Other
- Fix Bytewax step flat_map tests
- Refactor streaming validation helpers<br>
  > --------
  > Co-authored-by: Claude Sonnet 4.6 <noreply@anthropic.com>




# 🚀 Release 0.7.0 ([#25](https://github.com/the-reacher-data/loom-py/pull/25)) ([`0d8941a`](https://github.com/the-reacher-data/loom-py/commit/0d8941a724ec768b162a07e756bac3d53157ec85))


## ✨ Features
### config
- **config:** add StructBinder and migrate streaming binding resolution<br>
  > Introduces StructBinder in core/config/binder.py — a Strategy that
  > injects constructor arguments from a config mapping via msgspec.convert,
  > covering primitives, Literal constraints, and LoomFrozenStruct subclasses.
  > Migrates streaming _instantiate_binding to use it, removing ~30 lines of
  > private helpers and adding typed ConfigError on resolution failures.

- **config:** add ConfigContext for typed section extraction and binding resolution<br>
  > Single entry-point for runner and bootstrap code to read config:
  > section() extracts typed sub-trees, bind() injects constructor args
  > from a config path + overrides, resolve() materializes ConfigBinding
  > declarations. Accepts an optional StructBinder for strict-mode control.



## 🐛 Fixes
### core
- **core:** clean async bridge timeout and celery typing

### etl
- **etl:** align missing storage error with config contract




## ♻️ Refactor
### model
- **model:** migrate public config structs to LoomFrozenStruct

### streaming
- **streaming:** clean up _instantiate_binding before extraction<br>
  > Replace mutable dict() + .update() with a single dict unpacking
  > Remove the dead param.annotation fallback (get_type_hints already
  > resolves all forward refs; the fallback silently passed strings to
  > _is_struct_annotation which always returned False)
  > Extract _SKIP_KINDS constant to name the variadic-parameter guard
  > Raise ConfigError instead of TypeError for missing required struct
  > params (semantically a config error, not a type error; both are
  > caught by the enclosing _resolve_binding handler so no behaviour
  > change in streaming)
  > All 7 binding tests pass.


### core
- **core:** homogenize config and runner boundaries
- **core:** add runner and compiler protocols
- **core:** apply runner lifecycle protocols
- **core:** harden runner lifecycle abstraction<br>
  > shutdown_runner/flush_runner now catch and log exceptions instead of
  > propagating, preserving the original exception when called from finally
  > blocks; contract updated in docstrings and covered by new tests
  > CompilerProtocol drops @runtime_checkable — no production isinstance
  > usage existed; Protocol remains valid for static typing
  > StreamingRunner.prepare_run() calls shutdown_runner(self) before
  > overwriting self._shutdown, preventing resource leaks on double calls
  > _build_backend_options extracted from loom.celery.config into
  > loom.core.async_bridge.build_backend_options, eliminating duplication
  > between the Celery and streaming domains
  > _CeleryAsyncRuntime._signals_connected ClassVar removed; the
  > module-level _SIGNALS_CONNECTED flag is the single source of truth
  > for the process-level signal guard
  > ETLRunner.flush() delegates to ETLExecutor.flush(), which checks
  > SupportsFlush via isinstance instead of getattr duck-typing
  > TypeVars in compiler.py renamed to _RequestT/_PlanT to signal they
  > are implementation details of the Protocol, not public API


### etl
- **etl:** make flush an optional runner capability

### celery
- **celery:** adopt runner shutdown helper
- **celery:** replace _SIGNALS_CONNECTED sentinel with dispatch_uid<br>
  > Eliminates the module-level mutable boolean that violated the no-global-
  > mutable-state architecture rule. Idempotency is now handled by Celery's
  > dispatch_uid mechanism: disconnect+connect on each call ensures the most-
  > recently registered closure is always active, so a second bootstrap_worker
  > call wires up fresh dependencies rather than leaving stale closures from
  > the first call.
  > This is strictly better than the sentinel: no global state, no race
  > condition between threads calling bootstrap_worker concurrently, and
  > correct closure replacement when called multiple times in tests.


### rest
- **rest:** use observability config directly







# 🚀 Release 0.6.0 ([#22](https://github.com/the-reacher-data/loom-py/pull/22)) ([`5146569`](https://github.com/the-reacher-data/loom-py/commit/51465697115036ed05f620a1099272e4fd216501))


## ✨ Features
### core
- **core:** add unified ObservabilityRuntime<br>
  > Introduces the core observability package: LifecycleEvent, LifecycleObserver
  > protocol, ObservabilityConfig, and ObservabilityRuntime as a single fan-out
  > engine replacing the fragmented per-module observer wiring. Adds
  > StructlogLifecycleObserver, OtelLifecycleObserver, NoopObserver, and
  > PrometheusLifecycleAdapter (with Pushgateway support for ETL batch jobs).
  > Backward-compat re-exports (safe_observe, notify_observers) kept for the
  > streaming composite observer until the legacy cleanup commit.



## 🐛 Fixes
### observability
- **observability:** restore default bootstraps

### rest
- **rest:** correct camel-to-snake conversion for acronyms in filter fields

### streaming
- **streaming:** propagate trace ids through bytewax paths
- **streaming:** keep input trace ids through the micro
- **streaming:** bridge otel trace ids from messages


## 📖 Documentation
- align yaml config and dummy repo links
- restore dummy repo urls
- fix markdown links for streaming dummies

### prometheus
- **prometheus:** update KafkaPrometheusMetrics docstring metric names



## ♻️ Refactor
### observability
- **observability:** simplify otel log correlation
- **observability:** simplify otel exporter imports
- **observability:** expose ObservabilityRuntime in public API and clean architecture debt<br>
  > Export ObservabilityRuntime and LifecycleStatus from loom.core.observability package
  > Compute dict(meta) once in span() instead of three separate allocations
  > Replace _SIGNALS_CONNECTED module-level global with ClassVar on _CeleryAsyncRuntime
  > Add RuntimeError to bootstrap_worker Raises docstring
  > Fix TraceIdMiddleware docstring example (removed spurious ObservabilityRuntime reference)
  > Add missing observability_runtime param to _make_handler docstring

- **observability:** unify runtime across services
- **observability:** clean up dead code and docs

### streaming
- **streaming:** migrate to unified ObservabilityRuntime<br>
  > Replace StreamingObservabilityConfig and the manual observer construction
  > in _load_observability_runtime() with ObservabilityRuntime.from_config().
  > Observability config now lives under streaming.runtime.observability in YAML,
  > eliminating the separate streaming.observability section.
  > Delete src/loom/streaming/_observability.py (StreamingObservabilityConfig)
  > Delete src/loom/streaming/observability/ directory (old observer stack)
  > Add observability: ObservabilityConfig field to BytewaxRuntimeConfig
  > StreamingRunner.from_config() calls ObservabilityRuntime.from_config() directly
  > StreamingTestRunner defaults to ObservabilityRuntime.noop(); callers pass
  > observability_runtime= explicitly when needed
  > Update tests to use new config shape and ObservabilityRuntime([observer])

- **streaming:** replace KafkaStreamingObserver with LifecycleEvent/TRANSPORT<br>
  > Eliminates the parallel KafkaStreamingObserver protocol hierarchy (NoopKafkaObserver,
  > StructlogKafkaObserver) and models all Kafka transport events as LifecycleEvent with
  > Scope.TRANSPORT. KafkaPrometheusMetrics now implements the LifecycleObserver protocol
  > via a single on_event() dispatcher instead of four typed callback methods.

- **streaming:** preserve trace lineage across boundaries

### prometheus
- **prometheus:** add KafkaMetricName enum and drop loom_ prefix<br>
  > Extracts Prometheus metric names into a public KafkaMetricName StrEnum so
  > callers can reference metric names without magic strings. Removes the loom_
  > namespace prefix from all four Kafka instruments (produced_total,
  > consumed_total, encode_duration_seconds, decode_duration_seconds).

- **prometheus:** drop loom prefix from lifecycle metrics

### rest
- **rest:** use core observability runtime
- **rest:** read prometheus from observability config

### celery
- **celery:** adopt async bridge and runtime config

### etl
- **etl:** make spark pytest plugin opt-in



## ✅ Tests
### kafka
- **kafka:** update metric name assertions after loom_ prefix removal

### observability
- **observability:** cover lineage and runtime branches

### integration
- **integration:** add in-memory REST, observability, and bootstrap integration tests

### etl
- **etl:** cover prometheus flush on runner shutdown


## 🛠 Chores
### deps
- **deps:** bump click to 8.3.3




# 🚀 Release 0.5.0 ([#20](https://github.com/the-reacher-data/loom-py/pull/20)) ([`70f7cf8`](https://github.com/the-reacher-data/loom-py/commit/70f7cf85275fcc1f590e06f980dfac91cad50893))


## ✨ Features
### core
- **core:** add shared expression routing primitives

### streaming
- **streaming:** add process DSL and routing
- **streaming:** add kafka transport and observability
- **streaming:** allow task resource kwargs
- **streaming:** add scoped dependency nodes
- **streaming:** make tasks configurable

### config
- **config:** add declarative config bindings











# 🚀 Release 0.4.0 ([#18](https://github.com/the-reacher-data/loom-py/pull/18)) ([`09e5aa3`](https://github.com/the-reacher-data/loom-py/commit/09e5aa340f88e0488daca2bf10320aad7aebbe1d))


## ✨ Features
### etl
- **etl:** add IntoHistory builder and SCD Type 2 domain contracts











# 🚀 Release 0.3.0 ([#14](https://github.com/the-reacher-data/loom-py/pull/14)) ([`ef414c5`](https://github.com/the-reacher-data/loom-py/commit/ef414c5bfd303296af450840318dfbe9d301e5d1))


## ✨ Features
### config
- **config:** add cloud URI support and pluggable resolver extension point<br>
  > Add fsspec as a hard dependency of loom[config]
  > load_config() now accepts s3://, gs://, abfss://, r2:// URIs via fsspec
  > Add ConfigResolver protocol for pluggable ${prefix:key} resolution at
  > parse time (enables SSM, Key Vault, etc. without baking secrets into images)
  > Resolver registration is idempotent; resolvers are evaluated at job startup
  > so secret rotation takes effect on the next run
  > Migrate loom.etl.runner.config_loader to use core load_config, removing
  > the parallel OmegaConf implementation
  > ETL _load_yaml inherits cloud URI and resolver support transparently


### etl
- **etl:** add FileLocator with explicit alias API for file routes<br>
  > Introduces `FileLocator` protocol and `MappingFileLocator` so that
  > `FromFile.alias("name")` / `IntoFile.alias("name")` specs resolve at
  > runtime through `storage.files` config rather than hard-coded URIs.
  > `FileLocation` / `FileLocator` / `MappingFileLocator` in `storage/_file_locator.py`
  > `StorageConfig.to_file_locator()` returns `MappingFileLocator | None`
  > (None when `files` is empty — no conditional needed at call sites)
  > `FromFile.alias()` / `IntoFile.alias()` classmethods set `is_alias=True`
  > on the emitted spec
  > `is_alias: bool` added to `FileSourceSpec` and `FileSpec`
  > Polars and Spark backends resolve aliases via injected `file_locator`
  > Both providers wired: `file_locator = config.to_file_locator()`
  > Full test coverage across io, storage, and backend layers



## 🐛 Fixes
### observability
- **observability:** honor missing table policy for record store writers


## 📖 Documentation
### etl
- **etl:** keep only user guide and drop refactor docs
- **etl:** expand ETL documentation and update directory table<br>
  > Add dummy-loom-etl companion repo link in README and etl guide
  > Expand README subpaths table with loom.etl and loom.core.config entries
  > Add FileLocator/alias API, cloud config URI, and ConfigResolver sections to etl guide
  > Add loom.etl.backends (polars + spark) to etl.rst API reference
  > Add loom.core.config to core.rst API reference




## ♻️ Refactor
### stepsql
- **stepsql:** delegate SQL execution to backend readers





# 🚀 Release 0.2.1 ([#12](https://github.com/the-reacher-data/loom-py/pull/12)) ([`87f7d1f`](https://github.com/the-reacher-data/loom-py/commit/87f7d1f1eb1ccde71d0aca1c5584b83317e30707))

## ✨ Features

### logger
- **logger:** support per-logger levels from config<br>
  > `LoomConfig` now accepts a `loggers` mapping to override the log level per named logger. Resolves `structlog` / stdlib incompatibility when mixing loom-managed and third-party loggers.

### repository
- **repository:** generalize main repo registration for loom structs<br>
  > `repository_for` is now importable from `loom.core.repository` (top-level). The SQLAlchemy-specific import path still works but is no longer the canonical one.

## 🐛 Bug Fixes

### rest
- **rest:** serialize pagination envelopes in camel case<br>
  > `PageResult` and list-envelope responses were serialized in snake_case. All envelope fields now follow the camelCase contract of the HTTP layer.
- **rest:** support loom structs in autocrud tests<br>
  > Auto-CRUD route generation was not exercising the `msgspec.Struct` code path in integration tests.

### prometheus
- **prometheus:** expose metrics at exact path<br>
  > Metrics endpoint was registered with a trailing-slash variant that did not match the documented `/metrics` path.

### docs
- **docs:** fix RTD build failure, logo and docs examples (#10, #11)<br>
  > Mock `starlette`, `celery`, `kombu`, `redis` in `autodoc_mock_imports`. Logo resized to natural proportions with dark-mode safe background. Status badges added to index. Rule/Compute examples updated to named predicates.

---

# 🚀 Release 0.2.0 ([#9](https://github.com/the-reacher-data/loom-py/pull/9)) ([`2f669ab`](https://github.com/the-reacher-data/loom-py/commit/2f669ab205c7255eb6494e4cdb8ab8092817af62))

## ✨ Features

### cache
- **cache:** aiocache gateway with auto-inferred invalidation specs<br>
  > CachedRepository wraps any repository with read-through/write-through caching. ONE_TO_MANY depends_on specs are auto-generated from field annotations — no explicit declaration needed. Explicit depends_on always wins.

### celery
- **celery:** production-ready Celery integration layer<br>
  > CeleryJobService, persistent worker event loop, trace propagation, eager fallback, and task_default_queue routing so callbacks land on the correct consumed queue. bootstrap_worker compiles use cases, repositories, and registers Celery tasks in a single call.

- **celery:** worker job discovery from modules or manifest<br>
  > bootstrap_worker discovers and registers Job classes automatically from module include paths (mode: modules) or from a typed WorkerManifest (mode: manifest). WorkerManifest replaces scattered JOBS/USE_CASES/INTERFACES module attributes with a single typed contract.

- **celery:** interfaces= and use_cases= on bootstrap_worker<br>
  > Callbacks that call ApplicationInvoker need matching use-case keys compiled in the worker. interfaces= extracts use-case types from RestInterface route declarations (including AutoCRUD-generated ones). use_cases= handles non-AutoCRUD scenarios. Both can be combined with discovery mode.

### core
- **core:** typed repository abstractions and SQLAlchemy backend<br>
  > Async repository protocol (RepositoryRead, RepositoryWrite, RepoFor) backed by SQLAlchemy 2.0 async session. Struct-based model system using msgspec.Struct as the single source of truth — models compile to SA mapped classes at startup via compile_all(). count() and UPDATE RETURNING included as first-class operations.

- **core:** use-case DSL with field refs, compute, rules and typed markers<br>
  > Declarative use-case definition via Input, Load, LoadById, Exists, Compute and Rule markers. Signature inspection runs once at compile time; RuntimeExecutor drives execution from an immutable ExecutionPlan. No per-request reflection.

- **core:** ApplicationInvoker and named use-case registry<br>
  > Use cases and job callbacks invoke other use cases by type through ApplicationInvoker without direct coupling. A named registry maps use-case keys to compiled instances at bootstrap, providing a stable cross-invocation contract.

- **core:** compiled model artifact and cache entity keys<br>
  > compile_all() produces a typed CompiledCore artifact exposing stable entity keys used by the cache layer for deterministic repository-level invalidation across reads and writes.

- **core:** executor skips UoW for read-only use cases and GET routes<br>
  > UseCase.read_only=True and all GET routes bypass UoW.begin/commit, removing at minimum one BEGIN+COMMIT round-trip from every read request on PostgreSQL.

### job
- **job:** async job domain model and orchestration primitives<br>
  > Job[ResultT] base class with Celery routing ClassVars. JobHandle / JobGroup with dual-mode waiting (Celery + inline). JobCallback lifecycle with on_success/on_failure. Dispatch is transactionally safe — jobs flush on UoW commit and are cleared on rollback.

### observability
- **observability:** trace_id propagation and Prometheus adapter<br>
  > trace_id injected into every request context and propagated to job callbacks. MetricsAdapter protocol emits execution events; PrometheusAdapter records latency histograms and error counters with low cardinality labels.

### projection
- **projection:** compiler-driven memory/SQL routing<br>
  > Projections are source-agnostic at declaration time. The backend compiler decides at compile_all() whether each projection runs in-memory (relation already loaded in the active profile) or via SQL. Users declare only CountLoader, ExistsLoader, or JoinFieldsLoader — no source= parameter. Internal _Memory* and _Sql* loaders are synthesized at compile time.

### rest
- **rest:** AutoCRUD and FastAPI adapter<br>
  > RestInterface.auto=True generates full CRUD routes at class definition time via build_auto_routes(). OpenAPI contracts expose query params, pagination defaults, and decoupled CreateInput/UpdateInput write DTOs. Discovery engine mounts all declared interfaces at bootstrap.

## 📖 Documentation

- Sphinx documentation platform with full public guides<br>
  > Quickstart, use-case DSL reference, AutoCRUD guide, Celery integration guide (job definition, dispatch, callbacks, YAML reference, bootstrap options, ApplicationInvoker, Docker-compose stack), and dummy-loom examples-repo walkthrough. Deployed to Read the Docs.

## ⚡ Performance

### engine
- **engine:** UPDATE RETURNING replaces SELECT + flush + refresh<br>
  > SQLAlchemyUpdateMixin.update() issues a single UPDATE ... RETURNING round-trip. Server-side onupdate expressions are pre-computed at init time and injected into the SET clause automatically.

### repository
- **repository:** single-query total count for offset pagination<br>
  > list_with_query with PaginationMode.OFFSET issues a single SELECT COUNT(*) instead of a separate full-table scan, eliminating one round-trip per paginated list operation.
