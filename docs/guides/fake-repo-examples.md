# Fake repo examples

This project contains a documented example in `docs/examples-repo/index.md`.

Recommended reading order:

1. Product commands and models.
2. Use cases with `Rule` and `Compute`.
3. Custom repository contract and SQLAlchemy implementation.
4. Interface declaration and route wiring.
5. Integration tests that validate end-to-end behavior using these examples.

These examples are the reference implementation for DSL usage and expected behavior.

Useful files:

- `tests/integration/fake_repo/product/repository_contract.py`
- `tests/integration/fake_repo/product/repository.py`
- `tests/integration/fake_repo/src/app/product/use_cases.py`
- `tests/integration/core/use_case/test_use_case_crud_integration.py`
- `tests/integration/core/use_case/test_custom_repository_integration.py`

Custom repository pattern:

- Declare the custom contract as `RepoFor[Model]` plus only the extra methods.
- Import `repository_for` from `loom.core.repository`, not from the SQLAlchemy package.
- Register the SQLAlchemy implementation with `@repository_for(Model, contract=...)`.
- Keep CRUD-only use cases on `self.main_repo`; they automatically receive the custom implementation.
- Reserve constructor-injected repository contracts for advanced cases where a use case or job needs a secondary repository dependency; the primary public example should stay on `main_repo`.
- `repository_for(...)` also works for non-persistible logical types:
  - use `LoomStruct` for neutral internal structs
  - use `Response` for API-facing structs that should serialize in `camelCase`
- The SQLAlchemy bootstrap module still provides the current default fallback repository builder for `BaseModel`, but explicit `repository_for(...)` bindings take precedence for both persistible and non-persistible types.
