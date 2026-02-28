# Clean architecture rules

Project invariants:

1. Domain/Application layers must not import infrastructure.
2. Repositories are interface-driven.
3. Runtime side effects are explicit and observable.
4. Public APIs are typed and documented.

This contract is enforced through module boundaries and strict typing checks.
