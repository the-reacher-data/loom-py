# Use case DSL

This guide covers the practical DSL patterns used in `loom-kernel` use cases.

## Core primitives

- `F(CommandClass).field`: typed field reference.
- `Compute.set(...).from_command(...)`: deterministic command transformation.
- `Rule.check(...)`: field validation returning an error message.
- `Rule.forbid(...)`: forbidden condition validation.
- `when_present(...)`: run only when selected fields are present.

## Create flow

```python
create_name_normalize = Compute.set(F(CreateProduct).name).from_command(
    F(CreateProduct).name,
    via=normalize_text,
)

create_price_rule = Rule.check(
    F(CreateProduct).price,
    via=price_positive_error,
)
```

## Patch/update flow

```python
update_name_normalize = (
    Compute.set(F(UpdateProduct).name)
    .from_command(F(UpdateProduct).name, via=normalize_text)
    .when_present(F(UpdateProduct).name)
)

update_not_empty = Rule.forbid(
    patch_is_empty,
    message="at least one field must be provided",
).from_command()

update_price_rule = Rule.check(
    F(UpdateProduct).price,
    via=price_positive_error,
).when_present(F(UpdateProduct).price)
```

## Runtime params in rules

Use `from_params(...)` when validation needs execute params (for example `product_id`):

```python
system_name_immutable = (
    Rule.forbid(
        system_name_change_forbidden,
        message="system product name cannot be changed",
    )
    .from_command(F(UpdateProduct).name)
    .from_params("product_id")
    .when_present(F(UpdateProduct).name)
)
```

## Composed predicates

Predicates support `AND` and `OR` composition using `&` and `|`:

```python
rule = Rule.forbid(
    name_matches_price,
    message="name cannot be equal to price",
).from_command(F(UpdateProduct).name, F(UpdateProduct).price).when_present(
    F(UpdateProduct).name & F(UpdateProduct).price
)
```

Internally, the DSL uses a typed predicate operator enum and avoids string literals.
