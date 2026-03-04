# Projection Loaders Agnostic Architecture

## Context

Current projection loaders (`CountLoader`, `ExistsLoader`, `JoinFieldsLoader`) live in the SQLAlchemy adapter and require `AsyncSession`.
This is efficient for DB batch loading, but it couples projection logic to one persistence backend.

`ComputedFromRelationLoader` introduces an in-memory path that avoids extra queries when relation data is already loaded, but the current contracts are still SQLAlchemy-centric.

## Problem

We need projection loaders to be backend-agnostic so the same domain projection intent can work with:

1. SQLAlchemy
2. Future adapters (other ORMs/storage)
3. In-memory relation-computed projections

Without breaking public APIs already used by users.

## Goals

1. Keep `ProjectionField(loader=...)` public API stable.
2. Support both in-memory and backend query strategies.
3. Avoid extra DB round-trips when relations are already loaded.
4. Enable new persistence adapters without rewriting domain projections.

## Non-Goals

1. Rewriting all existing SQLAlchemy loaders in one release.
2. Breaking current SQLAlchemy projection behavior.

## Proposed Design

### 1) Core projection loader port

Introduce a backend-agnostic projection port in core:

- `ProjectionLoaderPort`
- `ProjectionContext`

`ProjectionContext` is an abstraction that can provide:

1. Loaded objects/relations for in-memory computation.
2. Adapter query gateway for backend batch lookups.

No `AsyncSession` in core signatures.

### 2) Dual execution strategy

Projection runtime should support two optional capabilities:

1. `compute_many(objs, context)` for in-memory computation.
2. `load_many(ids, context)` for backend batch loading.

Resolution policy:

1. Prefer in-memory path when relation data is loaded.
2. Fallback to backend batch path when needed.

### 3) SQLAlchemy adapter context

Provide `SqlAlchemyProjectionContext` in infrastructure:

1. Wraps `AsyncSession`.
2. Exposes helper operations needed by SQLAlchemy-backed loaders.

### 4) Backward compatibility

Keep current loaders working by adapter shims:

1. Existing SQLAlchemy loaders continue implementing DB path.
2. `ComputedFromRelationLoader` becomes first-class in-memory path.
3. Runtime supports both old and new contracts during migration.

## Runtime Policy for `with_details`

When profile includes relation `notes` and projection uses that same relation:

1. Compute `has_notes`, `notes_count`, `note_snippets` from loaded relation.
2. Do not issue additional projection queries for those fields.

This is the critical optimization for detail endpoints.

## Migration Plan

1. Add core interfaces (`ProjectionLoaderPort`, `ProjectionContext`).
2. Add SQLAlchemy context adapter.
3. Update projection runtime to resolve in-memory-first, DB-fallback.
4. Keep existing loader APIs functional through compatibility adapters.
5. Add parity tests:
   - same output with and without preloaded relations
   - no extra DB calls for in-memory-capable projections
6. Incrementally migrate built-in loaders to the new contract.

## Risks

1. More abstraction can increase complexity.
2. Mixed old/new loaders may create edge-case behavior if resolution order is unclear.
3. Need strong tests around profile loading guarantees.

## Acceptance Criteria

1. No public API break for current `ProjectionField(loader=...)` users.
2. `with_details` + relation-derived projections execute with zero extra projection queries.
3. SQLAlchemy tests remain green.
4. New contract documented with migration examples.
