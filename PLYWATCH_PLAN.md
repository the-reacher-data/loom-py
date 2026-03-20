# Plywatch plan

## Goal

Bootstrap `plywatch` as a new open-source product focused on modern Celery monitoring with:

- single pod deployment in v1
- ephemeral state
- `aiocache` backend
- backend built with `loom-py`
- frontend built with SvelteKit
- better UX and project structure than Flower


## Naming

- Product: `Plywatch`
- Repo: `plywatch`
- Python package: `plywatch`
- Frontend title: `Plywatch`


## What we already have

We are starting with these base specifications:

- frontend architecture for SvelteKit + SSE
- product analysis for ephemeral Celery monitoring
- delivery plan with phases, repo structure and milestones


## Phase 0

Create the repo skeleton and keep scope tight.

Deliverables:

- workspace created
- specs copied
- `.claude` copied
- initial repo structure agreed


## Phase 1

Define the repo structure and contracts.

Deliverables:

- backend app structure
- frontend app structure
- environment variables contract
- event model contract
- API contract


## Phase 2

Build backend MVP.

Deliverables:

- Celery event ingestion
- ephemeral cache store
- retention by max tasks and max age
- tasks, workers, queues and overview APIs
- SSE endpoint


## Phase 3

Build frontend MVP.

Deliverables:

- dashboard
- tasks table
- task detail
- workers view
- queues view
- live updates


## Phase 4

Add visual analytics.

Deliverables:

- throughput charts
- failure charts
- retry charts
- worker processing charts


## Phase 5

Prepare future operator controls.

Deliverables:

- action API design
- revoke strategy analysis
- safe control UX


## Immediate next steps

1. Create `plywatch` folder.
2. Copy the current specs and `.claude`.
3. Create the initial repo tree.
4. Decide whether v1 serves frontend assets from the backend image.
5. Start with backend config and event ingestion spike.
