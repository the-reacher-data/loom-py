# Celery Worker

`loom-kernel` can bootstrap Celery workers from explicit jobs or from discovery (`modules`/`manifest`).

## Install

```bash
pip install "loom-kernel[celery]"
```

## Option 1: Explicit jobs list

```python
from loom.celery import create_app

from app.jobs import RecalculatePricesJob

celery_app = create_app(
    "config/base.yaml",
    "config/worker.yaml",
    jobs=[RecalculatePricesJob],
)
```

## Option 2: Discovery from config

You can omit `jobs=[...]` and let Loom discover jobs from `app.discovery`.

### Modules mode

```yaml
app:
  discovery:
    mode: modules
    modules:
      include:
        - app.jobs
        - app.use_cases

celery:
  broker_url: redis://redis:6379/0
  result_backend: redis://redis:6379/1
```

### Manifest mode

```yaml
app:
  discovery:
    mode: manifest
    manifest:
      module: app.manifest
```

```python
# app/manifest.py
from app.jobs import RecalculatePricesJob, RebuildSearchIndexJob

MODELS = []
USE_CASES = []
INTERFACES = []
JOBS = [RecalculatePricesJob, RebuildSearchIndexJob]
```

```python
from loom.celery import create_app

celery_app = create_app("config/worker.yaml")
```

## Notes

- Worker discovery supports `modules` and `manifest`.
- `interfaces` discovery is for REST bootstrap, not worker task registration.
- If no explicit `jobs` are passed and discovery finds no `JOBS`, bootstrap raises an error.
