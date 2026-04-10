"""Investigation tests for checkpoint async write behavior.

DO NOT MERGE TO MAIN - These tests are for diagnosing the sink_ipc timing issue.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import polars as pl
import pytest

from loom.etl.checkpoint._backends._polars import _PolarsCheckpointBackend


@pytest.fixture
def debug_checkpoint_dir(tmp_path: Path):
    """Fixture that creates dir, yields path, and keeps files for inspection."""
    base = tmp_path / "checkpoints"
    base.mkdir(parents=True, exist_ok=True)
    print(f"\n[DEBUG] Created base directory: {base}")
    yield str(base)
    # NO cleanup - files remain for inspection
    print(f"\n[DEBUG] Files remain at: {base}")
    if base.exists():
        for subdir in base.iterdir():
            if subdir.is_dir():
                files = list(subdir.iterdir())
                print(f"[DEBUG] Files in {subdir.name}/: {[f.name for f in files]}")


def test_reproduce_async_issue_no_delay(debug_checkpoint_dir: str) -> None:
    """Reproduce: segundo write falla porque el primero aún no escribió."""
    backend = _PolarsCheckpointBackend(storage_options={})
    base = debug_checkpoint_dir

    first = pl.DataFrame({"id": [1], "amount": [10.5]}).lazy()
    second = pl.DataFrame({"id": ["2"], "extra": ["x"]}).lazy()

    print("\n[TEST] Writing first frame...")
    backend.write("orders", base, first, append=True)

    # Verificar estado inmediatamente después del write
    orders_dir = Path(base) / "orders"
    files_after_first = list(orders_dir.iterdir())
    print(f"[TEST] Files after first write: {len(files_after_first)}")
    for f in files_after_first:
        size = f.stat().st_size if f.exists() else "N/A"
        print(f"  - {f.name} (exists: {f.exists()}, size: {size})")

    print("\n[TEST] Writing second frame (inmediatamente)...")
    try:
        backend.write("orders", base, second, append=True)
        print("[TEST] Second write OK")
    except Exception as e:
        print(f"[TEST] Second write FAILED: {e}")
        raise


def test_with_delay_between_writes(debug_checkpoint_dir: str) -> None:
    """Validar hipótesis: con delay de 5s, ¿el segundo write funciona?"""
    backend = _PolarsCheckpointBackend(storage_options={})
    base = debug_checkpoint_dir

    first = pl.DataFrame({"id": [1], "amount": [10.5]}).lazy()
    second = pl.DataFrame({"id": ["2"], "extra": ["x"]}).lazy()

    print("\n[TEST] Writing first frame...")
    backend.write("orders", base, first, append=True)

    print("[TEST] Esperando 5 segundos...")
    time.sleep(5)

    # Verificar estado después del delay
    orders_dir = Path(base) / "orders"
    files = list(orders_dir.iterdir())
    print(f"[TEST] Files after delay: {len(files)}")
    for f in files:
        print(f"  - {f.name} (exists: {f.exists()})")

    print("\n[TEST] Writing second frame...")
    try:
        backend.write("orders", base, second, append=True)
        print("[TEST] Second write OK - hipótesis confirmada!")
    except Exception as e:
        print(f"[TEST] Second write FAILED: {e}")
        raise


def test_check_files_exist_before_second_write(debug_checkpoint_dir: str) -> None:
    """Debug detallado: ¿Qué ve exactamente _find_arrow?"""
    backend = _PolarsCheckpointBackend(storage_options={})
    base = debug_checkpoint_dir

    first = pl.DataFrame({"id": [1], "amount": [10.5]}).lazy()

    print("\n[TEST] Writing first frame...")
    backend.write("orders", base, first, append=True)

    # Inmediatamente verificar qué ve _find_arrow
    print("\n[TEST] Verificando qué ve _find_arrow inmediatamente después:")
    found = backend._find_arrow("orders", base)
    print(f"  _find_arrow returned: {found}")

    # Verificar con glob manual
    import glob

    pattern = os.path.join(base, "orders", "*.arrow")
    manual_glob = glob.glob(pattern)
    print(f"  Manual glob '{pattern}': {manual_glob}")

    # Listar directorio
    orders_dir = Path(base) / "orders"
    if orders_dir.exists():
        all_files = list(orders_dir.iterdir())
        print(f"  os.listdir: {[f.name for f in all_files]}")
        for f in all_files:
            stat = f.stat() if f.exists() else None
            print(f"    {f.name}: exists={f.exists()}, size={stat.st_size if stat else 'N/A'}")
    else:
        print("  orders/ directory does not exist!")


def test_concurrent_writes_may_cause_issues(debug_checkpoint_dir: str) -> None:
    """Simular problema de concurrencia: dos writes simultáneos."""
    import threading

    backend = _PolarsCheckpointBackend(storage_options={})
    base = debug_checkpoint_dir
    errors = []

    def write_frame(name: str, data: pl.LazyFrame):
        try:
            print(f"\n[THREAD {name}] Starting write...")
            backend.write("orders", base, data, append=True)
            print(f"[THREAD {name}] Write OK")
        except Exception as e:
            print(f"[THREAD {name}] Write FAILED: {e}")
            errors.append((name, e))

    first = pl.DataFrame({"id": [1], "amount": [10.5]}).lazy()
    second = pl.DataFrame({"id": ["2"], "extra": ["x"]}).lazy()

    print("\n[TEST] Lanzando dos writes concurrentes...")
    t1 = threading.Thread(target=write_frame, args=("first", first))
    t2 = threading.Thread(target=write_frame, args=("second", second))

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    if errors:
        print(f"\n[TEST] Errors encontrados: {errors}")
        raise errors[0][1]
    else:
        print("\n[TEST] Ambos writes completaron (pero ¿con schema correcto?)")
        # Verificar resultado final
        scanned = backend.probe("orders", base)
        if scanned is not None:
            out = scanned.collect()
            print(f"[TEST] Resultado: {out.to_dict()}")
