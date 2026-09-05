"""Tests for ``includes`` resolution on any scheme, with globs."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from loom.core.config import ConfigError, expand_config_glob, load_config
from loom.core.config._includes import canonical_key, expand_include, resolve_include

_PARTS = {"parts/b.yaml": "b: 1\n", "parts/a.yaml": "a: 1\n", "parts/c.yaml": "c: 1\nb: 0\n"}


def _write_tree(root: Path, files: dict[str, str]) -> None:
    for name, content in files.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)


@pytest.fixture
def memfs() -> Iterator[Any]:
    fsspec = pytest.importorskip("fsspec")
    fs = fsspec.filesystem("memory")
    fs.store.clear()
    fs.pseudo_dirs[:] = [""]
    yield fs
    fs.store.clear()
    fs.pseudo_dirs[:] = [""]


def _pipe_tree(fs: Any, root: str, files: dict[str, str]) -> None:
    for name, content in files.items():
        fs.pipe(f"{root}/{name}", content.encode())


def _as_dict(cfg: Any) -> Any:
    from omegaconf import OmegaConf

    return OmegaConf.to_container(cfg, resolve=True)


# ---------------------------------------------------------------------------
# US1 scenario 1 — local glob, lexicographic order, own keys on top
# ---------------------------------------------------------------------------


def test_local_glob_merges_matches_in_order_then_own_keys(tmp_path: Path) -> None:
    _write_tree(tmp_path, {**_PARTS, "a.yaml": "includes:\n  - parts/*.yaml\nc: 2\n"})

    cfg = load_config(str(tmp_path / "a.yaml"))

    assert _as_dict(cfg) == {"a": 1, "b": 0, "c": 2}


def test_local_glob_ignores_directories_and_non_yaml_files(tmp_path: Path) -> None:
    _write_tree(tmp_path, {**_PARTS, "a.yaml": "includes:\n  - parts/*\n"})
    (tmp_path / "parts" / "dir.yaml").mkdir()
    (tmp_path / "parts" / "notes.txt").write_text("x: 9\n")

    cfg = load_config(str(tmp_path / "a.yaml"))

    assert _as_dict(cfg) == {"a": 1, "b": 0, "c": 1}


def test_local_glob_accepts_yml_suffix(tmp_path: Path) -> None:
    _write_tree(tmp_path, {"parts/x.yml": "x: 1\n", "a.yaml": "includes:\n  - parts/*\n"})

    cfg = load_config(str(tmp_path / "a.yaml"))

    assert _as_dict(cfg) == {"x": 1}


def test_absolute_local_glob_entry(tmp_path: Path) -> None:
    _write_tree(tmp_path, _PARTS)
    other = tmp_path / "elsewhere"
    other.mkdir()
    (other / "a.yaml").write_text(f"includes:\n  - {tmp_path / 'parts' / '*.yaml'}\n")

    cfg = load_config(str(other / "a.yaml"))

    assert _as_dict(cfg) == {"a": 1, "b": 0, "c": 1}


def test_parent_relative_glob_entry(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {"shared/s.yaml": "s: 1\n", "app/a.yaml": "includes:\n  - ../shared/*.yaml\n"},
    )

    cfg = load_config(str(tmp_path / "app" / "a.yaml"))

    assert _as_dict(cfg) == {"s": 1}


# ---------------------------------------------------------------------------
# US1 scenario 2 — same tree on memory://
# ---------------------------------------------------------------------------


def test_memory_uri_glob_equals_local_result(tmp_path: Path, memfs: Any) -> None:
    tree = {**_PARTS, "a.yaml": "includes:\n  - parts/*.yaml\nc: 2\n"}
    _write_tree(tmp_path, tree)
    _pipe_tree(memfs, "cfg", tree)

    local = load_config(str(tmp_path / "a.yaml"))
    cloud = load_config("memory://cfg/a.yaml")

    assert _as_dict(cloud) == _as_dict(local) == {"a": 1, "b": 0, "c": 2}


def test_cloud_plain_include_resolves_relative_to_including_uri(memfs: Any) -> None:
    _pipe_tree(
        memfs,
        "cfg",
        {"app/a.yaml": "includes:\n  - ../base.yaml\nname: app\n", "base.yaml": "pool: 3\n"},
    )

    cfg = load_config("memory://cfg/app/a.yaml")

    assert _as_dict(cfg) == {"pool": 3, "name": "app"}


def test_cloud_glob_ignores_directories_and_non_yaml_files(memfs: Any) -> None:
    _pipe_tree(memfs, "cfg", {**_PARTS, "a.yaml": "includes:\n  - parts/*\n"})
    memfs.mkdir("cfg/parts/dir.yaml")
    memfs.pipe("cfg/parts/notes.txt", b"x: 9\n")

    cfg = load_config("memory://cfg/a.yaml")

    assert _as_dict(cfg) == {"a": 1, "b": 0, "c": 1}


# ---------------------------------------------------------------------------
# US1 scenario 3 — mixed schemes
# ---------------------------------------------------------------------------


def test_local_file_includes_cloud_uri(tmp_path: Path, memfs: Any) -> None:
    memfs.pipe("cfg/remote.yaml", b"remote: 1\n")
    (tmp_path / "a.yaml").write_text("includes:\n  - memory://cfg/remote.yaml\nlocal: 1\n")

    cfg = load_config(str(tmp_path / "a.yaml"))

    assert _as_dict(cfg) == {"remote": 1, "local": 1}


def test_cloud_file_includes_absolute_local_path(tmp_path: Path, memfs: Any) -> None:
    (tmp_path / "local.yaml").write_text("local: 1\n")
    memfs.pipe("cfg/a.yaml", f"includes:\n  - {tmp_path / 'local.yaml'}\nremote: 1\n".encode())

    cfg = load_config("memory://cfg/a.yaml")

    assert _as_dict(cfg) == {"local": 1, "remote": 1}


def test_cloud_file_includes_local_glob(tmp_path: Path, memfs: Any) -> None:
    _write_tree(tmp_path, _PARTS)
    memfs.pipe("cfg/a.yaml", f"includes:\n  - {tmp_path / 'parts' / '*.yaml'}\n".encode())

    cfg = load_config("memory://cfg/a.yaml")

    assert _as_dict(cfg) == {"a": 1, "b": 0, "c": 1}


# ---------------------------------------------------------------------------
# US1 scenario 4 — no match / not found names the entry and the declaring file
# ---------------------------------------------------------------------------


def test_local_glob_with_no_match_raises(tmp_path: Path) -> None:
    main = tmp_path / "a.yaml"
    main.write_text("includes:\n  - parts/*.yaml\n")

    with pytest.raises(ConfigError) as info:
        load_config(str(main))

    assert "parts/*.yaml" in str(info.value)
    assert f"included from {str(main)!r}" in str(info.value)


def test_local_glob_matching_only_non_yaml_is_no_match(tmp_path: Path) -> None:
    (tmp_path / "parts").mkdir()
    (tmp_path / "parts" / "notes.txt").write_text("x: 1\n")
    main = tmp_path / "a.yaml"
    main.write_text("includes:\n  - parts/*\n")

    with pytest.raises(ConfigError, match="matches no configuration file"):
        load_config(str(main))


def test_local_plain_include_missing_keeps_not_found_and_names_declaring(
    tmp_path: Path,
) -> None:
    main = tmp_path / "a.yaml"
    main.write_text("includes:\n  - nonexistent.yaml\n")

    with pytest.raises(ConfigError) as info:
        load_config(str(main))

    message = str(info.value)
    assert "Configuration file not found" in message
    assert "nonexistent.yaml" in message
    assert f"(included from {str(main)!r})" in message


def test_cloud_glob_with_no_match_raises(memfs: Any) -> None:
    memfs.pipe("cfg/a.yaml", b"includes:\n  - parts/*.yaml\n")

    with pytest.raises(ConfigError) as info:
        load_config("memory://cfg/a.yaml")

    assert "memory://cfg/parts/*.yaml" in str(info.value)
    assert "included from 'memory://cfg/a.yaml'" in str(info.value)


def test_cloud_plain_include_missing_names_declaring(memfs: Any) -> None:
    memfs.pipe("cfg/a.yaml", b"includes:\n  - missing.yaml\n")

    with pytest.raises(ConfigError) as info:
        load_config("memory://cfg/a.yaml")

    assert "memory://cfg/missing.yaml" in str(info.value)
    assert "included from 'memory://cfg/a.yaml'" in str(info.value)


def test_included_parse_failure_names_declaring(memfs: Any) -> None:
    memfs.pipe("cfg/a.yaml", b"includes:\n  - broken.yaml\n")
    memfs.pipe("cfg/broken.yaml", b"key: [unclosed\n")

    with pytest.raises(ConfigError) as info:
        load_config("memory://cfg/a.yaml")

    assert "Failed to parse" in str(info.value)
    assert "included from 'memory://cfg/a.yaml'" in str(info.value)


def test_nested_include_error_names_the_direct_declaring_file(tmp_path: Path) -> None:
    _write_tree(
        tmp_path, {"a.yaml": "includes:\n  - b.yaml\n", "b.yaml": "includes:\n  - c.yaml\n"}
    )

    with pytest.raises(ConfigError) as info:
        load_config(str(tmp_path / "a.yaml"))

    assert f"included from {str(tmp_path / 'b.yaml')!r}" in str(info.value)


# ---------------------------------------------------------------------------
# US1 scenario 5 — cycles on cloud URIs
# ---------------------------------------------------------------------------


def test_cloud_circular_include_raises(memfs: Any) -> None:
    _pipe_tree(
        memfs, "cfg", {"a.yaml": "includes:\n  - b.yaml\n", "b.yaml": "includes:\n  - a.yaml\n"}
    )

    with pytest.raises(ConfigError, match="[Cc]ircular"):
        load_config("memory://cfg/a.yaml")


def test_cloud_cycle_detected_through_parent_segment(memfs: Any) -> None:
    _pipe_tree(
        memfs,
        "cfg",
        {"a.yaml": "includes:\n  - x/b.yaml\n", "x/b.yaml": "includes:\n  - ../x/../a.yaml\n"},
    )

    with pytest.raises(ConfigError, match="[Cc]ircular"):
        load_config("memory://cfg/a.yaml")


def test_canonical_key_normalises_cloud_path() -> None:
    assert canonical_key("s3://b/x/../a.yaml") == canonical_key("s3://b/a.yaml") == "s3://b/a.yaml"


def test_canonical_key_resolves_local_path(tmp_path: Path) -> None:
    assert canonical_key(str(tmp_path / "x" / ".." / "a.yaml")) == str(
        (tmp_path / "a.yaml").resolve()
    )


# ---------------------------------------------------------------------------
# US1 scenario 6 / edge cases — local path needs no fsspec
# ---------------------------------------------------------------------------


def test_local_includes_and_globs_work_without_fsspec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_tree(
        tmp_path,
        {
            **_PARTS,
            "base.yaml": "base: 1\n",
            "a.yaml": "includes:\n  - base.yaml\n  - parts/*.yaml\n",
        },
    )
    monkeypatch.setitem(sys.modules, "fsspec", None)
    monkeypatch.setitem(sys.modules, "fsspec.core", None)

    cfg = load_config(str(tmp_path / "a.yaml"))

    assert _as_dict(cfg) == {"base": 1, "a": 1, "b": 0, "c": 1}


def test_include_entry_interpolation_resolves_before_glob(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VARIANT", "prod")
    _write_tree(
        tmp_path, {"prod/x.yaml": "x: 1\n", "a.yaml": "includes:\n  - ${oc.env:VARIANT}/*.yaml\n"}
    )

    cfg = load_config(str(tmp_path / "a.yaml"))

    assert _as_dict(cfg) == {"x": 1}


# ---------------------------------------------------------------------------
# resolve_include / expand_include / expand_config_glob
# ---------------------------------------------------------------------------


def test_resolve_include_keeps_scheme_and_absolute_entries(tmp_path: Path) -> None:
    assert resolve_include("s3://b/a.yaml", "gs://o/x.yaml") == "gs://o/x.yaml"
    assert resolve_include("s3://b/a.yaml", "/etc/x.yaml") == "/etc/x.yaml"
    assert resolve_include(str(tmp_path / "a.yaml"), "s3://b/x.yaml") == "s3://b/x.yaml"


def test_resolve_include_joins_relative_entries(tmp_path: Path) -> None:
    assert resolve_include("s3://b/x/a.yaml", "../parts/*.yaml") == "s3://b/parts/*.yaml"
    assert resolve_include(str(tmp_path / "x" / "a.yaml"), "../p/*.yaml") == str(
        tmp_path / "p" / "*.yaml"
    )


def test_expand_include_returns_plain_entry_untouched(tmp_path: Path) -> None:
    assert expand_include("s3://b/a.yaml") == ["s3://b/a.yaml"]
    assert expand_include(str(tmp_path / "missing.yaml")) == [str(tmp_path / "missing.yaml")]


def test_expand_config_glob_local_sorted(tmp_path: Path) -> None:
    _write_tree(tmp_path, _PARTS)

    assert expand_config_glob(str(tmp_path / "parts" / "*.yaml")) == [
        str(tmp_path / "parts" / "a.yaml"),
        str(tmp_path / "parts" / "b.yaml"),
        str(tmp_path / "parts" / "c.yaml"),
    ]


def test_expand_config_glob_raises_on_no_match(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="matches no configuration file"):
        expand_config_glob(str(tmp_path / "*.yaml"))


def test_cloud_glob_sorts_and_restores_protocol_on_bare_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("fsspec")
    from fsspec.spec import AbstractFileSystem

    class FakeFileSystem(AbstractFileSystem):  # type: ignore[misc]
        protocol = "fake"

        def glob(
            self, path: str, maxdepth: int | None = None, **kwargs: Any
        ) -> dict[str, dict[str, Any]]:
            return {
                "cfg/parts/b.yaml": {"type": "file"},
                "cfg/parts/a.yml": {"type": "file"},
                "cfg/parts/sub": {"type": "directory"},
                "cfg/parts/c.txt": {"type": "file"},
            }

    fake = FakeFileSystem()
    monkeypatch.setattr("fsspec.core.url_to_fs", lambda uri, **kw: (fake, "cfg/parts/*"))

    assert expand_include("s3://cfg/parts/*") == [
        "fake://cfg/parts/a.yml",
        "fake://cfg/parts/b.yaml",
    ]


def test_cloud_glob_wraps_backend_errors_naming_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("fsspec")

    def boom(uri: str, **kw: Any) -> Any:
        raise PermissionError("access denied")

    monkeypatch.setattr("fsspec.core.url_to_fs", boom)

    with pytest.raises(ConfigError, match=r"s3://locked/\*\.yaml.*access denied"):
        expand_include("s3://locked/*.yaml")
