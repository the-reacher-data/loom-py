"""Tests for keyed collections merged by key across ``includes`` and explicit layers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom.core.config import ConfigContext, ConfigError, load_config

KEYED = ("things.items",)

_MAPPING_A = "things:\n  items:\n    alpha: {size: 1}\n"
_MAPPING_B = "things:\n  items:\n    beta: {size: 2}\n"
_MAPPING_A_DUP = "things:\n  items:\n    alpha: {size: 9}\n"
_LIST_A = "things:\n  items:\n    - {name: alpha}\n"
_LIST_B = "things:\n  items:\n    - {name: beta}\n"


def _write_tree(root: Path, files: dict[str, str]) -> None:
    for name, content in files.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)


def _as_dict(cfg: Any) -> Any:
    from omegaconf import OmegaConf

    return OmegaConf.to_container(cfg, resolve=True)


def _includes(*entries: str) -> str:
    return "includes:\n" + "".join(f"  - {entry}\n" for entry in entries)


# ---------------------------------------------------------------------------
# US2 scenario 2 — mappings from two includes merge by key
# ---------------------------------------------------------------------------


def test_mappings_from_two_includes_merge_into_union(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {"a.yaml": _MAPPING_A, "b.yaml": _MAPPING_B, "root.yaml": _includes("a.yaml", "b.yaml")},
    )

    cfg = load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

    assert _as_dict(cfg) == {"things": {"items": {"alpha": {"size": 1}, "beta": {"size": 2}}}}


def test_keyed_path_absent_from_every_layer_is_accepted(tmp_path: Path) -> None:
    _write_tree(tmp_path, {"a.yaml": "x: 1\n", "root.yaml": _includes("a.yaml") + "y: 2\n"})

    cfg = load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

    assert _as_dict(cfg) == {"x": 1, "y": 2}


# ---------------------------------------------------------------------------
# US2 scenario 3 — duplicate key inside an includes composition
# ---------------------------------------------------------------------------


def test_duplicate_key_across_two_includes_names_key_and_both_files(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {
            "a.yaml": _MAPPING_A,
            "c.yaml": _MAPPING_A_DUP,
            "root.yaml": _includes("a.yaml", "c.yaml"),
        },
    )

    with pytest.raises(ConfigError, match=r"things\.items\['alpha'\]") as exc_info:
        load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

    message = str(exc_info.value)
    assert "a.yaml" in message
    assert "c.yaml" in message


def test_duplicate_through_nested_include_names_declaring_files(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {
            "a.yaml": _MAPPING_A,
            "b.yaml": _includes("c.yaml"),
            "c.yaml": _MAPPING_A_DUP,
            "root.yaml": _includes("a.yaml", "b.yaml"),
        },
    )

    with pytest.raises(ConfigError) as exc_info:
        load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

    message = str(exc_info.value)
    assert "a.yaml" in message
    assert "c.yaml" in message
    assert "b.yaml" not in message.split("(included from")[0]


def test_duplicate_between_included_file_and_declaring_file_names_both(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {"a.yaml": _MAPPING_A, "root.yaml": _includes("a.yaml") + _MAPPING_A_DUP},
    )

    with pytest.raises(ConfigError, match=r"things\.items\['alpha'\]") as exc_info:
        load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

    message = str(exc_info.value)
    assert "a.yaml" in message
    assert "root.yaml" in message


def test_from_yaml_forwards_keyed_to_the_loader(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {
            "a.yaml": _MAPPING_A,
            "c.yaml": _MAPPING_A_DUP,
            "root.yaml": _includes("a.yaml", "c.yaml"),
        },
    )

    with pytest.raises(ConfigError, match=r"things\.items\['alpha'\]"):
        ConfigContext.from_yaml(str(tmp_path / "root.yaml"), keyed=KEYED)


# ---------------------------------------------------------------------------
# US2 scenario 4 — explicit layers override by key
# ---------------------------------------------------------------------------


def test_duplicate_across_explicit_layers_later_wins(tmp_path: Path) -> None:
    _write_tree(tmp_path, {"base.yaml": _MAPPING_A, "prod.yaml": _MAPPING_A_DUP})

    cfg = load_config(str(tmp_path / "base.yaml"), str(tmp_path / "prod.yaml"), keyed=KEYED)

    assert _as_dict(cfg) == {"things": {"items": {"alpha": {"size": 9}}}}


# ---------------------------------------------------------------------------
# US2 scenario 5 — list and mapping forms cannot be mixed
# ---------------------------------------------------------------------------


def test_list_in_every_layer_merges_as_today(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {"a.yaml": _LIST_A, "b.yaml": _LIST_B, "root.yaml": _includes("a.yaml", "b.yaml")},
    )

    cfg = load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

    assert _as_dict(cfg) == {"things": {"items": [{"name": "beta"}]}}


def test_list_and_mapping_across_includes_is_an_error(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {"a.yaml": _LIST_A, "b.yaml": _MAPPING_B, "root.yaml": _includes("a.yaml", "b.yaml")},
    )

    with pytest.raises(ConfigError, match="use one form within a composition") as exc_info:
        load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

    message = str(exc_info.value)
    assert "things.items is a list in" in message
    assert "a.yaml" in message
    assert "b.yaml" in message


def test_list_and_mapping_across_explicit_layers_is_an_error(tmp_path: Path) -> None:
    _write_tree(tmp_path, {"base.yaml": _LIST_A, "prod.yaml": _MAPPING_B})

    with pytest.raises(ConfigError, match="use one form within a composition") as exc_info:
        load_config(str(tmp_path / "base.yaml"), str(tmp_path / "prod.yaml"), keyed=KEYED)

    message = str(exc_info.value)
    assert "base.yaml" in message
    assert "prod.yaml" in message


# ---------------------------------------------------------------------------
# No ``keyed=`` — today's behaviour
# ---------------------------------------------------------------------------


def test_without_keyed_a_duplicate_across_includes_is_overridden(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {
            "a.yaml": _MAPPING_A,
            "c.yaml": _MAPPING_A_DUP,
            "root.yaml": _includes("a.yaml", "c.yaml"),
        },
    )

    cfg = load_config(str(tmp_path / "root.yaml"))

    assert _as_dict(cfg) == {"things": {"items": {"alpha": {"size": 9}}}}
