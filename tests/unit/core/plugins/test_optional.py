"""``import_optional``: the one way an island that needs an extra is loaded.

The island imports its SDK at the top; what selects it loads it here, and the
island's own ``ImportError`` becomes a ``MissingExtraError`` naming the extra.
"""

from __future__ import annotations

import pytest

from loom.core.plugins.optional import MissingExtraError, import_optional


class TestImportOptional:
    def test_returns_the_island_when_its_extra_is_installed(self) -> None:
        """A loom module whose imports resolve comes back as the module itself."""
        module = import_optional("loom.core.plugins.entrypoints", extra="none-needed")

        assert module.__name__ == "loom.core.plugins.entrypoints"

    def test_names_the_extra_and_the_missing_module_when_the_island_cannot_import(
        self,
    ) -> None:
        """The operator reads which extra to install, and what the interpreter could not find."""
        with pytest.raises(MissingExtraError) as failure:
            import_optional("loom_nonexistent_island_xyz", extra="ai-nowhere")

        error = failure.value
        assert error.module == "loom_nonexistent_island_xyz"
        assert error.extra == "ai-nowhere"
        assert error.missing == "loom_nonexistent_island_xyz"
        assert "ai-nowhere" in str(error)
        assert isinstance(error.__cause__, ImportError)

    def test_is_an_import_error_so_a_plain_except_still_catches_it(self) -> None:
        """A caller that only knows ``ImportError`` is not broken by the richer type."""
        with pytest.raises(ImportError):
            import_optional("loom_nonexistent_island_xyz", extra="ai-nowhere")
