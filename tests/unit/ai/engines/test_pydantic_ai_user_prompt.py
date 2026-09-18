"""How a :data:`~loom.ai.abc.Prompt` reaches pydantic-ai: text unchanged, an
attachment becomes ``BinaryContent``.
"""

from __future__ import annotations

from pydantic_ai import BinaryContent

from loom.ai.abc import Attachment
from loom.ai.engines.pydantic_ai._engine import _user_prompt


class TestUserPrompt:
    def test_a_text_prompt_passes_through_unchanged(self) -> None:
        assert _user_prompt("read this page") == "read this page"

    def test_attachments_become_binary_content_in_order(self) -> None:
        attachment = Attachment(media_type="image/png", data=b"\x89PNG")

        parts = _user_prompt(["describe this page", attachment])

        assert parts == [
            "describe this page",
            BinaryContent(data=b"\x89PNG", media_type="image/png"),
        ]
