"""Install a third-party auth strategy the way a real distribution would.

The point of ``loom.ai.remote_auth`` is that someone who is not loom can register
a strategy for the endpoints loom calls out to — MCP servers and A2A agents
alike, since the contract is ``httpx.Auth`` and knows neither protocol.
Monkey-patching the loader would prove nothing about that, so this helper
writes a genuine distribution — a module plus its ``.dist-info`` with an
``entry_points.txt`` — onto ``sys.path``. ``importlib.metadata.entry_points``
then discovers it exactly as it discovers loom's own registrations, and the
production loader resolves it with no test-only seam.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent

STRATEGY_MODULE = "third_party_remote_auth"
"""Module name of the installed strategy package."""

STRATEGY_SOURCE = dedent(
    '''
    """A deployment's own outbound authentication, living outside loom."""

    from __future__ import annotations

    import httpx


    class AgentSessionAuth(httpx.Auth):
        """Presents a bootstrap credential obtained from a session endpoint."""

        def __init__(self, *, session_url: str, bootstrap_ref: str) -> None:
            self.session_url = session_url
            self.bootstrap_ref = bootstrap_ref

        def auth_flow(self, request):  # noqa: ANN001, ANN201 - httpx generator contract
            request.headers["Authorization"] = f"Agent {self.bootstrap_ref}"
            yield request
    '''
).strip()


@contextmanager
def third_party_strategy(root: Path, *, name: str) -> Iterator[None]:
    """Register ``name`` in ``loom.ai.remote_auth`` from a distribution outside loom.

    Args:
        root: Directory the distribution is written into, added to ``sys.path``
            for the duration.
        name: Entry-point name the deployment would write in ``auth.kind``.

    Yields:
        Nothing; the strategy is resolvable inside the block and gone after it.

    Example::

        with third_party_strategy(tmp_path, name="agent-session"):
            ...
    """
    (root / f"{STRATEGY_MODULE}.py").write_text(STRATEGY_SOURCE, encoding="utf-8")
    dist_info = root / "third_party_auth-0.1.0.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: third-party-auth\nVersion: 0.1.0\n", encoding="utf-8"
    )
    (dist_info / "entry_points.txt").write_text(
        f"[loom.ai.remote_auth]\n{name} = {STRATEGY_MODULE}:AgentSessionAuth\n", encoding="utf-8"
    )
    sys.path.insert(0, str(root))
    importlib.invalidate_caches()
    try:
        yield
    finally:
        sys.path.remove(str(root))
        sys.modules.pop(STRATEGY_MODULE, None)
        importlib.invalidate_caches()
