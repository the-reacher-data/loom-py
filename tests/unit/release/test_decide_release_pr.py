from __future__ import annotations

import pytest

from scripts.release import decide_release_pr as decision

TIP = "a" * 40
BEHIND = "b" * 40


def _state(**overrides: object) -> decision.ReleaseBranchState:
    values: dict[str, object] = {
        "open_count": 0,
        "merged_count": 0,
        "branch_exists": False,
        "merge_base_sha": None,
        "base_branch_sha": None,
    }
    values.update(overrides)
    return decision.ReleaseBranchState(**values)  # type: ignore[arg-type]


def test_an_open_pr_on_the_current_tip_is_reused() -> None:
    state = _state(open_count=1, branch_exists=True, merge_base_sha=TIP, base_branch_sha=TIP)

    assert decision.decide(state) is decision.Decision.REUSE


def test_an_open_pr_whose_base_is_behind_the_tip_is_recycled() -> None:
    state = _state(open_count=1, branch_exists=True, merge_base_sha=BEHIND, base_branch_sha=TIP)

    assert decision.decide(state) is decision.Decision.RECYCLE


def test_a_closed_unmerged_pr_without_a_branch_lets_the_bump_be_rebuilt() -> None:
    assert decision.decide(_state()) is decision.Decision.CREATE


def test_a_merged_pr_is_honoured_instead_of_building_a_second_bump() -> None:
    state = _state(merged_count=1, branch_exists=True)

    assert decision.decide(state) is decision.Decision.REUSE


def test_a_version_with_no_pull_request_at_all_is_created() -> None:
    assert decision.decide(_state(branch_exists=False)) is decision.Decision.CREATE


def test_a_surviving_branch_without_an_open_or_merged_pr_needs_a_human() -> None:
    with pytest.raises(decision.ReleasePullRequestError, match="without an open or merged"):
        decision.decide(_state(branch_exists=True))


@pytest.mark.parametrize(
    "state",
    [
        pytest.param(_state(open_count=2, merge_base_sha=TIP, base_branch_sha=TIP), id="two-open"),
        pytest.param(_state(merged_count=2), id="two-merged"),
    ],
)
def test_more_than_one_live_release_pr_is_refused(state: decision.ReleaseBranchState) -> None:
    with pytest.raises(decision.ReleasePullRequestError, match="multiple open or merged"):
        decision.decide(state)


def test_an_open_pr_with_an_unresolved_base_is_refused() -> None:
    with pytest.raises(decision.ReleasePullRequestError, match="base and tip"):
        decision.decide(_state(open_count=1, base_branch_sha=TIP))
