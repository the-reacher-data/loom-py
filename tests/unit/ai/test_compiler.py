"""Multi-error compilation tests for ``AgentCompiler`` (US1, T048).

Pinned contract decisions, until the implementation says otherwise:

* ``issue.component`` is the artifact path handed via ``source_path`` — the
  ``AgentCompilationIssue`` docstring documents exactly that shape.
* ``AgentPlan.output`` is a ``CompiledOutput`` exposing a **built**
  ``msgspec.json.Decoder`` under ``output.decoder`` (data-model.md Tier 3).
* A compiled capability exposes its ``kind`` and carries the resolved handle
  (the ``SqlConnectionConfig``, the registered use-case types) among its
  field values — strings die at compile.
"""

from __future__ import annotations

from pathlib import Path

import msgspec
import pytest

from loom.ai.compiler import AgentCompilationError, AgentCompiler, AgentPlan
from loom.ai.config import AiConfig
from loom.ai.declarative import (
    AgentSpecV1,
    JsonSchemaOutput,
    PolicySpec,
    SqlCapability,
    TypeRefOutput,
    UsecaseCapability,
    load_specs,
)
from loom.ai.errors import AgentErrorCode
from loom.core.sql.config import SqlConfig
from loom.core.use_case.registry import UseCaseRegistry

CORPUS_DIR = Path(__file__).parent / "fixtures" / "corpus_v1"

ALL_KINDS: frozenset[str] = frozenset({"usecase", "sql", "mcp", "skills", "python", "a2a"})

BROKEN_SOURCE = "agents/broken.agent.yaml"

ANSWER_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["answer"],
    "properties": {"answer": {"type": "string"}},
}


def _spec(**overrides: object) -> AgentSpecV1:
    base: dict[str, object] = {
        "spec_version": 1,
        "name": "subject-agent",
        "description": "Answers questions for the compiler tests.",
        "instructions": "Answer using only the prompt. Say so when unsure.",
        "output": JsonSchemaOutput(schema=ANSWER_SCHEMA),
    }
    base.update(overrides)
    return AgentSpecV1(**base)  # type: ignore[arg-type]


def _triple_fault_spec() -> AgentSpecV1:
    """One artifact, three independent faults in three different phases."""
    return _spec(
        model_role="unbound-role",
        policies=PolicySpec(retries=99),
        capabilities=(UsecaseCapability(keys=("nowhere.unknown_key",)),),
    )


@pytest.fixture
def compiler(
    compiler_env_config: AiConfig,
    compiler_env_registry: UseCaseRegistry,
    compiler_env_sql: SqlConfig,
    fake_myapp_path: object,
) -> AgentCompiler:
    return AgentCompiler(
        config=compiler_env_config,
        registry=compiler_env_registry,
        supported_kinds=ALL_KINDS,
        sql=compiler_env_sql,
    )


@pytest.fixture
def triple_fault_error(compiler: AgentCompiler) -> AgentCompilationError:
    with pytest.raises(AgentCompilationError) as excinfo:
        compiler.compile(_triple_fault_spec(), source_path=BROKEN_SOURCE)
    return excinfo.value


class TestMultiErrorAccumulation:
    """A spec with three faults reports all three in one raise (FR-011)."""

    def test_compile_raises_exactly_three_issues_when_spec_has_three_faults(
        self, triple_fault_error: AgentCompilationError
    ) -> None:
        assert len(triple_fault_error.issues) == 3

    def test_compile_reports_three_distinct_codes_when_spec_has_three_faults(
        self, triple_fault_error: AgentCompilationError
    ) -> None:
        assert {issue.code for issue in triple_fault_error.issues} == {
            AgentErrorCode.MODEL_ROLE_UNBOUND,
            AgentErrorCode.POLICY_OUT_OF_RANGE,
            AgentErrorCode.USECASE_KEY_UNKNOWN,
        }

    def test_compile_reports_the_faulted_field_on_each_issue_when_spec_has_three_faults(
        self, triple_fault_error: AgentCompilationError
    ) -> None:
        assert {(issue.code, issue.field) for issue in triple_fault_error.issues} == {
            (AgentErrorCode.MODEL_ROLE_UNBOUND, "model_role"),
            (AgentErrorCode.POLICY_OUT_OF_RANGE, "policies.retries"),
            (AgentErrorCode.USECASE_KEY_UNKNOWN, "capabilities.keys"),
        }

    def test_compile_points_every_issue_at_the_artifact_when_source_path_is_given(
        self, triple_fault_error: AgentCompilationError
    ) -> None:
        assert {issue.component for issue in triple_fault_error.issues} == {BROKEN_SOURCE}


class TestCompileAll:
    """``compile_all`` accumulates across specs before raising once."""

    def test_compile_all_accumulates_issues_of_both_specs_when_two_specs_are_broken(
        self, compiler: AgentCompiler
    ) -> None:
        first = _spec(name="first-agent", model_role="unbound-role")
        second = _spec(
            name="second-agent",
            capabilities=(UsecaseCapability(keys=("nowhere.unknown_key",)),),
        )
        with pytest.raises(AgentCompilationError) as excinfo:
            compiler.compile_all([first, second])
        assert {issue.code for issue in excinfo.value.issues} == {
            AgentErrorCode.MODEL_ROLE_UNBOUND,
            AgentErrorCode.USECASE_KEY_UNKNOWN,
        }

    def test_compile_all_reports_duplicate_name_when_two_specs_share_a_name(
        self, compiler: AgentCompiler
    ) -> None:
        with pytest.raises(AgentCompilationError) as excinfo:
            compiler.compile_all([_spec(name="dup-agent"), _spec(name="dup-agent")])
        assert AgentErrorCode.AGENT_NAME_DUPLICATE in {issue.code for issue in excinfo.value.issues}

    def test_compile_accepts_each_duplicate_spec_when_compiled_alone(
        self, compiler: AgentCompiler
    ) -> None:
        """Duplication is an application-level fault, only visible in compile_all."""
        plan = compiler.compile(_spec(name="dup-agent"))
        assert plan.name == "dup-agent"


class TestCorpus:
    """The published valid corpus compiles clean against a satisfying config."""

    def test_compile_all_returns_one_plan_per_artifact_when_corpus_is_valid(
        self, compiler: AgentCompiler
    ) -> None:
        decoded = load_specs(["*.agent.yaml"], root=CORPUS_DIR)
        assert len(decoded) == 9
        plans = compiler.compile_all([item.spec for item in decoded])
        assert len(plans) == 9


def _flat_field_values(struct: object) -> list[object]:
    values: list[object] = []
    for name in struct.__struct_fields__:  # type: ignore[attr-defined]
        value = getattr(struct, name)
        if isinstance(value, tuple):
            values.extend(value)
        else:
            values.append(value)
    return values


def _capability_of_kind(plan: AgentPlan, kind: str) -> object:
    for capability in plan.capabilities:
        if getattr(capability, "kind", None) == kind:
            return capability
    pytest.fail(f"plan carries no compiled capability of kind {kind!r}")


class TestPlanShape:
    """The plan is immutable and carries resolved handles, not strings."""

    def test_plan_rejects_mutation_when_a_field_is_assigned(self, compiler: AgentCompiler) -> None:
        plan = compiler.compile(_spec())
        with pytest.raises(AttributeError):
            plan.name = "other"  # type: ignore[misc]

    def test_sql_capability_carries_connection_config_when_plan_is_compiled(
        self, compiler: AgentCompiler, compiler_env_sql: SqlConfig
    ) -> None:
        spec = _spec(
            capabilities=(
                SqlCapability(
                    connection="reporting_readonly",
                    max_rows=500,
                    max_result_bytes=1_048_576,
                ),
            )
        )
        capability = _capability_of_kind(compiler.compile(spec), "sql")
        expected = compiler_env_sql.connections["reporting_readonly"]
        assert any(value == expected for value in _flat_field_values(capability))

    def test_usecase_capability_carries_registered_types_when_plan_is_compiled(
        self, compiler: AgentCompiler, compiler_env_registry: UseCaseRegistry
    ) -> None:
        spec = _spec(capabilities=(UsecaseCapability(keys=("orders.get_order_status",)),))
        capability = _capability_of_kind(compiler.compile(spec), "usecase")
        expected = compiler_env_registry.resolve("orders.get_order_status")
        assert any(value is expected for value in _flat_field_values(capability))


class TestCompiledOutput:
    """``json_schema`` outputs compile to a built, strict msgspec decoder."""

    def test_plan_exposes_built_msgspec_decoder_when_output_is_json_schema(
        self, compiler: AgentCompiler
    ) -> None:
        plan = compiler.compile(_spec())
        assert isinstance(plan.output.decoder, msgspec.json.Decoder)

    def test_decoder_decodes_valid_bytes_when_output_is_json_schema(
        self, compiler: AgentCompiler
    ) -> None:
        plan = compiler.compile(_spec())
        decoded = plan.output.decoder.decode(b'{"answer": "ok"}')
        assert msgspec.to_builtins(decoded) == {"answer": "ok"}

    def test_decoder_rejects_unknown_fields_when_output_is_json_schema(
        self, compiler: AgentCompiler
    ) -> None:
        plan = compiler.compile(_spec())
        with pytest.raises(msgspec.ValidationError):
            plan.output.decoder.decode(b'{"answer": "ok", "extra": 1}')


class TestTypeRefOutput:
    """``type_ref`` accepts ``msgspec.Struct`` only (T053)."""

    def test_compile_returns_plan_when_type_ref_resolves_to_msgspec_struct(
        self, compiler: AgentCompiler
    ) -> None:
        spec = _spec(output=TypeRefOutput(ref="myapp.domain.invoices:InvoiceSummary"))
        plan = compiler.compile(spec)
        assert isinstance(plan, AgentPlan)

    @pytest.mark.parametrize(
        "ref",
        [
            "myapp.domain.unsupported:PlainModel",
            "myapp.domain.unsupported:NOT_A_TYPE",
        ],
    )
    def test_compile_reports_unsupported_when_type_ref_resolves_to_non_struct(
        self, compiler: AgentCompiler, ref: str
    ) -> None:
        with pytest.raises(AgentCompilationError) as excinfo:
            compiler.compile(_spec(output=TypeRefOutput(ref=ref)))
        assert [issue.code for issue in excinfo.value.issues] == [
            AgentErrorCode.OUTPUT_TYPE_REF_UNSUPPORTED
        ]

    def test_compile_reports_unresolvable_when_type_ref_does_not_import(
        self, compiler: AgentCompiler
    ) -> None:
        spec = _spec(output=TypeRefOutput(ref="no_such_pkg_zz.domain:Missing"))
        with pytest.raises(AgentCompilationError) as excinfo:
            compiler.compile(spec)
        assert [issue.code for issue in excinfo.value.issues] == [
            AgentErrorCode.OUTPUT_TYPE_REF_UNRESOLVABLE
        ]
