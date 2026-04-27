from __future__ import annotations

import loom.streaming as streaming


def test_streaming_public_api_exports_authoring_contracts() -> None:
    expected = {
        "AsyncContextDependency",
        "BatchExpandStep",
        "BatchStep",
        "Broadcast",
        "BroadcastRoute",
        "CollectBatch",
        "CompilationError",
        "ContextFactory",
        "Drain",
        "ErrorEnvelope",
        "ErrorKind",
        "ExpandStep",
        "ForEach",
        "FromTopic",
        "Fork",
        "ForkRoute",
        "IntoTopic",
        "Message",
        "MessageMeta",
        "PartitionGuarantee",
        "PartitionPolicy",
        "PartitionStrategy",
        "Process",
        "ProcessNode",
        "Predicate",
        "RecordStep",
        "ResourceFactory",
        "ResourceScope",
        "Route",
        "Router",
        "Selector",
        "Step",
        "StepContext",
        "StreamFlow",
        "StreamShape",
        "WindowStrategy",
        "SyncContextDependency",
        "With",
        "WithAsync",
        "compile_flow",
        "msg",
        "payload",
    }

    assert set(streaming.__all__) == expected
    for name in expected:
        assert getattr(streaming, name) is not None
