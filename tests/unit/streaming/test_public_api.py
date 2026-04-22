from __future__ import annotations

import loom.streaming as streaming


def test_streaming_public_api_exports_authoring_contracts() -> None:
    expected = {
        "BatchTask",
        "CollectBatch",
        "Drain",
        "ErrorEnvelope",
        "ErrorKind",
        "ForEach",
        "FromTopic",
        "IntoTopic",
        "Message",
        "MessageMeta",
        "PartitionGuarantee",
        "PartitionPolicy",
        "PartitionStrategy",
        "Process",
        "Predicate",
        "ResourceFactory",
        "Route",
        "Router",
        "Selector",
        "StreamShape",
        "StreamFlow",
        "Task",
        "TaskContext",
        "msg",
    }

    assert set(streaming.__all__) == expected
    for name in expected:
        assert getattr(streaming, name) is not None
