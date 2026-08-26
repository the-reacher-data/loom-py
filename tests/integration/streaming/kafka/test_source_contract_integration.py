"""The input/output contract the partitioned source must honour unchanged.

Replacing one runtime source with another is only safe if the *contract* around
it is untouched: the same compiled plan goes in, and the same item shape comes
out. Behaviour is covered by ``test_partitioned_source_integration``; this
module pins the contract itself, against a real broker so the emitted record is
the one Kafka actually produced rather than one a fake asserted into existence.

Input contract — ``build_runtime_source(compiled, tracker, observability)``
accepts every compiled Kafka source shape (``CompiledSingleSource`` and
``CompiledMultiSource``) and reads nothing from them but ``settings`` and
``topics``.

Output contract — the source emits ``KafkaRecord[bytes]`` with every transport
field populated: ``topic``, ``key``, ``value``, ``headers``, ``partition``,
``offset`` and ``timestamp_ms``. Downstream decode, DLQ routing and the commit
tracker each depend on a different one of these, so a source that silently
dropped any single field would still look correct in a narrow test.
"""

from __future__ import annotations

import time
from collections.abc import Callable

import pytest

from loom.streaming.bytewax._commit_tracker import KafkaCommitTracker
from loom.streaming.bytewax._runtime_io import KafkaPartitionedSource, build_runtime_source
from loom.streaming.compiler._plan import CompiledMultiSource, CompiledSingleSource
from loom.streaming.kafka._record import KafkaRecord

pytestmark = [pytest.mark.integration, pytest.mark.kafka]

_DRAIN_TIMEOUT_S = 30.0
_DRAIN_IDLE_POLL_S = 0.05

SourceFactory = Callable[..., CompiledSingleSource]
MultiSourceFactory = Callable[..., CompiledMultiSource]
TopicFactory = Callable[[int], str]
FullProducer = Callable[..., None]


def _drain_one(partition: object) -> KafkaRecord[bytes]:
    """Poll one source partition until exactly one record is available."""
    deadline = time.monotonic() + _DRAIN_TIMEOUT_S
    while time.monotonic() < deadline:
        batch = partition.next_batch()  # type: ignore[attr-defined]
        if batch:
            return batch[0]
        time.sleep(_DRAIN_IDLE_POLL_S)
    raise AssertionError("no record arrived within the drain timeout")


class TestInputContract:
    """Every compiled Kafka source shape builds the same runtime source."""

    def test_single_source_builds_a_partitioned_source(
        self,
        topic_factory: TopicFactory,
        make_source: SourceFactory,
        group_id: str,
    ) -> None:
        topic = topic_factory(2)
        tracker = KafkaCommitTracker()

        built = build_runtime_source(make_source(topic, group_id), tracker)

        assert isinstance(built, KafkaPartitionedSource)
        assert built.list_parts() == [f"{topic}:0", f"{topic}:1"]

    def test_multi_source_builds_the_same_partitioned_source(
        self,
        topic_factory: TopicFactory,
        make_multi_source: MultiSourceFactory,
        group_id: str,
    ) -> None:
        """A heterogeneous source is not a different runtime path."""
        topic = topic_factory(2)
        tracker = KafkaCommitTracker()

        built = build_runtime_source(make_multi_source(topic, group_id), tracker)

        assert isinstance(built, KafkaPartitionedSource)
        assert built.list_parts() == [f"{topic}:0", f"{topic}:1"]

    def test_multi_source_consumes_identically(
        self,
        topic_factory: TopicFactory,
        make_multi_source: MultiSourceFactory,
        produce: Callable[..., None],
        group_id: str,
    ) -> None:
        """The compiled shape changes decoding downstream, never consumption."""
        topic = topic_factory(1)
        produce(topic, [(0, b"multi-0"), (0, b"multi-1")])
        tracker = KafkaCommitTracker()
        source = build_runtime_source(make_multi_source(topic, group_id), tracker)

        part = source.build_part("step", f"{topic}:0", None)
        first = _drain_one(part)
        part.close()

        assert first.value == b"multi-0"


class TestOutputContract:
    """Emitted records carry every transport field downstream code reads."""

    def test_record_exposes_the_full_transport_surface(
        self,
        topic_factory: TopicFactory,
        make_source: SourceFactory,
        produce_full: FullProducer,
        group_id: str,
    ) -> None:
        topic = topic_factory(1)
        produce_full(
            topic,
            partition=0,
            key=b"order-42",
            value=b'{"value":"contract"}',
            headers={"x-loom-trace-id": b"trace-abc", "x-loom-correlation-id": b"corr-xyz"},
        )
        tracker = KafkaCommitTracker()
        source = build_runtime_source(make_source(topic, group_id), tracker)

        part = source.build_part("step", f"{topic}:0", None)
        record = _drain_one(part)
        part.close()

        assert record.topic == topic
        assert record.key == b"order-42"
        assert record.value == b'{"value":"contract"}'
        assert record.headers["x-loom-trace-id"] == b"trace-abc"
        assert record.headers["x-loom-correlation-id"] == b"corr-xyz"
        assert record.partition == 0
        assert record.offset == 0
        assert record.timestamp_ms is not None and record.timestamp_ms > 0

    def test_snapshot_is_the_next_offset_to_read(
        self,
        topic_factory: TopicFactory,
        make_source: SourceFactory,
        produce: Callable[..., None],
        group_id: str,
    ) -> None:
        """``snapshot`` is the Bytewax recovery contract: the next offset, not the last."""
        topic = topic_factory(1)
        produce(topic, [(0, b"a"), (0, b"b"), (0, b"c")])
        tracker = KafkaCommitTracker()
        source = build_runtime_source(make_source(topic, group_id), tracker)

        part = source.build_part("step", f"{topic}:0", None)
        deadline = time.monotonic() + _DRAIN_TIMEOUT_S
        drained: list[KafkaRecord[bytes]] = []
        while len(drained) < 3 and time.monotonic() < deadline:
            drained.extend(part.next_batch())
            time.sleep(_DRAIN_IDLE_POLL_S)
        snapshot = part.snapshot()
        part.close()

        assert len(drained) == 3
        assert snapshot == 3, "snapshot must be the next offset to read, not the last read one"

    def test_idle_partition_snapshot_preserves_the_resume_position(
        self,
        topic_factory: TopicFactory,
        make_source: SourceFactory,
        group_id: str,
    ) -> None:
        """An epoch with no traffic must not overwrite a prior resume state."""
        topic = topic_factory(1)
        tracker = KafkaCommitTracker()
        source = build_runtime_source(make_source(topic, group_id), tracker)

        part = source.build_part("step", f"{topic}:0", 7)
        assert part.next_batch() == []
        snapshot = part.snapshot()
        part.close()

        assert snapshot == 7, "an idle epoch snapshotted None and would rewind on resume"
