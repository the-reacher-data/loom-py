"""Tests for Delta sink configuration parsing."""

from __future__ import annotations

from loom.streaming.nodes._table import DeltaSinkConfig


def test_delta_sink_config_parses_staging_and_writer_properties() -> None:
    config = DeltaSinkConfig.from_config(
        {
            "uri": "s3://lake/cdc_events",
            "table": "cdc_events",
            "partition_by": ["year", "month"],
            "target_file_size": 134_217_728,
            "spool_max_bytes": 1_024,
            "part_max_records": 128,
            "staging": {
                "compression": "zstd",
            },
            "writer_properties": {
                "compression": "SNAPPY",
                "max_row_group_size": 256,
            },
        },
        default_table="cdc_events",
    )

    assert config.uri == "s3://lake/cdc_events"
    assert config.table == "cdc_events"
    assert config.partition_by == ("year", "month")
    assert config.target_file_size == 134_217_728
    assert config.spool_max_bytes == 1_024
    assert config.part_max_records == 128
    assert config.staging_compression == "zstd"
    assert config.writer_properties.compression == "SNAPPY"
    assert config.writer_properties.max_row_group_size == 256
