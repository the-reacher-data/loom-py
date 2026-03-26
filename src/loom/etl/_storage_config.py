"""ETL storage configuration — OmegaConf-loadable, msgspec-typed.

Defines the ``storage:`` section of the Loom YAML config.  All sub-option
dictionaries (``storage_options``, ``writer``, ``delta_config``, ``commit``)
are passed **verbatim** to the underlying delta-rs API — Loom does not
validate or restrict their contents beyond attempting construction of the
corresponding delta-rs objects at startup (fail-fast).

Supported option keys (per dict)
---------------------------------
``storage_options``
    Object-store credentials and connection settings.
    See https://delta-io.github.io/delta-rs/api/delta_writer/ — keys differ
    per cloud (AWS, GCS, Azure).

``writer``
    Parquet writer settings forwarded to ``deltalake.WriterProperties``.
    Supported fields: ``compression``, ``compression_level``,
    ``max_row_group_size``, ``write_batch_size``, ``data_page_size_limit``,
    ``dictionary_page_size_limit``, ``data_page_row_count_limit``,
    ``statistics_truncate_length``, ``default_column_properties``,
    ``column_properties``.
    See https://delta-io.github.io/delta-rs/api/delta_writer/#deltalake.WriterProperties

``delta_config``
    Delta table properties written to the transaction log.
    Examples: ``delta.logRetentionDuration``, ``delta.enableChangeDataFeed``.
    See https://docs.delta.io/latest/table-properties.html

``commit``
    Fields forwarded to ``deltalake.CommitProperties`` (e.g. ``custom_metadata``).
    See https://delta-io.github.io/delta-rs/api/delta_writer/#deltalake.CommitProperties

Loading from YAML with OmegaConf
----------------------------------
OmegaConf resolves ``${oc.env:VAR,default}`` interpolations before the dict
reaches msgspec.  See https://omegaconf.readthedocs.io/en/latest/custom_resolvers.html ::

    from omegaconf import OmegaConf
    import msgspec

    raw      = OmegaConf.load("loom.yaml")
    resolved = OmegaConf.to_container(raw.storage, resolve=True)
    config   = msgspec.convert(resolved, StorageConfig)
    config.validate()
    locator  = config.to_locator()   # only for DeltaConfig

YAML examples
-------------
Minimal (credentials from environment)::

    storage:
      root: s3://my-lake/

With explicit options::

    storage:
      root: s3://my-lake/
      storage_options:
        AWS_REGION: ${oc.env:AWS_REGION,eu-west-1}
        AWS_ACCESS_KEY_ID: ${oc.env:AWS_KEY}
      writer:
        compression: SNAPPY
      tables:
        raw.orders:
          root: s3://raw-account/raw/
          storage_options:
            AWS_ACCESS_KEY_ID: ${oc.env:RAW_KEY}
          writer:
            compression: ZSTD
            compression_level: 3

Unity Catalog (all from Databricks runtime environment)::

    storage:
      type: unity_catalog

Unity Catalog (explicit) with one external Delta table::

    storage:
      type: unity_catalog
      workspace_url: ${oc.env:DATABRICKS_HOST}
      catalog: my_catalog
      token: ${oc.env:DATABRICKS_TOKEN}
      tables:
        external.legacy:
          type: delta
          root: s3://legacy-bucket/
          storage_options:
            AWS_REGION: us-east-1
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

import msgspec

from loom.etl._locator import MappingLocator, PrefixLocator, TableLocation, TableLocator


class StorageBackend(StrEnum):
    """Named constants for the ``type:`` field of the storage config.

    The underlying YAML discriminator values are plain strings (``"delta"``,
    ``"unity_catalog"``).  :class:`StorageBackend` provides type-safe names
    for comparing against ``config.type`` in code without hardcoding bare
    strings.

    .. note::
        ``msgspec`` requires ``Literal`` annotations to contain plain strings,
        not enum values.  The struct fields use ``Literal["delta"]`` etc.
        internally; this enum is for **comparison use only**.

    Example::

        from loom.etl import StorageBackend

        if config.type == StorageBackend.UNITY_CATALOG:
            runner = ETLRunner.from_spark(spark)
        elif config.type == StorageBackend.DELTA:
            runner = ETLRunner.from_yaml("loom.yaml")
    """

    DELTA = "delta"
    UNITY_CATALOG = "unity_catalog"


class TableOverride(msgspec.Struct, frozen=True):
    """Per-table storage override — specify only what differs from the parent config.

    Args:
        type:            Always ``"delta"`` (only Delta tables supported as overrides).
        root:            Override root URI for this table only.
        storage_options: Override cloud credentials.
                         See https://delta-io.github.io/delta-rs/api/delta_writer/
        writer:          Override Parquet writer settings.
                         See https://delta-io.github.io/delta-rs/api/delta_writer/#deltalake.WriterProperties
        delta_config:    Override Delta table properties.
                         See https://docs.delta.io/latest/table-properties.html
    """

    type: Literal["delta"] = "delta"
    root: str = ""
    storage_options: dict[str, str] = {}
    writer: dict[str, Any] = {}
    delta_config: dict[str, str | None] = {}

    def validate(self, ref: str) -> None:
        """Attempt to construct ``WriterProperties`` to surface bad keys at startup.

        Args:
            ref: Table reference string — used only in the error message.

        Raises:
            TypeError: When *writer* contains a key not accepted by delta-rs.
        """
        _validate_writer(self.writer, context=f"tables.{ref}.writer")


class DeltaConfig(msgspec.Struct, frozen=True):
    """Delta Lake storage configuration.

    The preferred programmatic entry point — construct directly in Python
    without any YAML file.  All fields have safe defaults so you only
    specify what differs from the baseline::

        from loom.etl import DeltaConfig, ETLRunner

        # Minimal — local or cloud path, credentials from environment
        config = DeltaConfig(root="s3://my-lake/")
        runner = ETLRunner.from_config(config)

        # Explicit credentials and writer options
        config = DeltaConfig(
            root="s3://my-lake/",
            storage_options={"AWS_REGION": "eu-west-1"},
            writer={"compression": "SNAPPY"},
        )

    Derive a new config from an existing one without mutation — use
    :func:`msgspec.structs.replace` to create a modified copy::

        import msgspec

        base   = DeltaConfig(root="s3://my-lake/")
        prod   = msgspec.structs.replace(base, writer={"compression": "ZSTD"})
        staging = msgspec.structs.replace(base, root="s3://staging-lake/")

    Load from YAML via :func:`~loom.etl.ETLRunner.from_yaml` or from a
    pre-resolved dict via :func:`~loom.etl.ETLRunner.from_dict`.  ``type``
    defaults to ``"delta"`` — omit it in YAML for the common case.

    Args:
        type:                 Discriminator.  Defaults to ``"delta"`` — safe to omit.
        root:                 Root URI.  Accepts local paths and cloud URIs:
                              ``s3://``, ``gs://``, ``abfss://``.
        storage_options:      Cloud credentials forwarded verbatim to delta-rs.
                              See https://delta-io.github.io/delta-rs/api/delta_writer/
        writer:               Parquet writer settings → ``WriterProperties(**writer)``.
                              See https://delta-io.github.io/delta-rs/api/delta_writer/#deltalake.WriterProperties
        delta_config:         Delta table properties (transaction log).
                              See https://docs.delta.io/latest/table-properties.html
        commit:               Commit metadata → ``CommitProperties(**commit)``.
                              See https://delta-io.github.io/delta-rs/api/delta_writer/#deltalake.CommitProperties
        tables:               Per-table overrides — only specify what differs from
                              the top-level defaults above.
        tmp_root:             Root URI for intermediate (temp) storage.  Required
                              when the pipeline uses :class:`~loom.etl.IntoTemp`.
        tmp_storage_options:  Cloud credentials for the temp store.  Defaults to
                              ``storage_options`` when empty.
    """

    type: Literal["delta"] = "delta"
    root: str = ""
    storage_options: dict[str, str] = {}
    writer: dict[str, Any] = {}
    delta_config: dict[str, str | None] = {}
    commit: dict[str, Any] = {}
    tables: dict[str, TableOverride] = {}
    tmp_root: str = ""
    tmp_storage_options: dict[str, str] = {}

    def validate(self) -> None:
        """Construct delta-rs sub-objects eagerly to surface bad keys at startup.

        Raises:
            TypeError: When any option dict contains a key not accepted by delta-rs.
        """
        _validate_writer(self.writer, context="writer")
        _validate_commit(self.commit, context="commit")
        for ref, override in self.tables.items():
            override.validate(ref)

    def to_locator(self) -> TableLocator:
        """Build a :class:`~loom.etl._locator.TableLocator` from this config.

        Returns:
            :class:`~loom.etl._locator.PrefixLocator` when no per-table overrides
            are defined; :class:`~loom.etl._locator.MappingLocator` otherwise.
        """
        default = TableLocation(
            uri=self.root,
            storage_options=self.storage_options,
            writer=self.writer,
            delta_config=self.delta_config,
            commit=self.commit,
        )
        if not self.tables:
            return PrefixLocator(
                root=self.root,
                storage_options=self.storage_options or None,
                writer=self.writer or None,
                delta_config=self.delta_config or None,
                commit=self.commit or None,
            )
        mapping = {
            ref: TableLocation(
                uri=t.root or self.root,
                storage_options=t.storage_options or self.storage_options,
                writer=t.writer or self.writer,
                delta_config=t.delta_config or self.delta_config,
                commit=self.commit,
            )
            for ref, t in self.tables.items()
        }
        return MappingLocator(mapping, default)


class UnityCatalogConfig(msgspec.Struct, frozen=True):
    """Unity Catalog storage configuration.

    Unity Catalog manages physical storage internally — no root URI or locator
    is required.  Connection details default to Databricks runtime environment
    variables (``DATABRICKS_HOST``, ``DATABRICKS_TOKEN``).

    The simplest programmatic entry point for Databricks is
    :meth:`~loom.etl.ETLRunner.from_spark`, which constructs this config
    automatically::

        from loom.etl import ETLRunner

        runner = ETLRunner.from_spark(spark)

    Construct directly when you need explicit connection details or external
    table overrides::

        from loom.etl import UnityCatalogConfig, ETLRunner

        config = UnityCatalogConfig(
            type="unity_catalog",
            workspace_url="https://my-workspace.azuredatabricks.net",
            catalog="my_catalog",
            token=secret_manager.get("databricks-token"),
        )
        runner = ETLRunner.from_config(config, spark=spark)

    Derive a modified copy without mutation::

        import msgspec

        base = UnityCatalogConfig(type="unity_catalog", catalog="prod")
        dev  = msgspec.structs.replace(base, catalog="dev")

    ``tables`` accepts external Delta table overrides for refs that live
    outside Unity Catalog (e.g. legacy tables in S3).

    Args:
        type:                 Discriminator.  Must be ``"unity_catalog"`` in YAML.
        workspace_url:        Databricks workspace URL.  Falls back to ``DATABRICKS_HOST``.
        catalog:              Unity Catalog catalog name.  Uses workspace default when empty.
        token:                Personal access token.  Falls back to ``DATABRICKS_TOKEN``.
        tables:               External Delta overrides — refs not managed by UC.
        tmp_root:             Root URI for intermediate (temp) storage.  Required
                              when the pipeline uses :class:`~loom.etl.IntoTemp`.
        tmp_storage_options:  Cloud credentials for the temp store.
    """

    type: Literal["unity_catalog"]
    workspace_url: str = ""
    catalog: str = ""
    token: str = ""
    tables: dict[str, TableOverride] = {}
    tmp_root: str = ""
    tmp_storage_options: dict[str, str] = {}

    def validate(self) -> None:
        """Validate external table overrides.

        Raises:
            TypeError: When any override writer dict contains an invalid delta-rs key.
        """
        for ref, override in self.tables.items():
            override.validate(ref)


StorageConfig = DeltaConfig | UnityCatalogConfig
"""Union of all supported storage configuration types.

Do not pass this union directly to ``msgspec.convert`` — use
:func:`convert_storage_config` instead, which dispatches on the ``type``
field and handles the ``"delta"`` default correctly.
"""


def convert_storage_config(raw: dict[str, Any]) -> StorageConfig:
    """Convert a raw dict into the correct :data:`StorageConfig` variant.

    Dispatches on the ``"type"`` key.  Defaults to :class:`DeltaConfig` when
    ``"type"`` is absent or ``"delta"``.

    ``msgspec.convert`` cannot decode a union of two Struct types without
    explicit tag metadata conflicting with the existing ``type`` field, so
    this function performs the dispatch manually before converting each
    concrete type.

    Args:
        raw: Plain Python dict (already resolved — no ``${...}`` interpolations).

    Returns:
        :class:`DeltaConfig` or :class:`UnityCatalogConfig`.

    Raises:
        msgspec.ValidationError: When the dict does not match the resolved type.
        ValueError:              When ``type`` contains an unrecognised backend name.

    Example::

        from loom.etl._storage_config import convert_storage_config

        config = convert_storage_config({"root": "s3://my-lake/"})
        config = convert_storage_config({"type": "unity_catalog"})
    """
    backend = raw.get("type", StorageBackend.DELTA)
    if backend == StorageBackend.UNITY_CATALOG:
        return msgspec.convert(raw, UnityCatalogConfig)
    if backend == StorageBackend.DELTA:
        return msgspec.convert(raw, DeltaConfig)
    raise ValueError(
        f"Unknown storage backend type {backend!r}. "
        f"Expected one of: {[b.value for b in StorageBackend]}"
    )


# ---------------------------------------------------------------------------
# Internal validators — construct delta-rs objects to fail fast at startup
# ---------------------------------------------------------------------------


def _validate_writer(writer: dict[str, Any], *, context: str) -> None:
    if not writer:
        return
    try:
        from deltalake.table import WriterProperties

        WriterProperties(**writer)
    except TypeError as exc:
        raise TypeError(f"Invalid key in storage.{context}: {exc}") from exc


def _validate_commit(commit: dict[str, Any], *, context: str) -> None:
    if not commit:
        return
    try:
        from deltalake import CommitProperties

        CommitProperties(**commit)
    except TypeError as exc:
        raise TypeError(f"Invalid key in storage.{context}: {exc}") from exc
