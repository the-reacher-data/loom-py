"""TableLocator — protocol and built-in implementations for Delta URI resolution.

Decouples logical table references (e.g. ``"raw.orders"``) from physical
storage URIs and cloud credentials.  Backends receive a locator at construction
time instead of a raw root string, enabling per-table storage routing without
coupling the reader or writer to any naming convention.

Built-in implementations
------------------------
* :class:`PrefixLocator`  — all tables under one root URI.
* :class:`MappingLocator` — explicit ``ref → location`` mapping with optional default.

For Unity Catalog or other managed catalogs, implement the protocol directly
in the backend — no locator is needed when the catalog resolves URIs itself.

Cloud URI formats accepted by delta-rs
---------------------------------------
* Local:  ``/data/delta/``
* AWS S3: ``s3://bucket/prefix/``
* GCS:    ``gs://bucket/prefix/``
* Azure:  ``abfss://container@account.dfs.core.windows.net/prefix/``

See https://delta-io.github.io/delta-rs/api/delta_writer/ for the full list of
valid ``storage_options`` keys per cloud provider.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from loom.etl.schema._table import TableRef


@dataclass(frozen=True)
class TableLocation:
    """Physical storage address and write-time configuration for one Delta table.

    All dict fields are passed **verbatim** to the corresponding delta-rs
    parameter — Loom does not validate or restrict their contents.

    Args:
        uri:             Full URI accepted by delta-rs.
        storage_options: Cloud credentials / connection settings.
                         See https://delta-io.github.io/delta-rs/api/delta_writer/
        writer:          Parquet writer settings → ``WriterProperties(**writer)``.
                         See https://delta-io.github.io/delta-rs/api/delta_writer/#deltalake.WriterProperties
        delta_config:    Delta table properties written to the transaction log.
                         See https://docs.delta.io/latest/table-properties.html
        commit:          Commit metadata → ``CommitProperties(**commit)``.
                         See https://delta-io.github.io/delta-rs/api/delta_writer/#deltalake.CommitProperties
    """

    uri: str
    storage_options: dict[str, str] = field(default_factory=dict)
    writer: dict[str, Any] = field(default_factory=dict)
    delta_config: dict[str, str | None] = field(default_factory=dict)
    commit: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class TableLocator(Protocol):
    """Protocol for resolving a logical ``TableRef`` to a physical ``TableLocation``.

    Implement this to support custom storage topologies: per-env routing,
    secret-manager-backed credentials, or Unity Catalog external tables.

    Example::

        class MyLocator:
            def locate(self, ref: TableRef) -> TableLocation:
                return TableLocation(uri=f"s3://my-bucket/{ref.ref.replace('.', '/')}/")
    """

    def locate(self, ref: TableRef) -> TableLocation:
        """Resolve *ref* to its physical storage location.

        Args:
            ref: Logical table reference (e.g. ``TableRef("raw.orders")``).

        Returns:
            :class:`TableLocation` with full URI and write-time configuration.
        """
        ...


class PrefixLocator:
    """Resolve all table refs under one root URI.

    Dots in the ref are converted to ``/``, so ``"raw.orders"`` under
    ``"s3://my-lake/"`` resolves to ``"s3://my-lake/raw/orders"``.

    Works equally for flat refs (``"orders"`` → ``"s3://my-lake/orders"``)
    and layered refs (``"raw.orders"`` → ``"s3://my-lake/raw/orders"``).

    All keyword arguments are forwarded verbatim to every :class:`TableLocation`
    produced by this locator.

    Args:
        root:            Root URI — local path or cloud URI.  Accepts
                         ``str`` or any :class:`os.PathLike` (e.g. a
                         ``pathlib.Path``); converted to ``str`` internally.
        storage_options: Cloud credentials.
                         See https://delta-io.github.io/delta-rs/api/delta_writer/
        writer:          Parquet writer settings → ``WriterProperties(**writer)``.
                         See https://delta-io.github.io/delta-rs/api/delta_writer/#deltalake.WriterProperties
        delta_config:    Delta table properties.
                         See https://docs.delta.io/latest/table-properties.html
        commit:          Commit metadata → ``CommitProperties(**commit)``.
                         See https://delta-io.github.io/delta-rs/api/delta_writer/#deltalake.CommitProperties

    Example::

        # Minimal — credentials from environment variables
        locator = PrefixLocator(root="s3://my-lake/")

        # From a pathlib.Path (converted to str internally)
        locator = PrefixLocator(root=Path("data/delta"))

        # With explicit credentials and compression
        locator = PrefixLocator(
            root="s3://my-lake/",
            storage_options={"AWS_REGION": "eu-west-1"},
            writer={"compression": "SNAPPY"},
        )
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        storage_options: dict[str, str] | None = None,
        writer: dict[str, Any] | None = None,
        delta_config: dict[str, str | None] | None = None,
        commit: dict[str, Any] | None = None,
    ) -> None:
        self._root = str(root).rstrip("/")
        self._location_defaults = TableLocation(
            uri="",  # filled per-call in locate()
            storage_options=storage_options or {},
            writer=writer or {},
            delta_config=delta_config or {},
            commit=commit or {},
        )

    def locate(self, ref: TableRef) -> TableLocation:
        """Resolve *ref* by appending its slash-separated path to the root.

        Args:
            ref: Logical table reference.

        Returns:
            :class:`TableLocation` with the full URI and shared configuration.
        """
        uri = f"{self._root}/{'/'.join(ref.ref.split('.'))}"
        return TableLocation(
            uri=uri,
            storage_options=self._location_defaults.storage_options,
            writer=self._location_defaults.writer,
            delta_config=self._location_defaults.delta_config,
            commit=self._location_defaults.commit,
        )


class MappingLocator:
    """Resolve table refs via an explicit mapping, with an optional fallback.

    Useful when tables span multiple cloud accounts, regions, or providers.
    Refs absent from the mapping fall back to *default* (if provided), with
    the ref path appended via the same dot-to-slash conversion as
    :class:`PrefixLocator`.

    Args:
        mapping: ``ref_string → TableLocation`` map.
        default: Fallback :class:`TableLocation` for unmapped refs.

    Raises:
        KeyError: On :meth:`locate` when the ref is not in *mapping* and no
                  *default* is set.

    Example::

        locator = MappingLocator(
            mapping={
                "raw.orders": TableLocation(
                    uri="s3://raw-account/orders/",
                    storage_options={"AWS_ACCESS_KEY_ID": os.environ["RAW_KEY"]},
                    writer={"compression": "SNAPPY"},
                ),
                "curated.payments": TableLocation(
                    uri="gs://curated-project/payments/",
                    storage_options={"GOOGLE_SERVICE_ACCOUNT_KEY": os.environ["GCP_SA"]},
                ),
            },
            default=TableLocation(uri="s3://default-lake/"),
        )
    """

    def __init__(
        self,
        mapping: dict[str, TableLocation],
        default: TableLocation | None = None,
    ) -> None:
        self._mapping = mapping
        self._default = default

    def locate(self, ref: TableRef) -> TableLocation:
        """Resolve *ref* from the mapping or fall back to the default.

        Args:
            ref: Logical table reference.

        Returns:
            Matching :class:`TableLocation`.

        Raises:
            KeyError: When *ref* is absent from the mapping and no default is set.
        """
        loc = self._mapping.get(ref.ref)
        if loc is not None:
            return loc
        if self._default is None:
            raise KeyError(f"No storage location configured for table {ref.ref!r}")
        suffix = "/".join(ref.ref.split("."))
        return TableLocation(
            uri=f"{self._default.uri.rstrip('/')}/{suffix}",
            storage_options=self._default.storage_options,
            writer=self._default.writer,
            delta_config=self._default.delta_config,
            commit=self._default.commit,
        )


def _as_locator(locator: str | os.PathLike[str] | TableLocator) -> TableLocator:
    """Coerce a root URI / path to a :class:`PrefixLocator`, or return *locator* as-is.

    Backends call this in their constructors so callers can pass a plain string
    or :class:`pathlib.Path` for the common single-root case without constructing
    a :class:`PrefixLocator` explicitly.
    """
    if isinstance(locator, TableLocator):
        return locator
    return PrefixLocator(locator)


def _as_location(location: str | os.PathLike[str] | TableLocation) -> TableLocation:
    """Coerce a URI string / path to a :class:`TableLocation`, or return *location* as-is.

    Sink constructors call this so callers can pass a plain URI string for the
    common no-credentials case without constructing a :class:`TableLocation` explicitly.
    """
    if isinstance(location, TableLocation):
        return location
    return TableLocation(uri=str(location))
