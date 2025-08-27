from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional


def _need_pyarrow() -> tuple[Any, Any]:
    """
    Import pyarrow lazily and raise an actionable error if it's missing.
    Returns (pa, pq).
    """
    try:
        import pyarrow as pa  # type: ignore[import-not-found]
        import pyarrow.parquet as pq  # type: ignore[import-not-found]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            "pyarrow is required for ParquetWriter. Install with: pip install -e .[storage]"
        ) from e
    return pa, pq


def _to_arrow_table(obj: Any) -> Any:
    """
    Best-effort conversion to a pyarrow.Table.
    Supports: pyarrow.Table, pandas.DataFrame, list-of-dicts.
    """
    pa, _ = _need_pyarrow()
    # Already a table
    if isinstance(obj, pa.Table):
        return obj

    # Pandas DataFrame → Arrow
    try:
        import pandas as pd  # type: ignore[import-not-found]

        if isinstance(obj, pd.DataFrame):
            return pa.Table.from_pandas(obj, preserve_index=False)
    except Exception:
        # pandas is optional; ignore if unavailable
        pass

    # List[dict] → Arrow
    if isinstance(obj, list) and (len(obj) == 0 or isinstance(obj[0], dict)):
        return pa.Table.from_pylist(obj)

    raise TypeError(
        "Unsupported input type for ParquetWriter.write(). "
        "Provide a pyarrow.Table, a pandas.DataFrame, or a list of dicts."
    )


class ParquetWriter:
    """
    Thin wrapper over pyarrow.parquet with safe defaults.
    """

    def __init__(
        self,
        *,
        compression: str = "zstd",
        coerce_timestamps: str = "us",
        use_dictionary: Optional[bool] = None,
    ) -> None:
        self.compression = compression
        self.coerce_timestamps = coerce_timestamps
        self.use_dictionary = use_dictionary

    def write(
        self,
        table: Any,
        path: str | Path,
        partitioning: Optional[dict] = None,
    ) -> None:
        pa, pq = _need_pyarrow()
        tbl = _to_arrow_table(table)

        out = Path(path)
        if partitioning:
            # Expect partitioning like: {"by": ["date", "symbol"]}
            by: Iterable[str] = partitioning.get("by", [])  # type: ignore[assignment]
            if not by:
                raise ValueError('partitioning must include a non-empty "by" list')
            out.mkdir(parents=True, exist_ok=True)
            pq.write_to_dataset(
                tbl,
                root_path=str(out),
                partition_cols=list(by),
                compression=self.compression,
                coerce_timestamps=self.coerce_timestamps,
                use_dictionary=self.use_dictionary,
            )
        else:
            out.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                tbl,
                str(out),
                compression=self.compression,
                coerce_timestamps=self.coerce_timestamps,
                use_dictionary=self.use_dictionary,
            )


class SCIDWriter:
    """
    Placeholder. We are not writing SCID in this project.
    Kept only to preserve the public surface for now.
    """

    def __init__(self) -> None:  # pragma: no cover
        pass

    def write(self, table: Any, path: str) -> None:  # pragma: no cover
        raise NotImplementedError("SCIDWriter is intentionally not implemented.")
