from __future__ import annotations

from typing import Any, Dict


class TradeSchema:
    # canonical column names
    COLUMNS = ["ts", "price", "qty", "side", "trade_id", "seq"]

    def to_dict(self) -> Dict[str, Any]:
        # placeholder dtype hints; refine later (Arrow/Parquet dtypes)
        return {
            "ts": "timestamp[ns]",
            "price": "float64",
            "qty": "int32",
            "side": "int8",  # 1=buy, -1=sell (tbd)
            "trade_id": "int64",
            "seq": "int64",
        }


class DepthSchema:
    COLUMNS = [
        "ts",
        "side",  # 1=bid, -1=ask
        "level",  # 0..N-1
        "price",
        "qty",
        "event_type",  # add/update/delete (tbd)
    ]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts": "timestamp[ns]",
            "side": "int8",
            "level": "int16",
            "price": "float64",
            "qty": "int32",
            "event_type": "int8",
        }


class AlignedSchema:
    COLUMNS = [
        "ts",
        "trade_idx",
        "depth_idx",
        "match_type",  # exact/nearest/none (tbd)
        "qty",
        "side",
        "delta_price",  # optional alignment features
        "delta_ts_ns",
    ]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts": "timestamp[ns]",
            "trade_idx": "int64",
            "depth_idx": "int64",
            "match_type": "int8",
            "qty": "int32",
            "side": "int8",
            "delta_price": "float64",
            "delta_ts_ns": "int64",
        }
