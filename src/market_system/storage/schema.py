from typing import Any, Dict


class TradeSchema:
    def to_dict(self) -> Dict[str, Any]:
        raise NotImplementedError


class DepthSchema:
    def to_dict(self) -> Dict[str, Any]:
        raise NotImplementedError


class AlignedSchema:
    def to_dict(self) -> Dict[str, Any]:
        raise NotImplementedError
