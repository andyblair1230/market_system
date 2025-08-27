from typing import Any, Optional


class ParquetWriter:
    def __init__(self) -> None:
        pass

    def write(self, table: Any, path: str, partitioning: Optional[dict] = None) -> None:
        pass


class SCIDWriter:
    def __init__(self) -> None:
        pass

    def write(self, table: Any, path: str) -> None:
        pass
