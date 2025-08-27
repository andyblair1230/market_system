from typing import Any, Iterable, Optional


class StreamPublisher:
    def __init__(self) -> None:
        pass

    def publish(self, records: Iterable[Any]) -> None:
        pass


class StreamSubscriber:
    def __init__(self) -> None:
        pass

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def next(self, timeout_ms: Optional[int] = None) -> Any:
        pass
