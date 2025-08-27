from __future__ import annotations

from typing import Optional

# Defer/guard imports so the repo works without viewer deps installed
_import_error: Exception | None = None
try:
    from PySide6.QtWidgets import QApplication, QMainWindow
except Exception as e:  # noqa: BLE001
    _import_error = e


class ViewerApp:
    def __init__(self) -> None:
        self._app: Optional["QApplication"] = None
        self._win: Optional["QMainWindow"] = None

    def start(self) -> None:
        if _import_error:
            raise RuntimeError(
                "Viewer dependencies not installed. Install with: pip install -e .[viewer]"
            ) from _import_error

        # Minimal window; plot canvas comes later
        self._app = QApplication.instance() or QApplication([])
        self._win = QMainWindow()
        self._win.setWindowTitle("Market System Viewer")
        self._win.resize(1280, 800)
        self._win.show()
        self._app.exec()

    def stop(self) -> None:
        # placeholder for clean shutdown; no-op in stub
        pass
