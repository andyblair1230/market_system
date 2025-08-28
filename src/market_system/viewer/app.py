# src/market_system/viewer/app.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Any

# Defer/guard imports so the repo works without viewer deps installed
_import_error: Exception | None = None
try:
    from PySide6.QtWidgets import QApplication, QMainWindow, QFileDialog, QMessageBox
    from PySide6.QtGui import QAction
    import pyqtgraph as pg
except Exception as e:  # noqa: BLE001
    _import_error = e


class ViewerApp:
    def __init__(self) -> None:
        self._app: Optional["QApplication"] = None
        self._win: Optional["QMainWindow"] = None
        self._plot: Optional[Any] = None  # pg.PlotWidget (no stubs)
        self._curve: Optional[Any] = None  # pg.PlotDataItem (no stubs)
        self._current_path: Optional[Path] = None

    def start(self) -> None:
        if _import_error:
            raise RuntimeError(
                "Viewer dependencies not installed. Install with: pip install -e .[viewer]"
            ) from _import_error

        self._app = QApplication.instance() or QApplication([])
        self._win = QMainWindow()
        self._win.setWindowTitle("Market System Viewer")
        self._win.resize(1280, 800)

        # Central plot
        self._plot = pg.PlotWidget()
        self._plot.showGrid(x=True, y=True, alpha=0.3)
        self._plot.setLabel("bottom", "ts")
        self._plot.setLabel("left", "price")
        self._curve = self._plot.plot([], [], pen=None, symbol="o", symbolSize=3)
        self._win.setCentralWidget(self._plot)
        self._win.statusBar().showMessage("Ready")

        # Menu
        mbar = self._win.menuBar()
        file_menu = mbar.addMenu("&File")

        open_act = QAction("Open Parquet...", self._win)
        open_act.triggered.connect(self._on_open_parquet)
        file_menu.addAction(open_act)

        demo_act = QAction("Load Demo Data", self._win)
        demo_act.triggered.connect(self._on_load_demo)
        file_menu.addAction(demo_act)

        clear_act = QAction("Clear", self._win)
        clear_act.triggered.connect(self._clear_plot)
        file_menu.addAction(clear_act)

        exit_act = QAction("Exit", self._win)
        exit_act.triggered.connect(self._win.close)
        file_menu.addAction(exit_act)

        self._win.show()
        self._app.exec()

    # ---------- actions ----------

    def _on_open_parquet(self) -> None:
        assert self._win is not None
        win = self._win
        path, _ = QFileDialog.getOpenFileName(
            win,
            "Open Parquet",
            str(Path.cwd()),
            "Parquet Files (*.parquet);;All Files (*.*)",
        )
        if not path:
            return
        try:
            p = Path(path)
            self._load_parquet(p)
            self._current_path = p
            win.statusBar().showMessage(f"Loaded {path}")
        except Exception as e:  # noqa: BLE001
            QMessageBox.critical(win, "Error", f"Failed to load {path}\n{e}")

    def _on_load_demo(self) -> None:
        import numpy as np
        import pyarrow as pa

        n = 500
        ts = np.arange(n, dtype="int64")
        price = np.cumsum(np.random.normal(0, 0.5, n)) + 100.0
        tbl = pa.table({"ts": ts, "price": price})
        self._plot_table(tbl, "demo data")

    def _clear_plot(self) -> None:
        if self._curve is None or self._plot is None or self._win is None:
            return
        self._curve.setData([], [])
        self._plot.enableAutoRange()
        self._win.statusBar().showMessage("Cleared")

    # ---------- data loading / plotting ----------

    def _load_parquet(self, path: Path) -> None:
        import pyarrow.parquet as pq

        if path.is_dir():
            ds = pq.ParquetDataset(str(path))
            tbl = ds.read()
        else:
            tbl = pq.read_table(str(path))
        self._plot_table(tbl, path.name)

    def _plot_table(self, tbl, label: str) -> None:
        import pyarrow as pa

        assert (
            self._curve is not None and self._plot is not None and self._win is not None
        )
        curve = self._curve
        plot = self._plot
        win = self._win

        # Prefer explicit 'ts' and 'price'; else pick first two numeric columns.
        names = tbl.column_names
        if "ts" in names and "price" in names:
            ts_col = tbl.column("ts")
            price_col = tbl.column("price")
        else:
            numeric_names = [
                n
                for n, col in zip(names, tbl.columns)
                if pa.types.is_integer(col.type) or pa.types.is_floating(col.type)
            ]
            if len(numeric_names) < 2:
                raise ValueError(
                    "Expected columns 'ts' and 'price' or at least two numeric columns."
                )
            ts_col = tbl.column(numeric_names[0])
            price_col = tbl.column(numeric_names[1])

        ts = ts_col.to_numpy(zero_copy_only=False)
        price = price_col.to_numpy(zero_copy_only=False)

        # If ts is timestamp, convert to seconds as float for plotting
        if pa.types.is_timestamp(ts_col.type):
            unit = ts_col.type.unit  # "s", "ms", "us", or "ns"
            scale = {"s": 1.0, "ms": 1e-3, "us": 1e-6, "ns": 1e-9}.get(unit, 1.0)
            ts = ts.astype("float64") * scale

        curve.setData(ts, price)
        plot.enableAutoRange()
        win.setWindowTitle(f"Market System Viewer — {label}")
        win.statusBar().showMessage(f"Rows: {len(price)}")

    def stop(self) -> None:
        # placeholder for clean shutdown; no-op
        pass
