# src/market_system/viewer/app.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Any

# Defer/guard imports so the repo works without viewer deps installed
_import_error: Exception | None = None
try:
    from PySide6.QtWidgets import (
        QApplication,
        QMainWindow,
        QFileDialog,
        QMessageBox,
    )
    from PySide6.QtGui import QAction
    from PySide6.QtCore import QTimer
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

        # Replay state
        self._timer: Optional[QTimer] = None
        self._r_ts = None  # numpy.ndarray
        self._r_px = None  # numpy.ndarray
        self._r_idx: int = 0
        self._r_step: int = 200  # points per tick
        self._r_running: bool = False

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

        # Menus
        mbar = self._win.menuBar()
        file_menu = mbar.addMenu("&File")
        replay_menu = mbar.addMenu("&Replay")

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

        # Replay actions
        r_parquet_act = QAction("Replay Parquet...", self._win)
        r_parquet_act.triggered.connect(self._on_replay_parquet)
        replay_menu.addAction(r_parquet_act)

        r_demo_act = QAction("Replay Demo", self._win)
        r_demo_act.triggered.connect(self._on_replay_demo)
        replay_menu.addAction(r_demo_act)

        r_play_act = QAction("Play / Pause", self._win)
        r_play_act.triggered.connect(self._on_play_pause)
        replay_menu.addAction(r_play_act)

        r_stop_act = QAction("Stop", self._win)
        r_stop_act.triggered.connect(self._on_stop)
        replay_menu.addAction(r_stop_act)

        self._timer = QTimer(self._win)
        self._timer.setInterval(30)  # ~33 FPS-ish; we step multiple points per tick
        self._timer.timeout.connect(self._on_tick)

        self._win.show()
        self._app.exec()

    # ---------- basic load / plot ----------

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

    # ---------- replay ----------

    def _on_replay_parquet(self) -> None:
        assert self._win is not None
        path, _ = QFileDialog.getOpenFileName(
            self._win,
            "Replay Parquet",
            str(Path.cwd()),
            "Parquet Files (*.parquet);;All Files (*.*)",
        )
        if not path:
            return
        try:
            tbl = self._read_parquet(Path(path))
            self._start_replay_from_table(tbl, Path(path).name)
        except Exception as e:  # noqa: BLE001
            QMessageBox.critical(self._win, "Error", f"Failed to start replay\n{e}")

    def _on_replay_demo(self) -> None:
        import numpy as np
        import pyarrow as pa

        n = 10_000
        ts = np.arange(n, dtype="int64")
        price = 100.0 + np.cumsum(np.random.normal(0, 0.05, n))
        tbl = pa.table({"ts": ts, "price": price})
        self._start_replay_from_table(tbl, "demo replay")

    def _on_play_pause(self) -> None:
        if self._timer is None:
            return

        win = self._win  # narrow Optional for mypy

        if self._r_ts is None:
            if win is not None:
                win.statusBar().showMessage("No replay loaded")
            return

        if self._r_running:
            self._timer.stop()
            self._r_running = False
            if win is not None:
                win.statusBar().showMessage("Paused")
        else:
            self._timer.start()
            self._r_running = True
            if win is not None:
                win.statusBar().showMessage("Playing")

    def _on_stop(self) -> None:
        if self._timer:
            self._timer.stop()
        self._r_running = False
        self._r_ts = None
        self._r_px = None
        self._r_idx = 0
        if self._curve:
            self._curve.setData([], [])
        if self._plot:
            self._plot.enableAutoRange()
        if self._win:
            self._win.statusBar().showMessage("Stopped")

    def _on_tick(self) -> None:
        if (
            not self._r_running
            or self._r_ts is None
            or self._r_px is None
            or self._curve is None
        ):
            return
        n = self._r_ts.shape[0]
        if self._r_idx >= n:
            self._on_stop()
            if self._win:
                self._win.statusBar().showMessage("Replay finished")
            return
        end = min(self._r_idx + self._r_step, n)
        self._curve.setData(self._r_ts[:end], self._r_px[:end])
        self._r_idx = end

    def _start_replay_from_table(self, tbl, label: str) -> None:
        import pyarrow as pa

        # pick 'ts' and 'price' or fallback to first two numeric columns
        names = tbl.column_names
        if "ts" in names and "price" in names:
            ts_col = tbl.column("ts")
            price_col = tbl.column("price")
        else:
            numeric_ix = [
                i
                for i, col in enumerate(tbl.columns)
                if pa.types.is_integer(col.type) or pa.types.is_floating(col.type)
            ]
            if len(numeric_ix) < 2:
                raise ValueError(
                    "Expected columns 'ts' and 'price' or at least two numeric columns."
                )
            ts_col = tbl.column(numeric_ix[0])
            price_col = tbl.column(numeric_ix[1])

        ts = ts_col.to_numpy(zero_copy_only=False)
        price = price_col.to_numpy(zero_copy_only=False)

        if pa.types.is_timestamp(ts_col.type):
            unit = ts_col.type.unit  # "s","ms","us","ns"
            scale = {"s": 1.0, "ms": 1e-3, "us": 1e-6, "ns": 1e-9}.get(unit, 1.0)
            ts = ts.astype("float64") * scale

        # reset state and prime chart
        if self._timer:
            self._timer.stop()
        self._r_ts = ts
        self._r_px = price
        self._r_idx = 0
        self._r_running = False

        if self._curve:
            self._curve.setData([], [])
        if self._plot:
            self._plot.enableAutoRange()
        if self._win:
            self._win.setWindowTitle(f"Market System Viewer — {label}")
            self._win.statusBar().showMessage(f"Replay loaded: {len(price)} rows")

        # auto-start
        self._on_play_pause()

    # ---------- load helpers ----------

    def _load_parquet(self, path: Path) -> None:
        tbl = self._read_parquet(path)
        self._plot_table(tbl, path.name)

    def _read_parquet(self, path: Path):
        import pyarrow.parquet as pq

        if path.is_dir():
            ds = pq.ParquetDataset(str(path))
            return ds.read()
        return pq.read_table(str(path))

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
