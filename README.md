market_system
Purpose

A framework to ingest, normalize, align, and visualize market data originally sourced from Sierra Chart.

Sierra Chart is used only as the source of raw SCID/depth files and as the platform where trades are placed. This project focuses entirely on data handling, storage, and visualization outside of Sierra.

Why

Sierra Chart’s storage formats are inconsistent (SCID per-contract vs depth per-day).

Research, replay, and ML workflows require a canonical, columnar store (Parquet/Arrow).

Visualization and analysis should not depend on Sierra’s UI.

This project provides a clean, extensible pipeline for handling market data independently.

Scope

In scope:

Reading SCID and depth files

Converting to Arrow/Parquet partitioned by date/symbol

Aligning trades with depth changes

Storing aligned results

Custom viewer for visualization and replay

Real-time streaming and replay pipelines

Out of scope:

Trade execution

Writing back to Sierra formats

Project Structure

src/market_system/
  core/ — native C++/Rust bindings for parsing & alignment
  ingestion/ — SCID & depth readers
  storage/ — schemas, Parquet/Arrow writers
  alignment/ — trade ↔ depth alignment
  realtime/ — pub/sub, streaming
  viewer/ — standalone visualization & replay

tests/ — unit tests
examples/ — demo workflows
scripts/ — CLI utilities
docs/ — architecture & design notes

Status

Skeleton structure in place

CLI scaffolded with subcommands

Ingestion, storage, alignment, realtime, and viewer modules stubbed

Native core directories created (C++ and Rust)

CI scaffolding added

Roadmap

Finalize canonical schemas (trades, depth, aligned)

Implement ingestion → Parquet pipeline

Build native alignment engine (C++ or Rust)

Develop standalone viewer with replay controls

Add real-time streaming and replay capabilities

Requirements

Python 3.12+

Git

CMake
 3.27+

Visual Studio 2022
 with "Desktop development with C++" workload (Windows)

Or clang/gcc + CMake on Linux/macOS

Setup

git clone https://github.com/andyblair1230/market_system.git

cd market_system
python -m venv .venv
.venv\Scripts\activate # (Windows) or source .venv/bin/activate
pip install -e .[dev,viewer]