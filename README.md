## Requirements

- Python 3.12+
- Git
- [CMake](https://cmake.org/download/) 3.27+
- [Visual Studio 2022](https://visualstudio.microsoft.com/vs/) with "Desktop development with C++" workload (Windows)
- Or clang/gcc + CMake on Linux/macOS

## Setup

```bash
git clone https://github.com/andyblair1230/market_system.git
cd market_system
python -m venv .venv
.venv\Scripts\activate  # (Windows) or source .venv/bin/activate
pip install -e .[dev,viewer]
