.PHONY: install lint test format doctor

install:
	pip install -e .[dev]

lint:
	ruff check src tests

format:
	black src tests

test:
	pytest -q

doctor:
	@echo "Python:" && python --version
	@echo "Virtualenv Python:" && where python
	@echo "Pre-commit:" && pre-commit --version
	@echo "Black:" && black --version
	@echo "Ruff:" && ruff --version
	@echo "Mypy:" && mypy --version
	@echo "Pytest:" && pytest --version
	@echo "CMake:" && cmake --version || echo "CMake not found"
	@echo "MSVC cl:" && cl 2>NUL | more +1 || echo "cl not found (open Native Tools prompt)"
