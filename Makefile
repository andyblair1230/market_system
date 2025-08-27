.PHONY: install lint test format

install:
	pip install -e .[dev]

lint:
	ruff check src tests

format:
	black src tests

test:
	pytest -q
