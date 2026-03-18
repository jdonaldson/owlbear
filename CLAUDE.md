# Claude Instructions for owlbear

## Project Overview
Owlbear is a Python client that bridges AWS Athena and Polars. It executes Athena SQL queries and returns results as typed Polars DataFrames via PyArrow. Named for its two halves: Owl (Athena) + Bear (Polars).

## Development Guidelines
- Use Polars for all data processing operations
- Follow Python packaging best practices with pyproject.toml
- Maintain compatibility with Python 3.10+

## Dependencies
- polars: Core data processing library
- boto3: AWS SDK for Athena integration

## Development Dependencies
- pytest: Testing framework
- black: Code formatter
- ruff: Linter
- mypy: Type checker

## Commands
- Install dependencies: `pip install -e .[dev]`
- Run tests: `pytest`
- Format code: `black .`
- Lint code: `ruff check .`
- Type check: `mypy src/` or `pyright src/owlbear/`

## Notes
- Pre-commit hook blocks `Co-Authored-By:.*Claude` in commit messages
- `dist/` has build artifacts from the PyPI upload (not gitignored but also not tracked)
- Athena `DESCRIBE` fails on partitioned tables with ragged metadata — always use `_get_columns()` (information_schema first, DESCRIBE fallback) for schema introspection
- Release flow: bump version in pyproject.toml, commit, push, `python -m build`, `python -m twine upload dist/owlbear-X.Y.Z*`
- MCP server (v0.6.0): 10 tools (with pagination + TABLESAMPLE + backend-aware snippets + explain/search/partitions), 2 prompts, 1 resource — configured in `~/.mcp.json`
