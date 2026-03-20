# SQL PySpark Test Runner Extension

This extension adds shortcuts to run local `sql_pyspark` test commands.

## Commands

- `SQL PySpark: Test Current File`
  - Runs `make test <target>` for the active file under `sql_pyspark`.
- `SQL PySpark: Test All Cases`
  - Runs `make sqlpyspark-test-all`.

## Shortcuts (default)

- `Cmd+Alt+T` -> test current file
- `Cmd+Alt+Shift+T` -> test all

## Requirements

- Open this repository root as your VS Code workspace.
- Ensure Python/PySpark runtime works locally.
- Ensure make targets in root `Makefile` are available.
