# Local SQL + PySpark Testing

This folder includes a local test runner that validates:
- Spark SQL solution (`<problem>.sql`)
- PySpark solution (`<problem>.py`)

for the same problem and checks they produce identical output.

## File Naming Convention

For each problem, keep these files in the same folder:
- `<problem>.sql`
- `<problem>.py`
- `<problem>.test.json`

Example:
- `leetcode_hard/active_user_retention.sql`
- `leetcode_hard/active_user_retention.py`
- `leetcode_hard/active_user_retention.test.json`

The PySpark file must expose a function with the same name as the file:
- File: `active_user_retention.py`
- Function: `active_user_retention(...) -> DataFrame`

## Test Case Format (`.test.json`)

```json
{
  "tables": {
    "table_name": {
      "schema": {
        "col1": "int",
        "col2": "date",
        "col3": "string"
      },
      "rows": [
        {"col1": 1, "col2": "2022-07-01", "col3": "value"}
      ]
    }
  },
  "expected": [
    {"out_col": 123}
  ],
  "sort_by": ["out_col"]
}
```

### Parameter mapping for PySpark function

For table `user_actions`, these parameter names are supported:
- `user_actions`
- `user_actions_df`

## Run Locally

From repository root:

```bash
python sql_pyspark/local_test_runner.py --problem leetcode_hard/active_user_retention
```

Or with make:

```bash
make sqlpyspark-test PROBLEM=leetcode_hard/active_user_retention
```

Simplest form:

```bash
make test leetcode_hard/active_user_retention
```

You can also pass:
- `make test active_user_retention`
- `make test leetcode_hard/active_user_retention.sql`
- `make test leetcode_hard/active_user_retention.py`
- `make test leetcode_hard/active_user_retention.test.json`

Clean local Spark/Python test artifacts:

```bash
make clean
```

Run all test cases:

```bash
python sql_pyspark/local_test_runner.py --all
```

Or:

```bash
make sqlpyspark-test-all
```

Run an explicit test case:

```bash
python sql_pyspark/local_test_runner.py --case leetcode_hard/active_user_retention.test.json
```

Or:

```bash
make sqlpyspark-test-case CASE=leetcode_hard/active_user_retention.test.json
```

## What the runner validates

For each case:
1. Builds temp Spark DataFrames from `tables`.
2. Registers temp views for SQL execution.
3. Executes `<problem>.sql`.
4. Executes `<problem>.py`.
5. Compares each output against `expected`.
6. Compares SQL output vs PySpark output.
