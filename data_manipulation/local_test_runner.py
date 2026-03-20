import argparse
import importlib.util
import inspect
import json
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyspark
from pyspark.sql import DataFrame, SparkSession, functions as F


ROOT_DIR = Path(__file__).resolve().parent


def _configure_spark_runtime() -> None:
    """
    Prefer the Spark distribution bundled with pyspark.
    This avoids failures when SPARK_HOME points to a missing local install.
    """
    bundled_spark_home = Path(pyspark.__file__).resolve().parent
    bundled_submit = bundled_spark_home / "bin" / "spark-submit"
    configured_submit = Path(os.environ.get("SPARK_HOME", "")) / "bin" / "spark-submit"

    if bundled_submit.exists() and not configured_submit.exists():
        os.environ["SPARK_HOME"] = str(bundled_spark_home)

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _normalize_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _normalize_rows(rows: list[dict[str, Any]], sort_by: list[str] | None) -> list[dict[str, Any]]:
    normalized = [{k: _normalize_value(v) for k, v in row.items()} for row in rows]
    if sort_by:
        return sorted(normalized, key=lambda r: tuple(r.get(k) for k in sort_by))
    return sorted(normalized, key=lambda r: json.dumps(r, sort_keys=True))


def _collect_rows(df: DataFrame, sort_by: list[str] | None) -> list[dict[str, Any]]:
    return _normalize_rows([row.asDict(recursive=True) for row in df.collect()], sort_by)


def _build_tables(spark: SparkSession, spec: dict[str, Any]) -> dict[str, DataFrame]:
    tables_spec = spec.get("tables", {})
    if not tables_spec:
        raise ValueError("Test spec must include non-empty 'tables'.")

    tables: dict[str, DataFrame] = {}
    for table_name, table_spec in tables_spec.items():
        rows = table_spec.get("rows", [])
        schema = table_spec.get("schema", {})
        df = spark.createDataFrame(rows)
        if schema:
            casted_cols = [F.col(col).cast(dtype).alias(col) for col, dtype in schema.items()]
            df = df.select(*casted_cols)
        df.createOrReplaceTempView(table_name)
        tables[table_name] = df
    return tables


def _load_problem_module(py_path: Path):
    spec = importlib.util.spec_from_file_location(py_path.stem, py_path)
    if not spec or not spec.loader:
        raise RuntimeError(f"Unable to load module from {py_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module


def _run_pyspark_solution(py_path: Path, tables: dict[str, DataFrame]) -> DataFrame:
    module = _load_problem_module(py_path)
    func_name = py_path.stem
    if not hasattr(module, func_name):
        raise AttributeError(
            f"Expected function '{func_name}' in {py_path.name}. "
            "Name your solution function exactly like the file name."
        )

    func = getattr(module, func_name)
    sig = inspect.signature(func)
    args = []
    for param_name in sig.parameters:
        if param_name in tables:
            args.append(tables[param_name])
            continue
        if param_name.endswith("_df"):
            table_name = param_name[:-3]
            if table_name in tables:
                args.append(tables[table_name])
                continue
        raise ValueError(
            f"Cannot resolve function parameter '{param_name}' in {py_path.name}. "
            f"Available tables: {sorted(tables.keys())}"
        )

    result = func(*args)
    if not isinstance(result, DataFrame):
        raise TypeError(f"{py_path.name}:{func_name} must return a pyspark.sql.DataFrame.")
    return result


def _run_sql_solution(spark: SparkSession, sql_path: Path) -> DataFrame:
    query = sql_path.read_text(encoding="utf-8")
    return spark.sql(query)


def _extract_scala_query(scala_content: str) -> str:
    patterns = [
        r'val\s+query(?:\s*:\s*[\w\.\[\]]+)?\s*=\s*"""(.*?)"""',
        r'val\s+sql(?:\s*:\s*[\w\.\[\]]+)?\s*=\s*"""(.*?)"""',
        r'val\s+sparkSql(?:\s*:\s*[\w\.\[\]]+)?\s*=\s*"""(.*?)"""',
    ]
    for pattern in patterns:
        match = re.search(pattern, scala_content, flags=re.DOTALL)
        if match:
            raw_query = match.group(1).strip()
            cleaned_lines = [re.sub(r"^\s*\|", "", line) for line in raw_query.splitlines()]
            return "\n".join(cleaned_lines).strip()

    raise ValueError(
        "Could not parse Scala query. Add one of: val query = \"\"\"...\"\"\", "
        "val sql = \"\"\"...\"\"\", or val sparkSql = \"\"\"...\"\"\"."
    )


def _run_scala_solution(spark: SparkSession, scala_path: Path) -> DataFrame | None:
    scala_content = scala_path.read_text(encoding="utf-8")
    try:
        query = _extract_scala_query(scala_content)
    except ValueError:
        # OOP Scala implementations may not embed SQL strings.
        if re.search(r"\bdef\s+solve\s*\(", scala_content):
            return None
        raise
    return spark.sql(query)


def _assert_equal(name: str, actual: list[dict[str, Any]], expected: list[dict[str, Any]]) -> None:
    if actual != expected:
        raise AssertionError(
            f"{name} output mismatch.\nExpected: {json.dumps(expected, indent=2)}\n"
            f"Actual: {json.dumps(actual, indent=2)}"
        )


def run_case(spark: SparkSession, case_path: Path) -> None:
    case_spec = _read_json(case_path)
    stem = case_path.stem.replace(".test", "")
    sql_path = case_path.with_name(f"{stem}.sql")
    py_path = case_path.with_name(f"{stem}.py")
    scala_path = case_path.with_name(f"{stem}.scala")
    if not sql_path.exists() or not py_path.exists():
        raise FileNotFoundError(
            f"Expected matching files for {case_path.name}: {sql_path.name} and {py_path.name}."
        )

    tables = _build_tables(spark, case_spec)
    sort_by = case_spec.get("sort_by", [])
    expected = _normalize_rows(case_spec.get("expected", []), sort_by)

    sql_result = _collect_rows(_run_sql_solution(spark, sql_path), sort_by)
    py_result = _collect_rows(_run_pyspark_solution(py_path, tables), sort_by)
    scala_result = None
    if scala_path.exists():
        scala_df = _run_scala_solution(spark, scala_path)
        if scala_df is not None:
            scala_result = _collect_rows(scala_df, sort_by)
        else:
            print(f"WARN: {scala_path.name} uses OOP Scala; SQL-string parity check skipped.")

    _assert_equal(f"{sql_path.name}", sql_result, expected)
    _assert_equal(f"{py_path.name}", py_result, expected)
    _assert_equal("SQL vs PySpark", sql_result, py_result)
    if scala_result is not None:
        _assert_equal(f"{scala_path.name}", scala_result, expected)
        _assert_equal("SQL vs Scala", sql_result, scala_result)
        _assert_equal("PySpark vs Scala", py_result, scala_result)


def _discover_all_cases() -> list[Path]:
    return sorted(ROOT_DIR.rglob("*.test.json"))


def _resolve_problem_case(problem: str) -> Path:
    direct = ROOT_DIR / f"{problem}.test.json"
    if direct.exists():
        return direct

    # Allow basename usage, e.g. `active_user_retention`.
    stem = Path(problem).name
    matches = sorted(ROOT_DIR.rglob(f"{stem}.test.json"))
    if not matches:
        raise FileNotFoundError(
            f"Could not find test case for problem '{problem}'. "
            "Use a relative path like leetcode_hard/active_user_retention "
            "or ensure a unique basename exists."
        )
    if len(matches) > 1:
        options = ", ".join(str(m.relative_to(ROOT_DIR)) for m in matches)
        raise ValueError(
            f"Problem name '{problem}' is ambiguous. "
            f"Use one of: {options}"
        )
    return matches[0]


def _resolve_cases(problem: str | None, case: str | None, run_all: bool) -> list[Path]:
    if run_all:
        return _discover_all_cases()

    if problem:
        problem_path = _resolve_problem_case(problem)
        return [problem_path]

    if case:
        return [ROOT_DIR / case]

    raise ValueError("Provide one of: --problem, --case, or --all.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run local SQL + PySpark parity tests for sql_pyspark problems."
    )
    parser.add_argument("--problem", help="Problem path without extension, e.g. leetcode_hard/active_user_retention")
    parser.add_argument("--case", help="Explicit .test.json path relative to sql_pyspark/")
    parser.add_argument("--all", action="store_true", help="Run all discovered test cases.")
    args = parser.parse_args()

    cases = _resolve_cases(args.problem, args.case, args.all)
    if not cases:
        raise ValueError("No test cases found.")

    _configure_spark_runtime()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("sql-pyspark-local-tests")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    failed = 0
    for case_path in cases:
        if not case_path.exists():
            print(f"FAIL: {case_path.relative_to(ROOT_DIR)} (missing case file)")
            failed += 1
            continue
        try:
            run_case(spark, case_path)
            print(f"PASS: {case_path.relative_to(ROOT_DIR)}")
        except Exception as exc:
            print(f"FAIL: {case_path.relative_to(ROOT_DIR)}")
            print(f"  {exc}")
            failed += 1

    spark.stop()
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
