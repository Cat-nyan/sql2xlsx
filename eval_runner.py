import argparse
import json
import math
import os
import statistics
import time
from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import create_engine

from config import settings
from sql_agent import generate_sql


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.strip().rstrip(";").strip().lower().split())


def _is_readonly_sql(sql: str) -> bool:
    normalized = sql.strip().lower()
    if not normalized:
        return False
    if ";" in normalized:
        return False
    if "--" in normalized or "/*" in normalized or "*/" in normalized or "#" in normalized:
        return False
    if not (normalized.startswith("select") or normalized.startswith("with")):
        return False
    blocked = (
        "insert ",
        "update ",
        "delete ",
        "drop ",
        "alter ",
        "create ",
        "truncate ",
        "replace ",
        "grant ",
        "revoke ",
        "outfile",
        "load_file",
        "benchmark(",
        "sleep(",
        "information_schema",
        "mysql.",
        "performance_schema",
        "sys.",
    )
    return not any(keyword in normalized for keyword in blocked)


def _preview_sql(sql: str, limit: int) -> str:
    normalized = sql.strip().rstrip(";").strip()
    if not normalized:
        return normalized
    if " limit " in f" {_normalize_sql(normalized)} ":
        return normalized
    return f"{normalized} LIMIT {int(limit)}"


def _get_engine():
    settings.validate()
    return create_engine(
        f"mysql+mysqlconnector://{settings.DB_USERNAME}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DATABASE}",
        pool_pre_ping=True,
        connect_args={"connection_timeout": settings.DB_CONNECT_TIMEOUT_SECONDS},
    )


def _estimate_query_rows(sql: str) -> int | None:
    sql = sql.strip().rstrip(";").strip()
    if not sql:
        return None
    try:
        df = pd.read_sql(f"EXPLAIN {sql}", _get_engine())
    except Exception:
        return None
    if "rows" not in df.columns:
        return None
    rows = pd.to_numeric(df["rows"], errors="coerce").dropna()
    if rows.empty:
        return None
    return int(rows.max())


def _jsonable(value: Any):
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _stable_cell(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _fetch_preview(sql: str, preview_limit: int) -> tuple[list[str], list[tuple[str | None, ...]]]:
    df = pd.read_sql(_preview_sql(sql, preview_limit), _get_engine())
    df = df.where(pd.notnull(df), None)
    columns = [str(c) for c in df.columns.tolist()]
    raw_rows = df.to_numpy().tolist()
    rows = [tuple(_stable_cell(v) for v in row) for row in raw_rows]
    return columns, rows


def _normalize_columns(columns: list[str]) -> list[str]:
    return [c.strip().lower() for c in columns]


def _compare_query_results(sql_a: str, sql_b: str, preview_limit: int) -> tuple[bool, str | None]:
    cols_a, rows_a = _fetch_preview(sql_a, preview_limit)
    cols_b, rows_b = _fetch_preview(sql_b, preview_limit)

    ncols_a = _normalize_columns(cols_a)
    ncols_b = _normalize_columns(cols_b)
    if len(set(ncols_a)) != len(ncols_a) or len(set(ncols_b)) != len(ncols_b):
        return False, "duplicate_columns"

    if set(ncols_a) != set(ncols_b):
        return False, "columns_mismatch"

    common_cols = sorted(set(ncols_a))
    idx_a = {c: i for i, c in enumerate(ncols_a)}
    idx_b = {c: i for i, c in enumerate(ncols_b)}

    norm_rows_a = [tuple(row[idx_a[c]] for c in common_cols) for row in rows_a]
    norm_rows_b = [tuple(row[idx_b[c]] for c in common_cols) for row in rows_b]

    if sorted(norm_rows_a) != sorted(norm_rows_b):
        return False, "rows_mismatch"

    return True, None


def _read_jsonl(path: str) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            cases.append(json.loads(raw))
    return cases


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    values_sorted = sorted(values)
    k = (len(values_sorted) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(values_sorted[int(k)])
    d0 = values_sorted[int(f)] * (c - k)
    d1 = values_sorted[int(c)] * (k - f)
    return float(d0 + d1)


@dataclass
class CaseResult:
    case_id: str
    ok: bool
    readonly_ok: bool
    sql_generated: str
    sql_expected: str | None
    sql_exact_match: bool | None
    result_match: bool | None
    gen_ms: int
    exec_ms: int | None
    exec_ok: bool
    expected_exec_ms: int | None
    expected_exec_ok: bool | None
    export_refused: bool
    estimated_rows: int | None
    error: str | None
    result_match_error: str | None
    preview_columns: list[str] | None
    preview_rows: int | None


def _run_case(case: dict[str, Any], preview_limit: int) -> CaseResult:
    case_id = str(case.get("id") or case.get("case_id") or "").strip() or f"case_{int(time.time() * 1000)}"
    question = str(case.get("question") or "").strip()
    expected_sql_raw = case.get("expected_sql")
    expected_sql = str(expected_sql_raw).strip() if expected_sql_raw is not None else None

    t0 = time.perf_counter()
    sql_generated = ""
    exec_ms: int | None = None
    exec_ok = False
    preview_columns: list[str] | None = None
    preview_rows: int | None = None
    expected_exec_ms: int | None = None
    expected_exec_ok: bool | None = None
    estimated_rows: int | None = None
    export_refused = False
    readonly_ok = False
    error: str | None = None
    result_match: bool | None = None
    result_match_error: str | None = None

    try:
        sql_generated = generate_sql(question)
    except Exception as e:
        gen_ms = int((time.perf_counter() - t0) * 1000)
        return CaseResult(
            case_id=case_id,
            ok=False,
            readonly_ok=False,
            sql_generated="",
            sql_expected=expected_sql,
            sql_exact_match=None,
            result_match=None,
            gen_ms=gen_ms,
            exec_ms=None,
            exec_ok=False,
            expected_exec_ms=None,
            expected_exec_ok=None,
            export_refused=False,
            estimated_rows=None,
            error=str(e),
            result_match_error=None,
            preview_columns=None,
            preview_rows=None,
        )

    gen_ms = int((time.perf_counter() - t0) * 1000)
    readonly_ok = _is_readonly_sql(sql_generated)

    sql_exact_match: bool | None = None
    if expected_sql is not None:
        sql_exact_match = _normalize_sql(sql_generated) == _normalize_sql(expected_sql)

    estimated_rows_limit = int(settings.EXPORT_ESTIMATED_ROW_LIMIT or 0)
    if estimated_rows_limit > 0 and readonly_ok:
        estimated_rows = _estimate_query_rows(sql_generated)
        if estimated_rows is not None and estimated_rows > estimated_rows_limit:
            export_refused = True

    if not readonly_ok:
        return CaseResult(
            case_id=case_id,
            ok=False,
            readonly_ok=False,
            sql_generated=sql_generated,
            sql_expected=expected_sql,
            sql_exact_match=sql_exact_match,
            result_match=None,
            gen_ms=gen_ms,
            exec_ms=None,
            exec_ok=False,
            expected_exec_ms=None,
            expected_exec_ok=None,
            export_refused=export_refused,
            estimated_rows=estimated_rows,
            error="readonly_check_failed",
            result_match_error=None,
            preview_columns=None,
            preview_rows=None,
        )

    t1 = time.perf_counter()
    try:
        df = pd.read_sql(_preview_sql(sql_generated, preview_limit), _get_engine())
        exec_ms = int((time.perf_counter() - t1) * 1000)
        exec_ok = True
        preview_columns = [str(c) for c in df.columns.tolist()]
        preview_rows = int(len(df.index))
    except Exception as e:
        exec_ms = int((time.perf_counter() - t1) * 1000)
        exec_ok = False
        error = str(e)

    if expected_sql is not None and exec_ok:
        if _is_readonly_sql(expected_sql):
            t2 = time.perf_counter()
            try:
                ok_match, match_err = _compare_query_results(sql_generated, expected_sql, preview_limit)
                expected_exec_ms = int((time.perf_counter() - t2) * 1000)
                expected_exec_ok = True
                result_match = ok_match
                result_match_error = match_err
            except Exception as e:
                expected_exec_ms = int((time.perf_counter() - t2) * 1000)
                expected_exec_ok = False
                result_match = False
                result_match_error = str(e)
        else:
            expected_exec_ok = None
            expected_exec_ms = None
            result_match = None
            result_match_error = "expected_sql_readonly_check_failed"

    ok = readonly_ok and exec_ok
    return CaseResult(
        case_id=case_id,
        ok=ok,
        readonly_ok=readonly_ok,
        sql_generated=sql_generated,
        sql_expected=expected_sql,
        sql_exact_match=sql_exact_match,
        result_match=result_match,
        gen_ms=gen_ms,
        exec_ms=exec_ms,
        exec_ok=exec_ok,
        expected_exec_ms=expected_exec_ms,
        expected_exec_ok=expected_exec_ok,
        export_refused=export_refused,
        estimated_rows=estimated_rows,
        error=error,
        result_match_error=result_match_error,
        preview_columns=preview_columns,
        preview_rows=preview_rows,
    )


def _aggregate(results: Iterable[CaseResult]) -> dict[str, Any]:
    results_list = list(results)
    total = len(results_list)
    readonly_ok = sum(1 for r in results_list if r.readonly_ok)
    exec_ok = sum(1 for r in results_list if r.exec_ok)
    ok = sum(1 for r in results_list if r.ok)
    export_refused = sum(1 for r in results_list if r.export_refused)

    gen_ms_values = [float(r.gen_ms) for r in results_list if r.gen_ms is not None]
    exec_ms_values = [float(r.exec_ms) for r in results_list if r.exec_ms is not None]

    match_den = sum(1 for r in results_list if r.sql_exact_match is not None)
    match_num = sum(1 for r in results_list if r.sql_exact_match is True)

    result_match_den = sum(1 for r in results_list if r.result_match is not None)
    result_match_num = sum(1 for r in results_list if r.result_match is True)

    return {
        "total": total,
        "ok": ok,
        "ok_rate": (ok / total) if total else None,
        "readonly_ok": readonly_ok,
        "readonly_ok_rate": (readonly_ok / total) if total else None,
        "exec_ok": exec_ok,
        "exec_ok_rate": (exec_ok / total) if total else None,
        "sql_exact_match": match_num,
        "sql_exact_match_rate": (match_num / match_den) if match_den else None,
        "result_match": result_match_num,
        "result_match_rate": (result_match_num / result_match_den) if result_match_den else None,
        "export_refused": export_refused,
        "export_refused_rate": (export_refused / total) if total else None,
        "gen_ms_avg": statistics.mean(gen_ms_values) if gen_ms_values else None,
        "gen_ms_p95": _percentile(gen_ms_values, 95) if gen_ms_values else None,
        "exec_ms_avg": statistics.mean(exec_ms_values) if exec_ms_values else None,
        "exec_ms_p95": _percentile(exec_ms_values, 95) if exec_ms_values else None,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", default=os.getenv("EVAL_CASES", "eval_cases.jsonl"))
    parser.add_argument("--preview-limit", type=int, default=int(os.getenv("EVAL_PREVIEW_LIMIT", "200")))
    parser.add_argument("--out", default=os.getenv("EVAL_OUT", "eval_report.json"))
    args = parser.parse_args()

    cases = _read_jsonl(args.cases)
    results: list[CaseResult] = []
    for case in cases:
        results.append(_run_case(case, args.preview_limit))

    report = {
        "meta": {
            "cases_path": args.cases,
            "preview_limit": args.preview_limit,
        },
        "summary": _aggregate(results),
        "cases": [
            {
                "id": r.case_id,
                "ok": r.ok,
                "readonly_ok": r.readonly_ok,
                "exec_ok": r.exec_ok,
                "sql_exact_match": r.sql_exact_match,
                "result_match": r.result_match,
                "gen_ms": r.gen_ms,
                "exec_ms": r.exec_ms,
                "export_refused": r.export_refused,
                "estimated_rows": r.estimated_rows,
                "error": r.error,
                "expected_exec_ok": r.expected_exec_ok,
                "expected_exec_ms": r.expected_exec_ms,
                "result_match_error": r.result_match_error,
                "sql_generated": r.sql_generated,
                "sql_expected": r.sql_expected,
                "preview_columns": r.preview_columns,
                "preview_rows": r.preview_rows,
            }
            for r in results
        ],
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(json.dumps(report["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
