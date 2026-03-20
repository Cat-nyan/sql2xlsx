from flask import Flask, request, send_file, jsonify, g
from sqlalchemy import create_engine
import pandas as pd
from io import BytesIO
from sql_agent import generate_sql
from config import settings
import uuid
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from functools import lru_cache
from typing import Any
import time
from threading import Lock
import re
import logging
import secrets
from logging import StreamHandler

app = Flask(__name__, static_folder='static')


class _RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        request_id = ""
        try:
            request_id = getattr(g, "request_id", "") or ""
        except Exception:
            request_id = ""
        setattr(record, "request_id", request_id)
        return True


def _configure_logging():
    level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    app.logger.setLevel(level)
    if not any(isinstance(h, StreamHandler) for h in app.logger.handlers):
        handler = StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s request_id=%(request_id)s %(message)s")
        )
        handler.addFilter(_RequestIdFilter())
        app.logger.addHandler(handler)
        app.logger.propagate = False


_configure_logging()

limiter = Limiter(get_remote_address, app=app, default_limits=["5 per minute"])

PREVIEW_ROW_LIMIT = 200
QUERY_CACHE_TTL_SECONDS = 10 * 60
_QUERY_CACHE: dict[str, dict[str, Any]] = {}
_QUERY_CACHE_LOCK = Lock()


@app.before_request
def _attach_request_id():
    g.request_id = request.headers.get("X-Request-Id") or uuid.uuid4().hex
    g.request_start = time.monotonic()


@app.after_request
def _log_request(response):
    try:
        request_id = getattr(g, "request_id", "") or ""
        elapsed_ms = int((time.monotonic() - float(getattr(g, "request_start", 0.0))) * 1000)
        app.logger.info(
            '%s %s -> %s %sms ip=%s ua="%s" request_id=%s',
            request.method,
            request.path,
            response.status_code,
            elapsed_ms,
            request.remote_addr or "",
            (request.user_agent.string or "").replace("\n", " ").replace("\r", " "),
            request_id,
            extra={"request_id": request_id},
        )
    except Exception:
        pass

    response.headers["X-Request-Id"] = getattr(g, "request_id", "") or ""
    return response


@lru_cache(maxsize=1)
def _get_engine():
    return create_engine(
        f"mysql+mysqlconnector://{settings.DB_USERNAME}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DATABASE}",
        pool_pre_ping=True,
        connect_args={"connection_timeout": settings.DB_CONNECT_TIMEOUT_SECONDS},
    )


def _xlsx_response(data: pd.DataFrame, filename: str):
    bio = BytesIO()
    data.to_excel(bio, index=False)
    bio.seek(0)
    return send_file(
        bio,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def _message_xlsx(message: str, filename: str = "message.xlsx"):
    return _xlsx_response(pd.DataFrame({"message": [message]}), filename)


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
    if re.search(r"\blimit\b", normalized, flags=re.IGNORECASE):
        return normalized
    return f"{normalized} LIMIT {int(limit)}"


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


def _cache_cleanup(now: float):
    expired = []
    for query_id, item in _QUERY_CACHE.items():
        created_at = float(item.get("created_at", 0.0))
        if now - created_at > QUERY_CACHE_TTL_SECONDS:
            expired.append(query_id)
    for query_id in expired:
        _QUERY_CACHE.pop(query_id, None)


def _cache_put(sql: str) -> str:
    now = time.monotonic()
    query_id = uuid.uuid4().hex
    with _QUERY_CACHE_LOCK:
        _cache_cleanup(now)
        _QUERY_CACHE[query_id] = {"sql": sql, "created_at": now}
    return query_id


def _cache_get(query_id: str) -> str | None:
    now = time.monotonic()
    with _QUERY_CACHE_LOCK:
        _cache_cleanup(now)
        item = _QUERY_CACHE.get(query_id)
        if not item:
            return None
        return str(item.get("sql", "")).strip() or None


def _to_jsonable(value: Any):
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


@app.route('/')
def index():
    return app.send_static_file('index.html')


def _auth_failed(token: str) -> bool:
    expected = settings.AUTH_SECRET or ""
    return not secrets.compare_digest(token, expected)


def _config_error_response(error: Exception):
    app.logger.error(str(error))
    return jsonify({"ok": False, "error": "服务配置错误，请联系管理员。"}), 500


@app.route('/preview', methods=['POST'])
@limiter.limit("10 per minute")
def preview():
    try:
        settings.validate()
    except Exception as e:
        return _config_error_response(e)

    request_data: dict[str, Any] = request.get_json(silent=True) or {}
    question = str(request_data.get("question", "")).strip()
    token = str(request_data.get("token", "")).strip()

    if not question:
        return jsonify({"ok": False, "error": "请输入问题"}), 400
    if len(question) > settings.QUESTION_MAX_CHARS:
        return jsonify({"ok": False, "error": "问题过长，请缩短后重试。"}), 400
    if _auth_failed(token):
        return jsonify({"ok": False, "error": "Token错误"}), 401

    try:
        sql = generate_sql(question)
    except Exception as e:
        app.logger.exception("SQL 生成失败", extra={"request_id": getattr(g, "request_id", "")})
        return jsonify({"ok": False, "error": "SQL生成失败，请重试。"}), 500

    if not _is_readonly_sql(sql):
        return jsonify({"ok": False, "error": "查询失败，请重试。"}), 400

    limited_sql = _preview_sql(sql, PREVIEW_ROW_LIMIT)
    try:
        df = pd.read_sql(limited_sql, _get_engine())
        limited_sql = " ".join(limited_sql.split())
        app.logger.info(f"用户问题：{question},预览查询:\n {limited_sql}", extra={"request_id": getattr(g, "request_id", "")})
    except Exception:
        app.logger.exception("执行预览查询失败", extra={"request_id": getattr(g, "request_id", "")})
        return jsonify({"ok": False, "error": "执行查询失败，请检查数据库或重试。"}), 500
    query_id = _cache_put(sql)
    df = df.where(pd.notnull(df), None)
    columns = [str(c) for c in df.columns.tolist()]
    rows = [[_to_jsonable(v) for v in row] for row in df.to_numpy().tolist()]
    truncated = len(rows) >= PREVIEW_ROW_LIMIT and not re.search(r"\blimit\b", sql, flags=re.IGNORECASE)
    return jsonify(
        {
            "ok": True,
            "query_id": query_id,
            "columns": columns,
            "rows": rows,
            "preview_limit": PREVIEW_ROW_LIMIT,
            "truncated": truncated,
        }
    )


@app.route('/export', methods=['POST'])
@limiter.limit("5 per minute")
def export():
    try:
        settings.validate()
    except Exception as e:
        app.logger.error(str(e))
        return _message_xlsx("服务配置错误，请联系管理员。", filename="error.xlsx")

    request_data: dict[str, Any] = request.get_json(silent=True) or {}
    token = str(request_data.get("token", "")).strip()
    query_id = str(request_data.get("query_id", "")).strip()

    if _auth_failed(token):
        return _message_xlsx("Token错误")

    sql: str | None = None
    if query_id:
        sql = _cache_get(query_id)
        if not sql:
            return _message_xlsx("查询已过期，请重新预览后再导出。", filename="error.xlsx")
    else:
        return _message_xlsx("缺少 query_id。", filename="error.xlsx")

    if not _is_readonly_sql(sql):
        return _message_xlsx("查询失败，请重试。", filename="error.xlsx")

    estimated_rows_limit = int(settings.EXPORT_ESTIMATED_ROW_LIMIT or 0)
    if estimated_rows_limit > 0:
        estimated_rows = _estimate_query_rows(sql)
        if estimated_rows is not None and estimated_rows > estimated_rows_limit:
            return _message_xlsx(
                f"预计结果约 {estimated_rows} 行，超过导出上限 {estimated_rows_limit} 行，请缩小范围或增加 LIMIT。",
                filename="error.xlsx",
            )

    try:
        data = pd.read_sql(sql, _get_engine())
    except Exception:
        app.logger.exception("执行导出查询失败", extra={"request_id": getattr(g, "request_id", "")})
        return _message_xlsx("执行查询失败，请检查数据库或重试。", filename="error.xlsx")

    filename = f"{uuid.uuid4().hex}.xlsx"
    return _xlsx_response(data, filename)

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=settings.DEBUG, port=settings.PORT)
