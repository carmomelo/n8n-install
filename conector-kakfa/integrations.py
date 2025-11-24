import json, sys, time, requests
from typing import Any, Dict, Optional
from . import config

def post_webhook_with_retry(payload: Dict[str, Any]) -> int:
    """POST no webhook do n8n com retry/backoff. Retorna status_code (2xx = sucesso)."""
    if not config.WEBHOOK_URL:
        raise RuntimeError("WEBHOOK_URL não configurado")

    headers = {"Content-Type": "application/json"}
    if config.WH_AUTH_HDR:
        try:
            k, v = config.WH_AUTH_HDR.split(":", 1)
            headers[k.strip()] = v.strip()
        except Exception:
            sys.stderr.write("[Webhook] WEBHOOK_AUTH_HEADER inválido; use 'Nome: valor'\n")

    attempt, backoff, last_exc = 0, config.WH_BACKOFF, None
    while attempt < config.WH_RETRIES:
        attempt += 1
        try:
            r = requests.post(
                config.WEBHOOK_URL,
                json=payload,
                timeout=config.WH_TIMEOUT,
                verify=config.WH_VERIFY_SSL,
                headers=headers,
            )
            if 200 <= r.status_code < 300:
                return r.status_code
            else:
                sys.stderr.write(f"[Webhook] HTTP {r.status_code}: {r.text[:300]}\n")
        except Exception as e:
            last_exc = e
            sys.stderr.write(f"[Webhook] erro no POST ({attempt}/{config.WH_RETRIES}): {e}\n")
        time.sleep(backoff)
        backoff *= 2.0
    if last_exc:
        raise last_exc
    raise RuntimeError("Falha no POST do webhook (sem exceção explícita)")

def custom_failure_log(context: Dict[str, Any]) -> None:
    """
    Insert em: <catalog>.<schema>.<table>
      (session_id, message_id, payload, dt_ms, reason, attempts, last_error)
    via Databricks SQL Statements API (INLINE).
    """
    if not (config.DB_SQL_URL and config.DB_TOKEN and config.DB_WAREHOUSE):
        sys.stderr.write("[FailureLog] SQL_URL/TOKEN/WAREHOUSE_ID ausentes; pulando\n")
        return

    payload_postado: Dict[str, Any] = context.get("payload_postado") or {}

    def _get(d: Dict[str, Any], path: str, default: Optional[str] = "") -> Optional[str]:
        try:
            cur: Any = d
            for p in path.split("."):
                if isinstance(cur, dict) and p in cur:
                    cur = cur[p]
                else:
                    return default
            return None if cur is None else str(cur)
        except Exception:
            return default

    session_id = _get(payload_postado, "request.session.id", "")
    message_id = _get(payload_postado, "request.message.id", "")
    record_payload_str = json.dumps(payload_postado, ensure_ascii=False)
    ts_ms = int(time.time() * 1000)

    statement_sql = f"""
INSERT INTO `{config.DB_CATALOG}`.`{config.DB_SCHEMA}`.`{config.DB_TABLE}`
  (session_id, message_id, payload, dt_ms, reason, attempts, last_error)
VALUES (
  :p_session_id,
  :p_message_id,
  :p_payload,
  to_timestamp(CAST(:p_ts_ms AS BIGINT) / 1000.0),
  :p_reason,
  :p_attempts,
  :p_last_error
)
"""

    body = {
        "catalog": config.DB_CATALOG,
        "schema": config.DB_SCHEMA,
        "statement": statement_sql,
        "parameters": [
            {"name": "p_session_id", "value": session_id},
            {"name": "p_message_id", "value": message_id},
            {"name": "p_payload",    "value": record_payload_str},
            {"name": "p_ts_ms",      "value": str(ts_ms)},
            {"name": "p_reason",     "value": str(context.get("reason", ""))},
            {"name": "p_attempts",   "value": str(context.get("attempts", ""))},
            {"name": "p_last_error", "value": str(context.get("last_error", ""))}
        ],
        "warehouse_id": config.DB_WAREHOUSE,
        "wait_timeout": config.DB_WAIT_TO,
        "disposition": "INLINE",
        "format": "JSON_ARRAY"
    }

    headers = {
        "Authorization": f"Bearer {config.DB_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        resp = requests.post(config.DB_SQL_URL, headers=headers, data=json.dumps(body),
                             verify=config.DB_VERIFY_SSL, timeout=30)
        if not (200 <= resp.status_code < 300):
            sys.stderr.write(f"[FailureLog] HTTP {resp.status_code}: {resp.text[:500]}\n")
            return
        try:
            j = resp.json()
            st = j.get("status")
            if isinstance(st, dict) and st.get("state") not in (None, "SUCCEEDED"):
                sys.stderr.write(f"[FailureLog] status={st}\n")
        except Exception:
            pass
    except Exception as e:
        sys.stderr.write(f"[FailureLog] erro Databricks: {e}\n")
