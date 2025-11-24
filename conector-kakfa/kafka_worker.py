import json
import sys
import time
import signal
import atexit  # <-- ADICIONADO
from typing import Any, Dict, Optional, List

from confluent_kafka import Consumer, Producer, KafkaException, KafkaError

from . import config
from .integrations import post_webhook_with_retry, custom_failure_log


# =========================
# Logging helpers
# =========================
def _log(msg: str) -> None:
    sys.stdout.write(msg.rstrip() + "\n")
    sys.stdout.flush()

def _err(msg: str) -> None:
    sys.stderr.write(msg.rstrip() + "\n")
    sys.stderr.flush()


# =========================
# Decode / headers helpers
# =========================
def _decode_value(val: Any):
    if isinstance(val, (bytes, bytearray)):
        try:
            s = val.decode("utf-8")
        except Exception:
            return {"_raw_bytes": list(val)}
        try:
            return json.loads(s)
        except Exception:
            return {"_raw_text": s}
    return val

def _headers_to_dict(kheaders) -> Dict[str, Optional[str]]:
    d: Dict[str, Optional[str]] = {}
    if not kheaders:
        return d
    for k, v in kheaders:
        key = k.decode("utf-8") if isinstance(k, (bytes, bytearray)) else k
        if isinstance(v, (bytes, bytearray)):
            try:
                d[key] = v.decode("utf-8")
            except Exception:
                d[key] = None
        else:
            d[key] = v
    return d

def _get_nested(d: Any, path: str, default: Optional[str] = "") -> Optional[str]:
    """
    Lê d['a']['b']['c'] a partir de path="a.b.c" de forma segura.
    Retorna default se o caminho não existir ou não for dict.
    """
    try:
        cur = d
        for p in path.split("."):
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                return default
        return default if cur is None else str(cur)
    except Exception:
        return default


# =========================
# Watchdogs e estado de grupo
# =========================
ASSIGNMENT_GRACE_SECS = 45.0        # tempo máximo sem assignment antes de recriar o consumer
STATS_GRACE_SECS = 60.0             # se stats indicarem 0 partitions assigned por mais tempo, reinicia
_last_assigned_ts: float = 0.0
_assigned: bool = False
_subscribe_start_ts: float = 0.0

# estado vindo de statistics do librdkafka
_last_stats_ts: float = 0.0
_last_stats_assignment_size: int = 0
_last_stats_group_state: str = "unknown"

def _mark_assigned():
    global _assigned, _last_assigned_ts
    _assigned = True
    _last_assigned_ts = time.time()

def _mark_revoked():
    global _assigned
    _assigned = False

def _should_restart_for_empty_assignment() -> bool:
    """
    Retorna True se estamos sem assignment há mais que ASSIGNMENT_GRACE_SECS.
    Considera também o tempo desde o subscribe caso nunca tenha havido assignment.
    """
    now = time.time()
    if _assigned:
        return False
    if _last_assigned_ts == 0.0:
        return (now - _subscribe_start_ts) > ASSIGNMENT_GRACE_SECS
    return (now - _last_assigned_ts) > ASSIGNMENT_GRACE_SECS

def _update_stats(stats_json: str):
    """
    Recebe JSON de statistics do librdkafka, atualiza métricas e decide reinício se necessário.
    """
    global _last_stats_ts, _last_stats_assignment_size, _last_stats_group_state
    _last_stats_ts = time.time()
    try:
        s = json.loads(stats_json)
    except Exception:
        return

    cgrp = s.get("cgrp", {})
    state = cgrp.get("state", "unknown")
    asn_size = int(cgrp.get("assignment_size", 0))

    _last_stats_group_state = state
    _last_stats_assignment_size = asn_size

def _should_restart_for_stats_empty() -> bool:
    """
    Se as stats mostrarem assignment_size == 0 por um período, reinicia para tentar sair de "Empty".
    """
    if _last_stats_ts == 0.0:
        return False
    if _last_stats_assignment_size > 0:
        return False
    # Sem assignment nas stats por tempo demais
    return (time.time() - _last_stats_ts) > STATS_GRACE_SECS


# =========================
# Kafka Producer (fallback)
# =========================
_producer: Optional[Producer] = None
_current_consumer: Optional[Consumer] = None  # <-- ADICIONADO

def get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer(config.producer_conf())
        _log("[kafka] producer criado")
    return _producer

def send_fallback_message(
    retorno_topic: str,
    key: Optional[bytes],
    correlation_id: Optional[str],
    session_id: Optional[str],
    retorno_text: str
) -> None:
    prod = get_producer()
    headers: List[tuple] = []
    if correlation_id:
        headers.append(("correlation_id", correlation_id.encode("utf-8")))
    if session_id:
        headers.append(("session_id", session_id.encode("utf-8")))
    headers.append(("content_type", b"application/json"))

    value = json.dumps(
        {
            "reply": retorno_text,
            "unavailable": True,
            "timestamp": int(time.time() * 1000),
        },
        ensure_ascii=False,
    ).encode("utf-8")

    def _delivery(err, msg):
        if err is not None:
            _err(f"[fallback] falha no envio: {err}")
        else:
            _log(f"[fallback] entregue em {msg.topic()}[{msg.partition()}]@{msg.offset()}")

    prod.produce(
        topic=retorno_topic,
        key=key,
        value=value,
        headers=headers,
        on_delivery=_delivery,
    )
    prod.flush(10)


# =========================
# Kafka Consumer
# =========================
def on_assign(consumer: Consumer, partitions):
    parts = ", ".join(f"{p.topic}-{p.partition} @ {p.offset}" for p in partitions)
    _log(f"[kafka] on_assign -> {parts}")
    _mark_assigned()

def on_revoke(consumer: Consumer, partitions):
    parts = ", ".join(f"{p.topic}-{p.partition}" for p in partitions)
    _log(f"[kafka] on_revoke -> {parts}")
    _mark_revoked()

def on_lost(consumer: Consumer, partitions):
    parts = ", ".join(f"{p.topic}-{p.partition}" for p in partitions)
    _err(f"[kafka] on_lost -> {parts}")
    _mark_revoked()

def _stats_cb(stats: str):
    # Log leve (opcional). Comente se ficar verboso.
    # _log(f"[kafka] stats: {stats}")
    _update_stats(stats)

def get_consumer() -> Consumer:
    global _subscribe_start_ts, _assigned, _last_assigned_ts, _current_consumer
    _assigned = False
    _last_assigned_ts = 0.0
    _subscribe_start_ts = time.time()

    # --- Base das configs do seu projeto:
    conf = config.consumer_conf()

    # --- Garantias e tempos (podem ser sobrepostos pelo seu consumer_conf()):
    conf.setdefault("enable.auto.commit", False)
    conf.setdefault("session.timeout.ms", 45000)                 # 45s
    conf.setdefault("heartbeat.interval.ms", 3000)               # 3s
    conf.setdefault("max.poll.interval.ms", 300000)              # 5min
    conf.setdefault("partition.assignment.strategy", "cooperative-sticky")
    conf.setdefault("statistics.interval.ms", 10000)             # 10s de stats
    conf["stats_cb"] = _stats_cb

    # Static membership (opcional)
    if getattr(config, "GROUP_INSTANCE_ID", None):
        conf["group.instance.id"] = config.GROUP_INSTANCE_ID

    c = Consumer(conf)
    c.subscribe(
        [config.KAFKA_TOPIC],
        on_assign=on_assign,
        on_revoke=on_revoke,
        on_lost=on_lost,
    )
    _current_consumer = c  # <-- ADICIONADO
    _log(
        "[kafka] consumer criado | topic="
        f"{config.KAFKA_TOPIC} | group={config.KAFKA_GROUP_ID} | "
        f"group.instance.id={conf.get('group.instance.id','-')}"
    )
    return c


# =========================
# Mensagem -> Webhook + Commit
# =========================
def handle(consumer: Consumer, msg) -> None:
    value_obj = _decode_value(msg.value())
    headers = _headers_to_dict(msg.headers() or [])
    key = msg.key()  # bytes ou None
    key_str = key.decode("utf-8") if isinstance(key, (bytes, bytearray)) else key

    # IDs úteis do payload/headers para log
    session_id = _get_nested(value_obj, "request.session.id", "")
    message_id = _get_nested(value_obj, "request.message.id", "")

    # correlation_id: header -> corpo (fallback)
    correlation_id = headers.get("correlation_id")
    if not correlation_id:
        correlation_id = _get_nested(value_obj, "request.correlation_id", "")

    # session_id no header (se existir)
    session_id_hdr = headers.get("session_id")

    # reply_to do corpo: usado como fallback de TOPIC_RETORNO
    reply_to_topic = _get_nested(value_obj, "request.reply_to", "")

    payload = {
        "kafka": {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp": msg.timestamp()[1] if msg.timestamp() else None,
            "key": key_str,
            "headers": headers,
        },
        "data": value_obj,
    }

    # 1) tenta webhook
    try:
        status = post_webhook_with_retry(payload)
        _log(
            "[webhook] sucesso HTTP "
            f"{status} | offset={msg.offset()} | "
            f"session_id={session_id or '-'} | message_id={message_id or '-'} | "
            f"correlation_id={(correlation_id or '-')}"
        )
        # commit síncrono após sucesso
        consumer.commit(message=msg, asynchronous=False)
        return

    except Exception as e:
        _err(
            "[webhook] falhou após retries: "
            f"{e} | offset={msg.offset()} | "
            f"session_id={session_id or '-'} | message_id={message_id or '-'} | "
            f"correlation_id={(correlation_id or '-')}"
        )

        # 2) registra falha (Databricks)
        context = {
            "reason": "webhook_max_retries_exceeded",
            "attempts": config.WH_RETRIES,
            "last_error": str(e),
            "webhook_url": config.WEBHOOK_URL,
            "kafka": payload["kafka"],
            "payload_postado": payload["data"],
        }
        try:
            custom_failure_log(context)
            _log("[failure_log] registrado no Databricks")
        except Exception as log_exc:
            _err(f"[failure_log] erro ao registrar: {log_exc}")

        # 3) fallback para tópico de retorno (se configurado)
        retorno_topic = config.KAFKA_TOPIC_RETORNO or reply_to_topic or ""
        if retorno_topic:
            try:
                send_fallback_message(
                    retorno_topic=retorno_topic,
                    key=msg.key(),
                    correlation_id=correlation_id or None,
                    session_id=session_id_hdr or session_id,
                    retorno_text=config.RETORNO_MESSAGE,
                )
                _log(
                    "[fallback] publicado | "
                    f"topic={retorno_topic} | "
                    f"session_id={session_id or session_id_hdr or '-'} | "
                    f"message_id={message_id or '-'}"
                )
            except Exception as prod_exc:
                _err(f"[fallback] erro ao publicar: {prod_exc}")
        else:
            _err("[fallback] Sem TOPIC_RETORNO e sem request.reply_to; pulando envio")

        # 4) sua regra: **comitar mesmo em falha** e seguir adiante
        try:
            consumer.commit(message=msg, asynchronous=False)
            _log(
                f"[commit] feito (mesmo em falha) | offset={msg.offset()} | "
                f"session_id={session_id or '-'} | message_id={message_id or '-'}"
            )
        except Exception as commit_exc:
            _err(f"[commit] erro ao commitar offset: {commit_exc}")


# =========================
# Loop resiliente com reconexão + shutdown limpo
# =========================
running = True

def _graceful_close():  # <-- ADICIONADO
    try:
        if _current_consumer is not None:
            _log("[shutdown] fechando consumer...")
            _current_consumer.close()
    except Exception as e:
        _err(f"[shutdown] erro ao fechar consumer: {e}")
    try:
        if _producer is not None:
            _log("[shutdown] flush producer...")
            _producer.flush(5)
    except Exception as e:
        _err(f"[shutdown] erro ao flush producer: {e}")

def _stop(*_):
    global running
    running = False
    _graceful_close()  # <-- ADICIONADO

signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)
atexit.register(_graceful_close)  # <-- ADICIONADO


def _poll_loop(c: Consumer) -> None:
    last_idle_log = 0.0
    last_topics_check = 0.0
    while running:
        # Watchdogs:
        # 1) sem assignment por muito tempo (callbacks)
        if _should_restart_for_empty_assignment():
            raise RuntimeError("no-assignment-timeout")

        # 2) stats indicam assignment_size=0 por muito tempo
        if _should_restart_for_stats_empty():
            raise RuntimeError(f"stats-empty-timeout (state={_last_stats_group_state})")

        # 3) a cada 60s, valida metadata (sanity de conexão)
        now = time.time()
        if now - last_topics_check > 60:
            try:
                c.list_topics(timeout=5.0)  # rápida; levanta erro se perdeu conexão
            except Exception as meta_exc:
                raise KafkaException(meta_exc)
            last_topics_check = now

        msg = c.poll(1.0)

        if msg is None:
            if now - last_idle_log > 30:
                state = "assigned" if _assigned else "no-assignment"
                stats_state = _last_stats_group_state
                _log(f"[kafka] idle ({state}) | stats.state={stats_state} asn={_last_stats_assignment_size}")
                last_idle_log = now
            continue

        if msg.error():
            code = msg.error().code()
            # erros que justificam recriar consumer
            if code in (
                KafkaError._ALL_BROKERS_DOWN,
                KafkaError._AUTHENTICATION,
                KafkaError._TRANSPORT,
                KafkaError._FATAL,
            ):
                raise KafkaException(msg.error())
            if code == KafkaError._PARTITION_EOF:
                # fim da partição — normal
                continue

            _err(f"[kafka] erro de mensagem: {msg.error()}")
            # não derruba o loop por erro pontual
            continue

        handle(c, msg)


def loop_listener_kafka():
    _log("[worker] Iniciando loop_listener_kafka()")
    backoff = 1.0  # segundos (exponencial até um limite)
    max_backoff = 60.0

    while running:
        c: Optional[Consumer] = None
        try:
            c = get_consumer()
            _log("[kafka] poll loop iniciado")
            _poll_loop(c)
            # saiu do loop normalmente (provável sinal de parada)
            _log("[kafka] poll loop finalizado")
            break

        except KafkaException as ke:
            _err(f"[kafka] KafkaException: {ke} (recriando)")  # <-- AJUSTADO
        except RuntimeError as re:
            _err(f"[watchdog] {re} (recriando consumer)")       # <-- AJUSTADO
        except Exception as e:
            _err(f"[kafka] exceção no loop: {e} (recriando)")   # <-- AJUSTADO
        finally:
            if c is not None:
                try:
                    c.close()
                    _log("[kafka] consumer fechado")
                except Exception:
                    pass

        if not running:
            break

        # Recriar o consumer após falha, com backoff exponencial
        _log(f"[kafka] tentando reconectar em {backoff:.1f}s ...")
        time.sleep(backoff)
        backoff = min(backoff * 2, max_backoff)

    _log("[worker] loop_listener_kafka encerrado")
