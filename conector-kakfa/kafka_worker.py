import json, sys, time, signal
from typing import Any, Dict, Optional
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from . import config
from .integrations import post_webhook_with_retry, custom_failure_log

# ---------- Helpers ----------
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

def _headers_to_dict(kheaders):
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

# ---------- Kafka clients ----------
_producer: Optional[Producer] = None

def get_consumer() -> Consumer:
    c = Consumer(config.consumer_conf())
    c.subscribe([config.KAFKA_TOPIC])
    return c

def get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer(config.producer_conf())
    return _producer

def send_fallback_message(
    retorno_topic: str,
    key: Optional[bytes],
    correlation_id: Optional[str],
    session_id: Optional[str],
    retorno_text: str
) -> None:
    prod = get_producer()
    headers = []
    if correlation_id:
        headers.append(("correlation_id", correlation_id.encode("utf-8")))
    if session_id:
        headers.append(("session_id", session_id.encode("utf-8")))
    headers.append(("content_type", b"application/json"))

    value = json.dumps({
        "reply": retorno_text,
        "unavailable": True,
        "timestamp": int(time.time() * 1000)
    }, ensure_ascii=False).encode("utf-8")

    def _delivery(err, msg):
        if err is not None:
            sys.stderr.write(f"[Producer] Falha ao enviar fallback: {err}\n")

    prod.produce(
        topic=retorno_topic,
        key=key,
        value=value,
        headers=headers,
        on_delivery=_delivery
    )
    prod.flush(10)

# ---------- Core ----------
running = True
def _stop(*_):
    global running
    running = False

signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)

def handle(consumer: Consumer, msg) -> None:
    value_obj = _decode_value(msg.value())
    headers = _headers_to_dict(msg.headers() or [])
    key = msg.key()  # bytes ou None
    key_str = key.decode("utf-8") if isinstance(key, (bytes, bytearray)) else key

    correlation_id = headers.get("correlation_id")
    session_id_hdr = headers.get("session_id")

    payload = {
        "kafka": {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp": msg.timestamp()[1] if msg.timestamp() else None,
            "key": key_str,
            "headers": headers,
        },
        "data": value_obj
    }

    try:
        status = post_webhook_with_retry(payload)
        sys.stdout.write(f"[Webhook] sucesso HTTP {status}\n")
    except Exception as e:
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
        except Exception as log_exc:
            sys.stderr.write(f"[FailureLog] erro ao logar falha: {log_exc}\n")

        if config.KAFKA_TOPIC_RETORNO:
            try:
                send_fallback_message(
                    retorno_topic=config.KAFKA_TOPIC_RETORNO,
                    key=msg.key(),
                    correlation_id=correlation_id,
                    session_id=session_id_hdr,
                    retorno_text=config.RETORNO_MESSAGE
                )
                sys.stdout.write("[Fallback] publicado no TOPIC_RETORNO\n")
            except Exception as prod_exc:
                sys.stderr.write(f"[Fallback] erro ao publicar retorno: {prod_exc}\n")
        else:
            sys.stderr.write("[Fallback] TOPIC_RETORNO não configurado; pulando envio\n")
    finally:
        try:
            consumer.commit(message=msg, asynchronous=False)
        except Exception as commit_exc:
            sys.stderr.write(f"[Commit] erro ao commitar offset: {commit_exc}\n")

def loop_listener_kafka():
    c = get_consumer()
    try:
        while running:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            handle(c, msg)
    finally:
        try:
            c.close()
        except:
            pass
