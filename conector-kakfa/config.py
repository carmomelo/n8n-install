import os

def _env_bool(name: str, default: bool = True) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() not in ("false", "0", "no", "off")

# Kafka
KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"].strip()
KAFKA_USERNAME  = os.environ["KAFKA_USERNAME"].strip()
KAFKA_PASSWORD  = os.environ["KAFKA_PASSWORD"].strip()
KAFKA_TOPIC     = os.environ["KAFKA_TOPIC"].strip()
KAFKA_GROUP_ID  = os.environ.get("KAFKA_GROUP_ID", "py-consumer").strip()
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL").strip().upper()
KAFKA_SASL_MECHANISM    = os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN").strip().upper()
KAFKA_AUTO_OFFSET_RESET = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "latest").strip().lower()

KAFKA_TOPIC_RETORNO = os.environ.get("KAFKA_TOPIC_RETORNO", "").strip()

# Webhook (n8n)
WEBHOOK_URL   = os.environ.get("WEBHOOK_URL", "").strip()
WH_TIMEOUT    = float(os.environ.get("WEBHOOK_TIMEOUT_SECS", "10"))
WH_RETRIES    = int(os.environ.get("WEBHOOK_RETRIES", "5"))
WH_BACKOFF    = float(os.environ.get("WEBHOOK_BACKOFF_SECS", "1.5"))
WH_VERIFY_SSL = _env_bool("WEBHOOK_VERIFY_SSL", default=False)
WH_AUTH_HDR   = os.environ.get("WEBHOOK_AUTH_HEADER")  # ex: "Authorization: Bearer xxx" se passar o harder pronto no futuro
WEBHOOK_BASIC_USER = os.environ.get("WEBHOOK_BASIC_USER", "").strip()
WEBHOOK_BASIC_PASS = os.environ.get("WEBHOOK_BASIC_PASS", "").strip()

RETORNO_MESSAGE = os.environ.get(
    "RETORNO_MESSAGE",
    "Tente novamente mais tarde, estamos com indisponibilidade de atendimento neste momento!"
).strip()

# Databricks SQL (failure log)
DB_SQL_URL    = os.environ.get("DATABRICKS_SQL_URL", "").strip()
DB_WAREHOUSE  = os.environ.get("DATABRICKS_WAREHOUSE_ID", "").strip()
DB_TOKEN      = os.environ.get("DATABRICKS_TOKEN", "").strip()
DB_CATALOG    = os.environ.get("DATABRICKS_CATALOG", "lcb_qas_raw_us_bigdata").strip()
DB_SCHEMA     = os.environ.get("DATABRICKS_SCHEMA", "n8n_comercial").strip()
DB_TABLE      = os.environ.get("DATABRICKS_TABLE", "tb_mensagem_stella_falha_conector").strip()
DB_WAIT_TO    = os.environ.get("DATABRICKS_WAIT_TIMEOUT", "20s").strip()
DB_VERIFY_SSL = _env_bool("DATABRICKS_VERIFY_SSL", default=True)


def consumer_conf():
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "security.protocol": KAFKA_SECURITY_PROTOCOL,
        "sasl.mechanisms": KAFKA_SASL_MECHANISM,
        "sasl.username": KAFKA_USERNAME,
        "sasl.password": KAFKA_PASSWORD,
        "group.id": KAFKA_GROUP_ID,
        # Garantir controle explícito do offset:
        "enable.auto.commit": False,
        # IMPORTANTE: latest para não reprocessar histórico
        "auto.offset.reset": "latest",
        "session.timeout.ms": 60000,
        "max.poll.interval.ms": 900000,
        "socket.keepalive.enable": True,
        "reconnect.backoff.ms": 500,
        "reconnect.backoff.max.ms": 60000,
        # "debug": "cgrp,broker,protocol,topic,fetch",
    }

def consumer_conf():
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "security.protocol": KAFKA_SECURITY_PROTOCOL,
        "sasl.mechanisms": KAFKA_SASL_MECHANISM,
        "sasl.username": KAFKA_USERNAME,
        "sasl.password": KAFKA_PASSWORD,
        "group.id": KAFKA_GROUP_ID,
        "enable.auto.commit": True,
        "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
        "session.timeout.ms": 45000,
        "max.poll.interval.ms": 300000,
        "socket.keepalive.enable": True,
        "reconnect.backoff.ms": 500,
        "reconnect.backoff.max.ms": 60000,
    }

def producer_conf():
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "security.protocol": KAFKA_SECURITY_PROTOCOL,
        "sasl.mechanisms": KAFKA_SASL_MECHANISM,
        "sasl.username": KAFKA_USERNAME,
        "sasl.password": KAFKA_PASSWORD,
        "compression.type": "lz4",
        "linger.ms": 10,
        "enable.idempotence": True,
        "acks": "all",
    }
