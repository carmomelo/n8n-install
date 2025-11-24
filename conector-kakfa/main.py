# app_kafka_n8n/main.py
import signal
import sys
import time

from .kafka_worker import loop_listener_kafka
from .http_server import start_http_server

def _handle_stop(signum, frame):
    sys.stderr.write(f"[signal] Recebido {signum}. Encerrando...\n")

def main():
    try:
        signal.signal(signal.SIGTERM, _handle_stop)
        signal.signal(signal.SIGINT, _handle_stop)
    except Exception:
        pass

    # inicia HTTP p/ health-check (thread daemon)
    start_http_server()

    # roda o worker Kafka no thread principal
    sys.stderr.write("[worker] Iniciando loop_listener_kafka()\n")
    loop_listener_kafka()

    time.sleep(1)  # dreno rápido de logs

if __name__ == "__main__":
    main()
