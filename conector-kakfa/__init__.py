# app_kafka_n8n/__init__.py

"""Pacote do consumidor Kafka + integrações n8n/Databricks."""

__version__ = "0.1.0"

# Exporte atalho para o loop principal
from .kafka_worker import loop_listener_kafka as run, loop_listener_kafka  

__all__ = ["loop_listener_kafka", "run"]
