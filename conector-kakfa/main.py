#kafka-conector-n8n/
#├─ kafka_app/
#│  ├─ __init__.py
#│  ├─ config.py
#│  ├─ integrations.py
#│  ├─ kafka_worker.py
#│  └─ main.py
#└─ requirements.txt

from .kafka_worker import loop_listener_kafka

if __name__ == "__main__":
    loop_listener_kafka()
