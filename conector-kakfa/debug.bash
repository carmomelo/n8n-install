python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
dos2unix .env
# carrega .env (Linux/wsl)
export $(grep -v '^#' .env | xargs) 
python -m app_kafka_n8n.main

