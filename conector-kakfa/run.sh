#!/usr/bin/env bash

# deligar SCM_DO_BUILD_DURING_DEPLOYMENT=0
#lcb_conector_kafka_n8n/
#├─ app_kafka_n8n/
#│  ├─ __init__.py
#│  ├─ config.py
#│  ├─ integrations.py
#│  ├─ kafka_worker.py
#│  └─ main.py
#├─ requirements.txt
#└─ run.sh

set -euo pipefail

APP_DIR="/home/site/wwwroot"
cd "$APP_DIR"
export PYTHONUNBUFFERED=1

# Ajustar permissões do diretório
export TMPDIR=/tmp
mkdir -p /home/logfiles /home/antenv

# ======= OPÇÃO A: VENV PERSISTENTE EM /home/antenv =======
VENV="/home/antenv"
if [ ! -d "$VENV" ]; then
  echo "[run.sh] criando venv em $VENV ..."
  python -m venv "$VENV"
fi
# ativa venv
. "$VENV/bin/activate"

echo "[run.sh] pip install -r requirements.txt (venv persistente)"
pip install --no-cache-dir --upgrade pip
pip install --no-cache-dir -r requirements.txt

# APP no PYTHONPATH
export PYTHONPATH="$APP_DIR:${PYTHONPATH:-}"
echo "[run.sh] PYTHONPATH=$PYTHONPATH"

# Se quiser subir HTTP + worker:
# python -m app_kafka_n8n.http_server &

echo "[run.sh] iniciando worker..."
exec python -m app_kafka_n8n.main
