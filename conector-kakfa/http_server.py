# app_kafka_n8n/http_server.py
import os
import sys
import threading
from typing import Optional

try:
    from fastapi import FastAPI
    import uvicorn
except Exception as e:
    FastAPI = None
    uvicorn = None
    sys.stderr.write(f"[http] fastapi/uvicorn não disponíveis: {e}\n")

app: Optional["FastAPI"] = FastAPI(title="kafka-n8n-worker") if FastAPI else None

if app:
    @app.get("/")
    def root():
        return {"ok": True}

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

def start_http_server() -> Optional[threading.Thread]:
    """
    Sobe um servidor HTTP em thread daemon para health-check do App Service.
    Retorna a thread criada (ou None se FastAPI/uvicorn não estiverem instalados).

    APP-LCB-COMBOTIA-N8N-<env>01 => Em Configuration => App settings, adicione e no .env:
    WEBSITES_PORT = 8000
    Reinicie. Isso força nosso HTTP a bindar na porta que o App Service espera.
    """
    if not (app and uvicorn):
        sys.stderr.write("[http] FastAPI/uvicorn indisponíveis; health-check pode falhar.\n")
        return None

    #port = int(os.environ.get("WEBSITES_PORT") or os.environ.get("PORT") or "8000")
    if os.environ.get("WEBSITE_SITE_NAME"):
        port = 8000 # forçar a porta
    else:
        port = int(os.environ.get("WEBSITES_PORT") or "8000")

    def _run():
        sys.stderr.write(f"[http] uvicorn em 0.0.0.0:{port}\n")
        uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")

    t = threading.Thread(target=_run, name="http-thread", daemon=True)
    t.start()
    return t
