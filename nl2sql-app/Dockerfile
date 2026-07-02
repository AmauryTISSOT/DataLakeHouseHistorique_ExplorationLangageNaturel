# Image de l'application Streamlit NL->SQL (banc d'essai text-to-SQL).
# Se connecte a la base GOLD (service `postgres`) et a Ollama (service `ollama`)
# sur le reseau Docker Compose.
FROM python:3.12-slim

# Confort d'exec Python en conteneur : pas de .pyc, logs non bufferises (voir
# les logs applicatifs en direct via `docker compose logs`).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

# 1) Dependances d'abord (couche cache Docker : re-build rapide si seul le code change).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) Code de l'application (app.py, llm_client.py, sql_guard.py, render.py, db/).
COPY . .

# Execution sous un utilisateur non-root.
RUN useradd --create-home appuser
USER appuser

EXPOSE 8501

# Sonde de sante : endpoint interne de Streamlit.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')" || exit 1

CMD ["streamlit", "run", "app.py", \
     "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
