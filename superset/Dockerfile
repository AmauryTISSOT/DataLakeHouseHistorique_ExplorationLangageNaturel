FROM apache/superset:latest

USER root
# Driver PostgreSQL (absent de l'image de base) installe dans le venv Superset.
RUN uv pip install --python /app/.venv/bin/python psycopg2-binary

# Script d'enregistrement de la base GOLD, joue par le conteneur superset-init.
COPY register_gold.py /app/register_gold.py

USER superset
