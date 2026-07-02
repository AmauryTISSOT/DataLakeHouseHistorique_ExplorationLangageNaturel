"""
Banc d'essai Streamlit : question en langage naturel -> SQL -> visualisation.

Chaine complete :
    question --(llm_client)--> SQL
             --(sql_guard)---> validation lecture seule
             --(SQLite)------> execution sur la base mockee (read-only)
             --(render)------> tableau + graphique

Lancement :
    streamlit run nl2sql-app/app.py
    (necessite Ollama joignable ; cf. OLLAMA_BASE_URL / OLLAMA_MODEL)
"""
from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

# Rend les imports freres robustes quel que soit le cwd de lancement.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
import streamlit as st

import render as render_mod
import sql_guard
from db import gold
from llm_client import SCHEMA_PATH, LLMError, generate_sql

# Logs pilotables par env : NL2SQL_LOG_LEVEL=DEBUG pour le detail (raisonnement
# du modele, JSON Ollama brut), WARNING pour rendre le terminal silencieux.
# Les logs sortent dans la console ou tourne `streamlit run`.
logging.basicConfig(
    level=os.environ.get("NL2SQL_LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-7s | %(name)-12s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("nl2sql.app")

st.set_page_config(page_title="NL -> SQL Coupe du Monde", page_icon="⚽", layout="wide")


@st.cache_resource
def get_connection():
    """Connexion UNIQUE a la base GOLD (PostgreSQL), read-only, gardee en vie
    a travers les reruns Streamlit."""
    return gold.connect()


@st.cache_data
def load_team_names() -> list[str]:
    """Libelles reels des pays (dim_equipe.pays, anglais), injectes dans le prompt
    pour ancrer le modele : il traduit lui-meme 'Espagne' -> 'Spain'."""
    return gold.get_team_names(get_connection())


conn = get_connection()
team_names = load_team_names()

st.title("⚽ Exploration en langage naturel — Coupe du Monde")
st.caption(
    "question → LLM → SQL → validation lecture seule → exécution → visualisation. "
    "Base GOLD (PostgreSQL), Coupe du Monde 1930–2022."
)

with st.sidebar:
    st.subheader("Schéma injecté dans le prompt")
    st.code(SCHEMA_PATH.read_text(encoding="utf-8"), language="sql")
    st.caption("Ce DDL décrit la base GOLD réelle (PostgreSQL) et sert de source de vérité au prompt.")

question = st.text_input(
    "Votre question",
    placeholder="Ex. Combien de buts l'Argentine a-t-elle marqués en 2022 ?",
)
lancer = st.button("Générer et exécuter", type="primary")

if lancer and not question.strip():
    st.warning("Saisissez une question.")
    st.stop()

if lancer:
    log.info("Question reçue : %r", question.strip())

    # 1) Generation du SQL par le LLM.
    with st.spinner("Génération du SQL par le LLM…"):
        try:
            t0 = time.perf_counter()
            gen = generate_sql(question, team_names=team_names)
            log.info("SQL généré en %.1fs", time.perf_counter() - t0)
        except LLMError as e:
            log.warning("Échec LLM après %.1fs : %s", time.perf_counter() - t0, e)
            st.error(f"LLM indisponible : {e}")
            st.stop()

    log.debug("Raisonnement du modèle : %s", gen.thinking or "(vide)")
    log.info("SQL généré : %s", gen.sql.replace("\n", " "))

    with st.expander("Raisonnement du modèle (think)"):
        st.write(gen.thinking or "_(vide)_")

    st.subheader("SQL généré")
    st.code(gen.sql, language="sql")

    # 2) Validation applicative (garde-fou lecture seule).
    valide, raison = sql_guard.is_read_only_sql(gen.sql)
    if valide:
        log.info("Validation : OK")
    else:
        log.warning("Validation : REFUSÉ (%s)", raison)
        st.error(f"🛑 Requête refusée par le garde-fou : {raison}")
        st.stop()
    st.success("✅ Validation OK — lecture seule, une seule instruction, tables autorisées.")

    # 3) Execution sur la base GOLD (session read-only cote serveur).
    try:
        with conn.cursor() as cur:
            cur.execute(gen.sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            df = pd.DataFrame(cur.fetchall(), columns=cols)
        log.info("Exécution : %d lignes × %d colonnes", len(df), len(df.columns))
    except Exception as e:  # erreur SQL affichee, pas de retry (decision 5c)
        log.warning("Erreur d'exécution SQL : %s", e)
        st.error(f"Erreur d'exécution SQL : {e}")
        st.stop()

    # 4) Rendu selon la forme du resultat (heuristique deterministe).
    st.subheader("Résultat")
    spec = render_mod.choose_rendering(df)
    log.info("Rendu choisi : %s (%s)", spec.kind, spec.reason)
    if spec.kind == "metric":
        st.metric(label=df.columns[0], value=spec.value)
    elif spec.kind == "bar":
        st.bar_chart(df.set_index(spec.x)[spec.y])
    elif spec.kind == "line":
        st.line_chart(df.set_index(spec.x)[spec.y])

    st.dataframe(df, use_container_width=True)
    st.caption(f"Rendu : **{spec.kind}** — {spec.reason}")
