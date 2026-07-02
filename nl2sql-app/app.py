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

import sys
from pathlib import Path

# Rend les imports freres robustes quel que soit le cwd de lancement.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
import streamlit as st

import render as render_mod
import sql_guard
from db.seed import EDITIONS, build_database
from llm_client import SCHEMA_PATH, LLMError, generate_sql

st.set_page_config(page_title="NL -> SQL Coupe du Monde", page_icon="⚽", layout="wide")


@st.cache_resource
def get_connection():
    """Connexion UNIQUE, semee et read-only, gardee en vie a travers les reruns."""
    return build_database(read_only=True)


conn = get_connection()

st.title("⚽ Exploration en langage naturel — Coupe du Monde")
st.caption(
    "question → LLM → SQL → validation lecture seule → exécution → visualisation. "
    f"Base mockée SQLite (éditions {' + '.join(EDITIONS)})."
)

with st.sidebar:
    st.subheader("Schéma injecté dans le prompt")
    st.code(SCHEMA_PATH.read_text(encoding="utf-8"), language="sql")
    st.caption("Ce DDL est la source de vérité : il alimente à la fois la base et le prompt.")

question = st.text_input(
    "Votre question",
    placeholder="Ex. Combien de buts l'Argentine a-t-elle marqués en 2022 ?",
)
lancer = st.button("Générer et exécuter", type="primary")

if lancer and not question.strip():
    st.warning("Saisissez une question.")
    st.stop()

if lancer:
    # 1) Generation du SQL par le LLM.
    with st.spinner("Génération du SQL par le LLM…"):
        try:
            gen = generate_sql(question)
        except LLMError as e:
            st.error(f"LLM indisponible : {e}")
            st.stop()

    with st.expander("Raisonnement du modèle (think)"):
        st.write(gen.thinking or "_(vide)_")

    st.subheader("SQL généré")
    st.code(gen.sql, language="sql")

    # 2) Validation applicative (garde-fou lecture seule).
    valide, raison = sql_guard.is_read_only_sql(gen.sql)
    if not valide:
        st.error(f"🛑 Requête refusée par le garde-fou : {raison}")
        st.stop()
    st.success("✅ Validation OK — lecture seule, une seule instruction, tables autorisées.")

    # 3) Execution sur la base mockee (connexion read-only).
    try:
        cur = conn.execute(gen.sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        df = pd.DataFrame(cur.fetchall(), columns=cols)
    except Exception as e:  # erreur SQL affichee, pas de retry (decision 5c)
        st.error(f"Erreur d'exécution SQL : {e}")
        st.stop()

    # 4) Rendu selon la forme du resultat (heuristique deterministe).
    st.subheader("Résultat")
    spec = render_mod.choose_rendering(df)
    if spec.kind == "metric":
        st.metric(label=df.columns[0], value=spec.value)
    elif spec.kind == "bar":
        st.bar_chart(df.set_index(spec.x)[spec.y])
    elif spec.kind == "line":
        st.line_chart(df.set_index(spec.x)[spec.y])

    st.dataframe(df, use_container_width=True)
    st.caption(f"Rendu : **{spec.kind}** — {spec.reason}")
