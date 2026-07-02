"""
Client LLM : traduit une question en langage naturel en une requete SQL SQLite.

Choix de conception (cf. synthese du projet) :
  - API NATIVE Ollama `/api/chat` (pas la couche compat `/v1`).
  - `think: true`  -> le raisonnement de qwen3 revient dans `message.thinking`,
    SEPARE du SQL (aucun regex a faire).
  - `format: {schema JSON}` -> `message.content` est un JSON pur `{"sql": "..."}`.
  - Le schema injecte dans le prompt est le DDL brut (`db/schema.sql`), qui porte
    deja le commentaire semantique sur la dimension a roles.
  - Un seul exemple few-shot cible : le patron "buts d'une equipe sur une edition"
    (agregation des deux roles), le cas qui fait echouer le modele sinon.

Ce module NE valide PAS et N'EXECUTE PAS le SQL : c'est le role de `sql_guard`
puis de la base. Il se contente de generer.

Testable en isolation (necessite Ollama joignable) :
    python nl2sql-app/llm_client.py
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

import requests

log = logging.getLogger("nl2sql.llm")

_HERE = Path(__file__).resolve().parent
SCHEMA_PATH = _HERE / "db" / "schema.sql"

DEFAULT_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
DEFAULT_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3:8b")

# Schema de sortie impose au modele : un objet {"sql": "..."} et rien d'autre.
_RESPONSE_FORMAT = {
    "type": "object",
    "properties": {"sql": {"type": "string"}},
    "required": ["sql"],
}

# Exemple few-shot cible sur la dimension a roles (le point dur).
_FEW_SHOT = """\
Question : Combien de buts la France a-t-elle marques en 2018 ?
SQL :
SELECT SUM(buts) AS total_buts FROM (
    SELECT m.score_domicile AS buts
    FROM FAIT_MATCH m
    JOIN DIM_EQUIPE  e ON e.equipe_id  = m.equipe_domicile_id
    JOIN DIM_EDITION d ON d.edition_id = m.edition_id
    WHERE e.nom_equipe = 'France' AND d.annee = 2018
    UNION ALL
    SELECT m.score_exterieur AS buts
    FROM FAIT_MATCH m
    JOIN DIM_EQUIPE  e ON e.equipe_id  = m.equipe_exterieur_id
    JOIN DIM_EDITION d ON d.edition_id = m.edition_id
    WHERE e.nom_equipe = 'France' AND d.annee = 2018
);"""


class LLMError(RuntimeError):
    """Erreur d'appel ou de reponse du LLM."""


@dataclass
class SQLGeneration:
    """Resultat d'une generation : le SQL propre + le raisonnement (isole)."""
    sql: str
    thinking: str
    raw_content: str


def _load_schema_ddl() -> str:
    return SCHEMA_PATH.read_text(encoding="utf-8")


def build_system_prompt(schema_ddl: str | None = None) -> str:
    """Construit le prompt systeme : regles + schema DDL + exemple few-shot."""
    ddl = schema_ddl if schema_ddl is not None else _load_schema_ddl()
    return f"""\
Tu es un assistant qui traduit une question en une requete SQL pour SQLite.

Regles imperatives :
- Genere UNE SEULE requete SELECT, en lecture seule. Jamais INSERT, UPDATE,
  DELETE, DROP, ALTER, PRAGMA ni ATTACH.
- Utilise UNIQUEMENT les tables et colonnes du schema ci-dessous.
- Dialecte SQLite.
- Reponds STRICTEMENT au format JSON demande : {{"sql": "<la requete>"}}.

Schema de la base (DDL) :
{ddl}

{_FEW_SHOT}"""


def generate_sql(
    question: str,
    *,
    base_url: str = DEFAULT_BASE_URL,
    model: str = DEFAULT_MODEL,
    timeout: float = 180.0,
) -> SQLGeneration:
    """Appelle Ollama et renvoie le SQL genere (+ le raisonnement isole)."""
    if not question or not question.strip():
        raise LLMError("Question vide.")

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": build_system_prompt()},
            {"role": "user", "content": question.strip()},
        ],
        "think": True,
        "format": _RESPONSE_FORMAT,
        "stream": False,
        "options": {"temperature": 0},
    }

    log.debug("POST %s/api/chat (model=%s) question=%r", base_url, model, question.strip())
    try:
        resp = requests.post(f"{base_url}/api/chat", json=payload, timeout=timeout)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        log.warning("Appel Ollama échoué (%s) : %s", base_url, e)
        raise LLMError(f"Appel Ollama echoue ({base_url}) : {e}") from e

    message = resp.json().get("message", {})
    content = message.get("content", "")
    thinking = message.get("thinking", "") or ""
    log.debug("Réponse Ollama brute : content=%r thinking=%r", content, thinking)

    try:
        sql = json.loads(content)["sql"]
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        log.warning("Réponse LLM inexploitable : %r", content)
        raise LLMError(f"Reponse LLM inexploitable : {content!r}") from e

    return SQLGeneration(sql=sql.strip(), thinking=thinking.strip(), raw_content=content)


# --- Smoke test de bout en bout (necessite Ollama) ---------------------------

if __name__ == "__main__":
    import sql_guard
    from db.seed import build_database

    QUESTIONS = [
        ("Combien de buts l'Argentine a-t-elle marques en 2022 ?", 15),
        ("Combien de buts la France a-t-elle marques en 2018 ?", 14),
    ]

    try:
        conn = build_database(read_only=True)
        for question, attendu in QUESTIONS:
            print(f"\nQ : {question}")
            gen = generate_sql(question)
            print(f"  raisonnement (extrait) : {gen.thinking[:120].replace(chr(10), ' ')}...")
            print(f"  SQL genere :\n    " + gen.sql.replace("\n", "\n    "))

            # 1) validation applicative
            valide, raison = sql_guard.is_read_only_sql(gen.sql)
            print(f"  validation : {'OK' if valide else 'REFUSE -> ' + raison}")
            if not valide:
                continue

            # 2) execution sur la base mockee (lecture seule)
            rows = conn.execute(gen.sql).fetchall()
            resultat = rows[0][0] if len(rows) == 1 and len(rows[0]) == 1 else rows
            etat = "OK" if resultat == attendu else f"!= attendu ({attendu})"
            print(f"  resultat : {resultat}  -> {etat}")
    except LLMError as e:
        print(f"[Ollama indisponible] {e}")
        raise SystemExit(0)
