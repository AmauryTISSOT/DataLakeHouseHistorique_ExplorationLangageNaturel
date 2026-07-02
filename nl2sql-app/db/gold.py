"""
Connexion a la base GOLD (PostgreSQL) servie par la couche Gold du lakehouse.

Remplace l'ancienne base mockee SQLite in-memory : l'application n'engendre plus
aucune donnee, elle interroge la base reelle produite par les DAGs Airflow.

Garde-fous lecture seule (defense en profondeur, cote moteur) :
  - `readonly=True` sur la session -> le serveur refuse toute ecriture, meme si
    la validation applicative (`sql_guard`) etait contournee. C'est l'equivalent
    Postgres du `PRAGMA query_only` de l'ancienne base SQLite.
  - `autocommit=True` -> chaque SELECT est sa propre transaction ; une requete en
    erreur ne laisse pas la connexion dans un etat "transaction avortee".
  - `search_path = gold` -> les tables du schema `gold` sont adressables sans
    prefixe (le LLM ecrit `fait_match`, pas `gold.fait_match`).
  - `statement_timeout` -> borne une requete generee trop couteuse.

Configuration par variables d'environnement (defauts = docker-compose local) :
    GOLD_HOST, GOLD_PORT, GOLD_DB, GOLD_USER, GOLD_PASSWORD, GOLD_SCHEMA,
    GOLD_STATEMENT_TIMEOUT_MS

Testable en isolation, sans LLM ni Streamlit :
    python nl2sql-app/db/gold.py
"""
from __future__ import annotations

import logging
import os

import psycopg2

log = logging.getLogger("nl2sql.gold")

# Defauts alignes sur le service `postgres` du docker-compose (base `gold`).
DEFAULT_HOST = os.environ.get("GOLD_HOST", "localhost")
DEFAULT_PORT = int(os.environ.get("GOLD_PORT", "5432"))
DEFAULT_DB = os.environ.get("GOLD_DB", "gold")
DEFAULT_USER = os.environ.get("GOLD_USER", "app")
DEFAULT_PASSWORD = os.environ.get("GOLD_PASSWORD", "app12345")
DEFAULT_SCHEMA = os.environ.get("GOLD_SCHEMA", "gold")
DEFAULT_TIMEOUT_MS = int(os.environ.get("GOLD_STATEMENT_TIMEOUT_MS", "15000"))


def connect(
    *,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    dbname: str = DEFAULT_DB,
    user: str = DEFAULT_USER,
    password: str = DEFAULT_PASSWORD,
    schema: str = DEFAULT_SCHEMA,
    statement_timeout_ms: int = DEFAULT_TIMEOUT_MS,
):
    """Ouvre une connexion PostgreSQL en LECTURE SEULE, prete a l'emploi.

    La connexion rendue ne peut rien ecrire (session read-only cote serveur) ;
    c'est ce que consomme l'application (via `st.cache_resource`).
    """
    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )
    # readonly = backstop moteur ; autocommit = pas de transaction avortee sur erreur.
    conn.set_session(readonly=True, autocommit=True)
    with conn.cursor() as cur:
        cur.execute("SET search_path TO %s", (schema,))
        cur.execute("SET statement_timeout TO %s", (statement_timeout_ms,))
    log.info("Connexion GOLD ouverte (%s:%s/%s, schema=%s, read-only)",
             host, port, dbname, schema)
    return conn


def get_team_names(conn) -> list[str]:
    """Libelles reels des pays (dim_equipe.pays, en anglais), pour ancrer le LLM."""
    with conn.cursor() as cur:
        cur.execute("SELECT pays FROM dim_equipe WHERE pays IS NOT NULL ORDER BY pays")
        return [r[0] for r in cur.fetchall()]


# --- Controle d'isolement (execute seul, sans LLM ni Streamlit) --------------

if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(levelname)s %(name)s %(message)s")
    conn = connect()

    tables = ("dim_edition", "dim_equipe", "fait_match", "fait_but",
              "mart_classement_buteurs", "mart_stats_edition")
    for t in tables:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"  {t:26} : {cur.fetchone()[0]} lignes")

    noms = get_team_names(conn)
    print(f"\n  dim_equipe.pays : {len(noms)} equipes (ex. {', '.join(noms[:6])} ...)")

    # Verite terrain sur la dimension a roles (double role agrege).
    for equipe, annee, attendu in (("France", 2018, 14), ("Argentina", 2022, 15)):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT SUM(buts) FROM (
                    SELECT score_domicile AS buts FROM fait_match m
                    JOIN dim_equipe e ON e.id_equipe = m.id_equipe_domicile
                    WHERE e.pays = %s AND m.annee = %s
                    UNION ALL
                    SELECT score_exterieur FROM fait_match m
                    JOIN dim_equipe e ON e.id_equipe = m.id_equipe_exterieur
                    WHERE e.pays = %s AND m.annee = %s
                ) t
                """,
                (equipe, annee, equipe, annee),
            )
            total = cur.fetchone()[0]
        etat = "OK" if total == attendu else f"!= attendu ({attendu})"
        print(f"  buts {equipe} {annee} : {total}  -> {etat}")

    # Backstop read-only : toute ecriture doit etre rejetee par le serveur.
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE x (a int)")
        print("read-only backstop -> ECHEC (ecriture acceptee !)")
    except psycopg2.Error as e:
        print(f"read-only backstop -> OK (ecriture rejetee : {str(e).strip()})")

    print("\nControles termines.")
