"""
Construction de la base mockee SQLite in-memory pour le banc d'essai text-to-SQL.

Contenu : mini schema en etoile (FAIT_MATCH + DIM_EQUIPE + DIM_EDITION),
peuple depuis les editions 2018 + 2022 du CSV Kaggle (`matches_1930_2022.csv`).

Points de conception (voir la synthese du projet) :
  - Connexion UNIQUE, in-memory, `check_same_thread=False` (compatible Streamlit
    `@st.cache_resource` : seed -> query_only -> reutilisee partout).
  - Backstop moteur : `PRAGMA query_only = ON` pose APRES le seed -> plus aucune
    ecriture possible, independamment de la validation applicative.
  - `PRAGMA foreign_keys = ON` pendant le seed : les REFERENCES (dont le double
    role vers DIM_EQUIPE) sont reellement verifiees.

Testable en isolation, sans LLM ni Streamlit :
    python nl2sql-app/db/seed.py
"""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

# Editions retenues pour le banc d'essai.
# >= 2 editions -> permet de tester le filtre sur DIM_EDITION ("en 2018" vs "en 2022").
EDITIONS = ("2018", "2022")

_DB_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _DB_DIR.parents[1]
SCHEMA_PATH = _DB_DIR / "schema.sql"
CSV_PATH = _REPO_ROOT / "Data" / "raw" / "kaggle" / "matches_1930_2022.csv"


def _load_rows() -> list[dict]:
    """Lignes du CSV limitees aux editions retenues (colonnes utiles seulement)."""
    with CSV_PATH.open(encoding="utf-8") as f:
        return [r for r in csv.DictReader(f) if r["Year"] in EDITIONS]


def _seed(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """Peuple les 3 tables. Ordre : dimensions (parents) avant le fait (enfant)."""
    # DIM_EDITION : une ligne par annee (avec son pays hote).
    hote_par_annee: dict[str, str] = {}
    for r in rows:
        hote_par_annee.setdefault(r["Year"], r["Host"])
    edition_id = {annee: i for i, annee in enumerate(sorted(hote_par_annee), start=1)}
    conn.executemany(
        "INSERT INTO DIM_EDITION (edition_id, annee, pays_hote) VALUES (?, ?, ?)",
        [(edition_id[a], int(a), hote_par_annee[a]) for a in sorted(hote_par_annee)],
    )

    # DIM_EQUIPE : une ligne par equipe, tous roles confondus.
    noms = sorted({r["home_team"] for r in rows} | {r["away_team"] for r in rows})
    equipe_id = {nom: i for i, nom in enumerate(noms, start=1)}
    conn.executemany(
        "INSERT INTO DIM_EQUIPE (equipe_id, nom_equipe) VALUES (?, ?)",
        [(equipe_id[nom], nom) for nom in noms],
    )

    # FAIT_MATCH : une ligne par match ; double role vers DIM_EQUIPE.
    conn.executemany(
        """INSERT INTO FAIT_MATCH
               (match_id, edition_id, equipe_domicile_id, equipe_exterieur_id,
                score_domicile, score_exterieur, phase, date_match, stade)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (
                mid,
                edition_id[r["Year"]],
                equipe_id[r["home_team"]],
                equipe_id[r["away_team"]],
                int(r["home_score"]),
                int(r["away_score"]),
                r["Round"],
                r["Date"],
                r["Venue"],
            )
            for mid, r in enumerate(rows, start=1)
        ],
    )


def build_database(read_only: bool = True) -> sqlite3.Connection:
    """Cree, peuple et renvoie la connexion SQLite in-memory prete a l'emploi.

    read_only=True pose le backstop `query_only` : la connexion rendue ne peut
    plus rien ecrire. C'est ce que consommera l'application (via cache_resource).
    """
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    _seed(conn, _load_rows())
    conn.commit()
    if read_only:
        conn.execute("PRAGMA query_only = ON")  # backstop : ecritures rejetees par le moteur
    return conn


# --- Auto-test de verite terrain (execute seul, sans LLM ni Streamlit) --------

# Le cas DUR : buts totaux d'une equipe sur une edition.
# C'est LE patron que le LLM doit apprendre -> agregation des DEUX roles.
_SQL_BUTS_EQUIPE_EDITION = """
    SELECT SUM(buts) AS total_buts FROM (
        SELECT m.score_domicile AS buts
        FROM FAIT_MATCH m
        JOIN DIM_EQUIPE  e  ON e.equipe_id   = m.equipe_domicile_id
        JOIN DIM_EDITION ed ON ed.edition_id = m.edition_id
        WHERE e.nom_equipe = ? AND ed.annee = ?
        UNION ALL
        SELECT m.score_exterieur AS buts
        FROM FAIT_MATCH m
        JOIN DIM_EQUIPE  e  ON e.equipe_id   = m.equipe_exterieur_id
        JOIN DIM_EDITION ed ON ed.edition_id = m.edition_id
        WHERE e.nom_equipe = ? AND ed.annee = ?
    )
"""


def _reference_buts(rows: list[dict], equipe: str, annee: str) -> int:
    """Recalcul naif direct depuis le CSV, pour verifier le SQL etoile."""
    total = 0
    for r in rows:
        if r["Year"] != annee:
            continue
        if r["home_team"] == equipe:
            total += int(r["home_score"])
        if r["away_team"] == equipe:
            total += int(r["away_score"])
    return total


if __name__ == "__main__":
    conn = build_database(read_only=True)

    n_ed = conn.execute("SELECT COUNT(*) FROM DIM_EDITION").fetchone()[0]
    n_eq = conn.execute("SELECT COUNT(*) FROM DIM_EQUIPE").fetchone()[0]
    n_ma = conn.execute("SELECT COUNT(*) FROM FAIT_MATCH").fetchone()[0]
    print(f"Tables peuplees : DIM_EDITION={n_ed}  DIM_EQUIPE={n_eq}  FAIT_MATCH={n_ma}")

    # Verite terrain sur la dimension a roles (Argentine 2022, championne).
    rows = _load_rows()
    for equipe, annee in (("Argentina", "2022"), ("France", "2018"), ("Brazil", "2022")):
        star = conn.execute(
            _SQL_BUTS_EQUIPE_EDITION, (equipe, int(annee), equipe, int(annee))
        ).fetchone()[0]
        ref = _reference_buts(rows, equipe, annee)
        etat = "OK" if star == ref else "ECHEC"
        print(f"  buts {equipe} {annee} : etoile={star}  reference CSV={ref}  -> {etat}")
        assert star == ref, "Seed ou schema incorrect : divergence avec le CSV."

    # Backstop query_only : toute ecriture doit etre rejetee par le moteur.
    try:
        conn.execute("INSERT INTO DIM_EQUIPE (equipe_id, nom_equipe) VALUES (9999, 'X')")
        print("query_only backstop -> ECHEC (ecriture acceptee !)")
    except sqlite3.OperationalError as e:
        print(f"query_only backstop -> OK (ecriture rejetee : {e})")

    print("\nTous les controles sont passes.")
