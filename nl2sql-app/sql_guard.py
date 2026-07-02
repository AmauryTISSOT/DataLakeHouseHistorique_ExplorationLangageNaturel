"""
Validation applicative des requetes SQL generees par le LLM (garde-fou lecture seule).

Strategie = LISTE BLANCHE (et non liste noire de mots-cles) :
  1. une seule instruction SQL (rejet de "SELECT ...; DROP ...") ;
  2. l'instruction doit etre une REQUETE DE LECTURE (SELECT / WITH ... SELECT /
     UNION / INTERSECT / EXCEPT) ; tout le reste (INSERT, UPDATE, DELETE, DROP,
     CREATE, ALTER, PRAGMA, ATTACH, ...) est refuse par defaut ;
  3. seules les tables connues du schema en etoile sont referencees.

Parsing par AST (sqlglot, dialecte sqlite) : robuste aux commentaires, a la casse
et aux mots-cles caches dans des chaines, contrairement a une regex.

C'est la 1re ligne de defense. Le backstop moteur `PRAGMA query_only = ON`
(cf. db/seed.py) reste le dernier rempart, meme si cette validation etait contournee.

Testable en isolation, sans LLM ni Streamlit :
    python nl2sql-app/sql_guard.py
"""
from __future__ import annotations

import sqlglot
from sqlglot import exp

DIALECT = "sqlite"

# Tables autorisees (le schema en etoile). Comparaison insensible a la casse.
ALLOWED_TABLES = {"FAIT_MATCH", "DIM_EQUIPE", "DIM_EDITION"}

# Types de noeuds interdits n'importe ou dans l'arbre (defense en profondeur,
# en complement de la liste blanche sur le type de l'instruction).
_FORBIDDEN_NODES = (
    exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create,
    exp.Alter, exp.Command, exp.Pragma, exp.Attach,
)


class SQLValidationError(ValueError):
    """Levee quand une requete ne respecte pas les regles de lecture seule."""


def validate_read_only_sql(sql: str) -> str:
    """Valide `sql`. Renvoie la requete (strippee) si acceptee, sinon leve SQLValidationError."""
    if not sql or not sql.strip():
        raise SQLValidationError("Requete vide.")

    # Regle 1 : une seule instruction.
    try:
        statements = [s for s in sqlglot.parse(sql, read=DIALECT) if s is not None]
    except sqlglot.errors.ParseError as e:
        raise SQLValidationError(f"SQL non analysable : {e}") from e
    if not statements:
        raise SQLValidationError("Aucune instruction SQL detectee.")
    if len(statements) > 1:
        raise SQLValidationError(
            f"Une seule instruction autorisee ({len(statements)} detectees)."
        )

    stmt = statements[0]

    # Regle 2 : ce doit etre une requete de LECTURE (SELECT/WITH/UNION/...).
    if not isinstance(stmt, exp.Query):
        raise SQLValidationError(
            f"Seules les requetes de lecture sont autorisees "
            f"(recu : {type(stmt).__name__.upper()})."
        )
    # Defense en profondeur : aucun noeud d'ecriture / administration, meme imbrique.
    forbidden = next(stmt.find_all(*_FORBIDDEN_NODES), None)
    if forbidden is not None:
        raise SQLValidationError(
            f"Operation interdite detectee : {type(forbidden).__name__.upper()}."
        )

    # Regle 3 : seules les tables connues (+ les CTE locales) sont referencees.
    cte_names = {c.alias.upper() for c in stmt.find_all(exp.CTE) if c.alias}
    allowed = {t.upper() for t in ALLOWED_TABLES} | cte_names
    for table in stmt.find_all(exp.Table):
        if table.name.upper() not in allowed:
            raise SQLValidationError(f"Table non autorisee : {table.name!r}.")

    return sql.strip()


def is_read_only_sql(sql: str) -> tuple[bool, str]:
    """Variante non levante : renvoie (True, '') si accepte, sinon (False, raison)."""
    try:
        validate_read_only_sql(sql)
        return True, ""
    except SQLValidationError as e:
        return False, str(e)


# --- Batterie de tests (executee seule) --------------------------------------

if __name__ == "__main__":
    # Le patron "dur" (double role) DOIT passer.
    ROLE_PLAYING = """
        SELECT SUM(buts) FROM (
            SELECT score_domicile AS buts FROM FAIT_MATCH m
            JOIN DIM_EQUIPE e ON e.equipe_id = m.equipe_domicile_id
            WHERE e.nom_equipe = 'Argentina'
            UNION ALL
            SELECT score_exterieur AS buts FROM FAIT_MATCH m
            JOIN DIM_EQUIPE e ON e.equipe_id = m.equipe_exterieur_id
            WHERE e.nom_equipe = 'Argentina'
        )
    """

    ACCEPTES = [
        ("select simple", "SELECT nom_equipe FROM DIM_EQUIPE"),
        ("union haut niveau", "SELECT 1 FROM FAIT_MATCH UNION ALL SELECT 2 FROM FAIT_MATCH"),
        ("CTE", "WITH t AS (SELECT * FROM FAIT_MATCH) SELECT COUNT(*) FROM t"),
        ("casse + commentaire", "/* ok */ select * from fait_match"),
        ("double role", ROLE_PLAYING),
    ]
    REFUSES = [
        ("injection multi", "SELECT 1 FROM DIM_EQUIPE; DROP TABLE DIM_EQUIPE"),
        ("drop", "DROP TABLE FAIT_MATCH"),
        ("delete", "DELETE FROM FAIT_MATCH"),
        ("update", "UPDATE DIM_EQUIPE SET nom_equipe = 'x'"),
        ("insert", "INSERT INTO DIM_EQUIPE (equipe_id, nom_equipe) VALUES (1, 'x')"),
        ("pragma", "PRAGMA query_only = OFF"),
        ("attach", "ATTACH DATABASE 'evil.db' AS e"),
        ("table inconnue", "SELECT * FROM sqlite_master"),
        ("vide", "   "),
    ]

    ok = True
    for label, sql in ACCEPTES:
        valide, raison = is_read_only_sql(sql)
        etat = "OK" if valide else f"ECHEC ({raison})"
        ok &= valide
        print(f"  [accepter] {label:22} -> {etat}")
    for label, sql in REFUSES:
        valide, raison = is_read_only_sql(sql)
        etat = f"OK (refuse : {raison})" if not valide else "ECHEC (accepte a tort !)"
        ok &= not valide
        print(f"  [refuser ] {label:22} -> {etat}")

    print("\nTous les controles sont passes." if ok else "\nDES CONTROLES ONT ECHOUE.")
    raise SystemExit(0 if ok else 1)
