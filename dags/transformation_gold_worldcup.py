"""DAG de modelisation Gold — Projet 4 (Lakehouse + text-to-SQL).

Lit les tables NETTOYEES de la couche Silver (Parquet dans le bucket ``silver``
de SeaweedFS), construit un SCHEMA EN ETOILE + des tables metier (marts), et
CHARGE le tout dans PostgreSQL (base ``gold``, schema ``gold``).

C'est la couche servie : Superset (dashboards) et l'application text-to-SQL
interrogent ce schema. Des cles primaires et etrangeres sont posees pour
documenter les relations (aide directe au text-to-SQL pour deviner les jointures).

    silver/*.parquet  (SeaweedFS)         gold.*  (PostgreSQL)
    ---------------------------------     ------------------------------------
    editions            ------------->    dim_edition
    matches, goals, fifa_ranking ----->    dim_equipe
    matches             ------------->    dim_stade
    player_stats, goals ------------->    dim_joueur
    matches             ------------->    fait_match   (FK -> dims)
    goals               ------------->    fait_but     (FK -> fait_match, dims)
                                          mart_classement_buteurs
                                          mart_stats_equipe
                                          mart_stats_edition
                                          mart_buts_par_periode

Modele en etoile (cf. enonce, section 6) : les FAIT_* portent les mesures et
referencent les DIM_* par cle etrangere.

Idempotent : la premiere tache fait ``DROP SCHEMA gold CASCADE`` puis le
reconstruit entierement a chaque execution (pattern "rebuild du warehouse").
Le schema ``gold`` est distinct du schema ``public`` ou Airflow stocke ses
metadonnees dans la meme base : aucune collision.
"""

from __future__ import annotations

import io
import os
import re
from datetime import datetime, timedelta

import boto3
import pandas as pd
from airflow.decorators import dag, task
from botocore.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.types import BigInteger, Boolean, Date, Float, Integer, Text

# --- Configuration ----------------------------------------------------------
S3_ENDPOINT = os.getenv("SILVER_S3_ENDPOINT", "http://seaweedfs:8333")
S3_ACCESS_KEY = os.getenv("SILVER_S3_ACCESS_KEY", "minio")
S3_SECRET_KEY = os.getenv("SILVER_S3_SECRET_KEY", "minio12345")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")

# Base servie. Depuis le conteneur Airflow, Postgres est joignable via "postgres".
PG_DSN = os.getenv("GOLD_PG_DSN", "postgresql+psycopg2://app:app12345@postgres/gold")
GOLD_SCHEMA = os.getenv("GOLD_SCHEMA", "gold")


def _s3_client():
    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
    )


def _engine():
    # future=True -> API 2.0 style (engine.begin() as conn: conn.execute(text(...)))
    return create_engine(PG_DSN, future=True)


# ---------------------------------------------------------------------------
# Lecture Silver (Parquet, partition ingest_date la plus recente)
# ---------------------------------------------------------------------------

def _read_silver(table: str) -> pd.DataFrame:
    """Charge silver/<table>/ingest_date=<max>/<table>.parquet en DataFrame."""
    client = _s3_client()
    keys, token = [], None
    prefix = f"{table}/"
    while True:
        kw = {"Bucket": SILVER_BUCKET, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = client.list_objects_v2(**kw)
        keys.extend(o["Key"] for o in resp.get("Contents", []))
        if resp.get("IsTruncated"):
            token = resp["NextContinuationToken"]
        else:
            break
    dated = []
    for k in keys:
        m = re.search(r"ingest_date=(\d{4}-\d{2}-\d{2})/", k)
        if m and k.endswith(".parquet"):
            dated.append((m.group(1), k))
    if not dated:
        raise FileNotFoundError(
            f"Aucun Parquet pour la table Silver '{table}' dans '{SILVER_BUCKET}'. "
            "Le DAG transformation_silver_worldcup a-t-il tourne ?"
        )
    key = max(dated)[1]
    raw = client.get_object(Bucket=SILVER_BUCKET, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(raw))


# ---------------------------------------------------------------------------
# Ecriture Gold (PostgreSQL)
# ---------------------------------------------------------------------------

def _surrogate(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    """Ajoute une cle de substitution entiere 1..n en tete du DataFrame."""
    df = df.reset_index(drop=True).copy()
    df.insert(0, id_col, range(1, len(df) + 1))
    return df


def _load(df: pd.DataFrame, table: str, dtype: dict, pk: str | None = None) -> int:
    """Ecrit la table dans le schema gold (remplacement), pose la PK."""
    eng = _engine()
    df.to_sql(
        table, eng, schema=GOLD_SCHEMA, if_exists="replace",
        index=False, dtype=dtype, method="multi", chunksize=500,
    )
    if pk:
        with eng.begin() as conn:
            conn.execute(text(
                f'ALTER TABLE {GOLD_SCHEMA}."{table}" '
                f'ADD PRIMARY KEY ("{pk}");'
            ))
    return len(df)


def _read_gold(sql: str) -> pd.DataFrame:
    return pd.read_sql(text(sql), _engine())


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="transformation_gold_worldcup",
    description="Modelisation Gold : Silver (Parquet) -> schema en etoile + marts (PostgreSQL)",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["gold", "star-schema", "postgres", "worldcup", "projet4"],
)
def transformation_gold_worldcup():

    @task
    def init_schema() -> str:
        """Reconstruit le schema gold a vide (idempotence)."""
        eng = _engine()
        with eng.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {GOLD_SCHEMA} CASCADE;"))
            conn.execute(text(f"CREATE SCHEMA {GOLD_SCHEMA};"))
        return GOLD_SCHEMA

    # --- Dimensions --------------------------------------------------------

    @task
    def build_dim_edition(schema: str) -> int:
        ed = _read_silver("editions")
        dim = pd.DataFrame({
            "annee": ed["year"].astype("Int64"),
            "pays_hote": ed["host"],
            "nb_equipes": ed["teams"].astype("Int64"),
            "champion": ed["champion"],
            "finaliste": ed["runner_up"],
            "meilleur_buteur": ed["top_scorer"],
            "meilleur_buteur_buts": ed["top_scorer_goals"].astype("Int64"),
            "affluence_totale": ed["attendance_total"].astype("Int64"),
            "affluence_moyenne": ed["attendance_avg"].astype("Int64"),
            "nb_matchs": ed["matches"].astype("Int64"),
        }).sort_values("annee")
        dim = _surrogate(dim, "id_edition")
        return _load(dim, "dim_edition", {
            "id_edition": Integer, "annee": Integer, "pays_hote": Text,
            "nb_equipes": Integer, "champion": Text, "finaliste": Text,
            "meilleur_buteur": Text, "meilleur_buteur_buts": Integer,
            "affluence_totale": BigInteger, "affluence_moyenne": Integer,
            "nb_matchs": Integer,
        }, pk="id_edition")

    @task
    def build_dim_equipe(schema: str) -> int:
        m = _read_silver("matches")
        g = _read_silver("goals")
        ed = _read_silver("editions")
        fifa = _read_silver("fifa_ranking")

        noms = pd.Series(pd.concat([
            m["home_team"], m["away_team"], m["host"],
            g["team"], g["opponent"],
            ed["host"], ed["champion"], ed["runner_up"],
            fifa["team"],
        ], ignore_index=True)).dropna().astype("string")
        noms = noms[noms.str.strip() != ""].drop_duplicates().sort_values()
        dim = pd.DataFrame({"pays": noms.values})

        # Confederation / code depuis le classement FIFA le plus recent par equipe.
        latest = (fifa.sort_values("snapshot_date")
                      .groupby("team", as_index=False).last()
                      [["team", "confederation", "team_code"]])
        dim = dim.merge(latest, left_on="pays", right_on="team", how="left")
        dim = dim.rename(columns={"team_code": "code_pays"})[["pays", "confederation", "code_pays"]]
        dim = _surrogate(dim, "id_equipe")
        return _load(dim, "dim_equipe", {
            "id_equipe": Integer, "pays": Text, "confederation": Text, "code_pays": Text,
        }, pk="id_equipe")

    @task
    def build_dim_stade(schema: str) -> int:
        m = _read_silver("matches")
        st = m[["venue_name", "venue_city", "host"]].copy()
        st = st[st["venue_name"].notna()]
        st["venue_city"] = st["venue_city"].fillna("")
        st = (st.drop_duplicates(subset=["venue_name", "venue_city"])
                .sort_values(["venue_name", "venue_city"]))
        dim = pd.DataFrame({
            "nom_stade": st["venue_name"].values,
            "ville": st["venue_city"].values,
            "pays_hote": st["host"].values,
        })
        dim = _surrogate(dim, "id_stade")
        return _load(dim, "dim_stade", {
            "id_stade": Integer, "nom_stade": Text, "ville": Text, "pays_hote": Text,
        }, pk="id_stade")

    @task
    def build_dim_joueur(schema: str) -> int:
        ps = _read_silver("player_stats")[["player", "pos", "nation"]].copy()
        ps = ps.rename(columns={"player": "nom", "pos": "poste", "nation": "nationalite"})
        ps = ps[ps["nom"].notna()]
        # Un joueur = un nom ; on garde la 1re occurrence (poste/nationalite renseignes).
        ps = ps.drop_duplicates(subset=["nom"])

        g = _read_silver("goals")
        scorers = g["scorer"].dropna().astype("string")
        scorers = scorers[scorers.str.strip() != ""].drop_duplicates()
        # Buteurs absents des stats FBref (editions <2014) : poste/nationalite inconnus.
        manquants = scorers[~scorers.isin(ps["nom"])]
        extra = pd.DataFrame({"nom": manquants.values, "poste": pd.NA, "nationalite": pd.NA})

        dim = pd.concat([ps, extra], ignore_index=True).drop_duplicates(subset=["nom"])
        dim = dim.sort_values("nom").reset_index(drop=True)
        dim = _surrogate(dim, "id_joueur")
        return _load(dim, "dim_joueur", {
            "id_joueur": Integer, "nom": Text, "poste": Text, "nationalite": Text,
        }, pk="id_joueur")

    # --- Faits -------------------------------------------------------------

    @task
    def build_fait_match(schema: str, *_dims) -> int:
        m = _read_silver("matches").copy()
        dim_ed = _read_gold(f"SELECT id_edition, annee FROM {GOLD_SCHEMA}.dim_edition")
        dim_eq = _read_gold(f"SELECT id_equipe, pays FROM {GOLD_SCHEMA}.dim_equipe")
        dim_st = _read_gold(f"SELECT id_stade, nom_stade, ville FROM {GOLD_SCHEMA}.dim_stade")

        m["venue_city"] = m["venue_city"].fillna("")
        f = m.merge(dim_ed, left_on="year", right_on="annee", how="left")
        f = f.merge(dim_eq.rename(columns={"id_equipe": "id_equipe_domicile", "pays": "home_team"}),
                    on="home_team", how="left")
        f = f.merge(dim_eq.rename(columns={"id_equipe": "id_equipe_exterieur", "pays": "away_team"}),
                    on="away_team", how="left")
        f = f.merge(dim_st, left_on=["venue_name", "venue_city"],
                    right_on=["nom_stade", "ville"], how="left")

        fait = pd.DataFrame({
            "id_match": m["match_key"],
            "id_edition": f["id_edition"].astype("Int64"),
            "id_equipe_domicile": f["id_equipe_domicile"].astype("Int64"),
            "id_equipe_exterieur": f["id_equipe_exterieur"].astype("Int64"),
            "id_stade": f["id_stade"].astype("Int64"),
            "date_match": m["match_date"],
            "annee": m["year"].astype("Int64"),
            "phase": m["round"],
            "score_domicile": m["home_score"].astype("Int64"),
            "score_exterieur": m["away_score"].astype("Int64"),
            "xg_domicile": m["home_xg"],
            "xg_exterieur": m["away_xg"],
            "penalty_domicile": m["penalty_home"].astype("Int64"),
            "penalty_exterieur": m["penalty_away"].astype("Int64"),
            "seance_tirs_au_but": m["went_to_penalties"].astype(bool),
            "affluence": m["attendance"].astype("Int64"),
        })
        return _load(fait, "fait_match", {
            "id_match": Text, "id_edition": Integer,
            "id_equipe_domicile": Integer, "id_equipe_exterieur": Integer,
            "id_stade": Integer, "date_match": Date, "annee": Integer, "phase": Text,
            "score_domicile": Integer, "score_exterieur": Integer,
            "xg_domicile": Float, "xg_exterieur": Float,
            "penalty_domicile": Integer, "penalty_exterieur": Integer,
            "seance_tirs_au_but": Boolean, "affluence": BigInteger,
        }, pk="id_match")

    @task
    def build_fait_but(schema: str, *_deps) -> int:
        g = _read_silver("goals").copy()
        dim_j = _read_gold(f"SELECT id_joueur, nom FROM {GOLD_SCHEMA}.dim_joueur")
        dim_eq = _read_gold(f"SELECT id_equipe, pays FROM {GOLD_SCHEMA}.dim_equipe")

        f = g.merge(dim_j, left_on="scorer", right_on="nom", how="left")
        f = f.merge(dim_eq, left_on="team", right_on="pays", how="left")

        fait = pd.DataFrame({
            "id_match": g["match_key"],
            "id_joueur": f["id_joueur"].astype("Int64"),
            "id_equipe": f["id_equipe"].astype("Int64"),
            "minute": g["minute"].astype("Int64"),
            "temps_additionnel": g["stoppage_time"].astype("Int64"),
            "minute_absolue": g["minute_absolute"].astype("Int64"),
            "periode": g["period"],
            "type_but": g["goal_type"],
            "est_penalty": g["is_penalty"].astype(bool),
            "est_csc": g["is_own_goal"].astype(bool),
        })
        fait = _surrogate(fait, "id_but")
        return _load(fait, "fait_but", {
            "id_but": Integer, "id_match": Text, "id_joueur": Integer, "id_equipe": Integer,
            "minute": Integer, "temps_additionnel": Integer, "minute_absolue": Integer,
            "periode": Text, "type_but": Text, "est_penalty": Boolean, "est_csc": Boolean,
        }, pk="id_but")

    # --- Cles etrangeres (documentent les relations pour le text-to-SQL) ---

    @task
    def add_foreign_keys(schema: str, *_facts) -> None:
        stmts = [
            f'ALTER TABLE {GOLD_SCHEMA}.fait_match ADD FOREIGN KEY (id_edition) REFERENCES {GOLD_SCHEMA}.dim_edition(id_edition);',
            f'ALTER TABLE {GOLD_SCHEMA}.fait_match ADD FOREIGN KEY (id_equipe_domicile) REFERENCES {GOLD_SCHEMA}.dim_equipe(id_equipe);',
            f'ALTER TABLE {GOLD_SCHEMA}.fait_match ADD FOREIGN KEY (id_equipe_exterieur) REFERENCES {GOLD_SCHEMA}.dim_equipe(id_equipe);',
            f'ALTER TABLE {GOLD_SCHEMA}.fait_match ADD FOREIGN KEY (id_stade) REFERENCES {GOLD_SCHEMA}.dim_stade(id_stade);',
            f'ALTER TABLE {GOLD_SCHEMA}.fait_but ADD FOREIGN KEY (id_match) REFERENCES {GOLD_SCHEMA}.fait_match(id_match);',
            f'ALTER TABLE {GOLD_SCHEMA}.fait_but ADD FOREIGN KEY (id_joueur) REFERENCES {GOLD_SCHEMA}.dim_joueur(id_joueur);',
            f'ALTER TABLE {GOLD_SCHEMA}.fait_but ADD FOREIGN KEY (id_equipe) REFERENCES {GOLD_SCHEMA}.dim_equipe(id_equipe);',
        ]
        eng = _engine()
        with eng.begin() as conn:
            for s in stmts:
                conn.execute(text(s))

    # --- Marts metier (agregats, moyennes, stats) --------------------------

    @task
    def build_marts(schema: str, *_facts) -> dict:
        eng = _engine()
        fm = _read_gold(f"SELECT * FROM {GOLD_SCHEMA}.fait_match")
        fb = _read_gold(f"SELECT * FROM {GOLD_SCHEMA}.fait_but")
        dj = _read_gold(f"SELECT id_joueur, nom, nationalite FROM {GOLD_SCHEMA}.dim_joueur")
        de = _read_gold(f"SELECT id_edition, annee, pays_hote FROM {GOLD_SCHEMA}.dim_edition")
        dq = _read_gold(f"SELECT id_equipe, pays, confederation FROM {GOLD_SCHEMA}.dim_equipe")
        counts = {}

        # 1) Classement des buteurs (hors csc), toutes editions confondues.
        vrais = fb[~fb["est_csc"]]
        clsm = (vrais.groupby("id_joueur")
                     .agg(nb_buts=("id_but", "count"),
                          nb_penaltys=("est_penalty", "sum"))
                     .reset_index()
                     .merge(dj, on="id_joueur", how="left")
                     .sort_values("nb_buts", ascending=False))
        clsm["nb_penaltys"] = clsm["nb_penaltys"].astype(int)
        clsm.insert(0, "rang", range(1, len(clsm) + 1))
        clsm = clsm[["rang", "id_joueur", "nom", "nationalite", "nb_buts", "nb_penaltys"]]
        clsm.to_sql("mart_classement_buteurs", eng, schema=GOLD_SCHEMA,
                    if_exists="replace", index=False, method="multi", chunksize=500)
        counts["mart_classement_buteurs"] = len(clsm)

        # 2) Stats par equipe (toutes editions) : V/N/D, buts pour/contre.
        dom = fm.rename(columns={
            "id_equipe_domicile": "id_equipe", "score_domicile": "bp", "score_exterieur": "bc"})[
            ["id_equipe", "bp", "bc"]].copy()
        ext = fm.rename(columns={
            "id_equipe_exterieur": "id_equipe", "score_exterieur": "bp", "score_domicile": "bc"})[
            ["id_equipe", "bp", "bc"]].copy()
        allm = pd.concat([dom, ext], ignore_index=True).dropna(subset=["id_equipe"])
        allm["victoire"] = (allm["bp"] > allm["bc"]).astype(int)
        allm["nul"] = (allm["bp"] == allm["bc"]).astype(int)
        allm["defaite"] = (allm["bp"] < allm["bc"]).astype(int)
        se = (allm.groupby("id_equipe")
                  .agg(nb_matchs=("bp", "count"), victoires=("victoire", "sum"),
                       nuls=("nul", "sum"), defaites=("defaite", "sum"),
                       buts_marques=("bp", "sum"), buts_encaisses=("bc", "sum"))
                  .reset_index())
        se["difference_buts"] = se["buts_marques"] - se["buts_encaisses"]
        se["id_equipe"] = se["id_equipe"].astype(int)
        se = se.merge(dq, on="id_equipe", how="left").sort_values(
            ["victoires", "difference_buts"], ascending=False)
        se = se[["id_equipe", "pays", "confederation", "nb_matchs", "victoires",
                 "nuls", "defaites", "buts_marques", "buts_encaisses", "difference_buts"]]
        se.to_sql("mart_stats_equipe", eng, schema=GOLD_SCHEMA,
                  if_exists="replace", index=False, method="multi", chunksize=500)
        counts["mart_stats_equipe"] = len(se)

        # 3) Stats par edition : nb matchs, nb buts, moyenne buts/match, affluence.
        agg = (fm.groupby("id_edition")
                 .agg(nb_matchs=("id_match", "count"),
                      buts_domicile=("score_domicile", "sum"),
                      buts_exterieur=("score_exterieur", "sum"),
                      affluence_moyenne=("affluence", "mean"))
                 .reset_index())
        agg["nb_buts"] = (agg["buts_domicile"].fillna(0) + agg["buts_exterieur"].fillna(0)).astype(int)
        agg["moyenne_buts_par_match"] = (agg["nb_buts"] / agg["nb_matchs"]).round(2)
        agg["affluence_moyenne"] = agg["affluence_moyenne"].round(0).astype("Int64")
        agg = agg.dropna(subset=["id_edition"])
        agg["id_edition"] = agg["id_edition"].astype(int)
        agg = agg.merge(de, on="id_edition", how="left").sort_values("annee")
        agg = agg[["id_edition", "annee", "pays_hote", "nb_matchs", "nb_buts",
                   "moyenne_buts_par_match", "affluence_moyenne"]]
        agg.to_sql("mart_stats_edition", eng, schema=GOLD_SCHEMA,
                   if_exists="replace", index=False, method="multi", chunksize=500)
        counts["mart_stats_edition"] = len(agg)

        # 4) Repartition des buts par periode de jeu.
        per = (fb.groupby("periode").agg(nb_buts=("id_but", "count")).reset_index())
        total = per["nb_buts"].sum() or 1
        per["pourcentage"] = (100 * per["nb_buts"] / total).round(1)
        per = per.sort_values("nb_buts", ascending=False)
        per.to_sql("mart_buts_par_periode", eng, schema=GOLD_SCHEMA,
                   if_exists="replace", index=False, method="multi", chunksize=500)
        counts["mart_buts_par_periode"] = len(per)

        return counts

    # --- Orchestration -----------------------------------------------------
    schema = init_schema()

    d_ed = build_dim_edition(schema)
    d_eq = build_dim_equipe(schema)
    d_st = build_dim_stade(schema)
    d_jo = build_dim_joueur(schema)

    f_match = build_fait_match(schema, d_ed, d_eq, d_st)
    f_but = build_fait_but(schema, f_match, d_jo, d_eq)

    add_foreign_keys(schema, f_match, f_but)
    build_marts(schema, f_match, f_but)


transformation_gold_worldcup()
