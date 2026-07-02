"""DAG de transformation Silver — Projet 4 (Lakehouse + text-to-SQL).

Lit les objets BRUTS deposes par le DAG Bronze dans le bucket ``bronze``
(SeaweedFS, API S3 compatible), les NETTOIE et les NORMALISE, puis ecrit des
tables Parquet typees dans le bucket ``silver``, partitionnees par date
d'ingestion :

    silver/
      matches/ingest_date=YYYY-MM-DD/matches.parquet
      goals/ingest_date=YYYY-MM-DD/goals.parquet
      editions/ingest_date=YYYY-MM-DD/editions.parquet
      fifa_ranking/ingest_date=YYYY-MM-DD/fifa_ranking.parquet
      schedule_2026/ingest_date=YYYY-MM-DD/schedule_2026.parquet
      fbref_schedule/ingest_date=YYYY-MM-DD/fbref_schedule.parquet
      player_stats/ingest_date=YYYY-MM-DD/player_stats.parquet
      player_shooting/ingest_date=YYYY-MM-DD/player_shooting.parquet
      team_stats/ingest_date=YYYY-MM-DD/team_stats.parquet
      _manifests/ingest_date=YYYY-MM-DD/manifest.json

Principe de la couche Silver : donnees conformes et exploitables telles quelles
par la couche Gold (schema en etoile, dbt) et le text-to-SQL. Les
transformations appliquees :

* noms de colonnes normalises en ``snake_case`` ;
* noms d'equipes/pays canonises (``TEAM_NAME_MAP``) ;
* typage strict (entiers nullables ``Int64``, flottants, booleens) ;
* DATES / HEURES / MINUTES traitees avec soin :
    - ``match_date`` en vraie date (Parquet ``date32``) ;
    - heures de coup d'envoi normalisees ``HH:MM`` + timestamp local combine ;
    - buts eclates a la minute, avec temps additionnel et periode de jeu
      (1re mi-temps / 2e mi-temps / prolongation).

Les fonctions ``transform_*`` sont PURES (DataFrame -> DataFrame) : elles ne
touchent pas a S3, ce qui les rend testables hors Airflow. Les taches se
chargent uniquement des entrees/sorties S3.

Re-executer le DAG pour une meme date ecrase les memes cles (idempotent).
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
from datetime import datetime, timedelta

import boto3
import pandas as pd
from airflow.decorators import dag, task
from botocore.config import Config

# --- Configuration (surchargable par variables d'environnement) -------------
S3_ENDPOINT = os.getenv("SILVER_S3_ENDPOINT", "http://seaweedfs:8333")
S3_ACCESS_KEY = os.getenv("SILVER_S3_ACCESS_KEY", "minio")
S3_SECRET_KEY = os.getenv("SILVER_S3_SECRET_KEY", "minio12345")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")


def _s3_client():
    # addressing_style "path" obligatoire pour SeaweedFS (cf. DAG Bronze).
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3},
        ),
    )


# ---------------------------------------------------------------------------
# Helpers de normalisation (purs, sans I/O)
# ---------------------------------------------------------------------------

# Canonisation des noms d'equipes/pays : les sources melangent plusieurs
# conventions (FBref, Kaggle, FIFA). On ramene tout a un libelle unique pour
# permettre les jointures en couche Gold (DIM_EQUIPE).
TEAM_NAME_MAP = {
    "Korea Republic": "South Korea",
    "Korea DPR": "North Korea",
    "IR Iran": "Iran",
    "Iran": "Iran",
    "China PR": "China",
    "USA": "United States",
    "United States": "United States",
    "Czechia": "Czech Republic",
    "West Germany": "Germany",
    "Soviet Union": "Russia",
    "FR Yugoslavia": "Serbia",
    "Turkiye": "Turkey",
    "Türkiye": "Turkey",
    "Republic of Ireland": "Ireland",
    "Bosnia and Herzegovina": "Bosnia and Herzegovina",
}


def _snake(name: str) -> str:
    """"Per 90 Minutes_G+A" -> "per_90_minutes_g_a"."""
    s = str(name).strip()
    s = s.replace("%", "_pct").replace("+", "_plus_").replace("/", "_per_")
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_").lower()
    return s or "col"


def _norm_team(value):
    """Trim + espaces internes reduits + canonisation via TEAM_NAME_MAP."""
    if pd.isna(value):
        return None
    s = re.sub(r"\s+", " ", str(value)).strip()
    return TEAM_NAME_MAP.get(s, s)


def _match_key(match_date, home: str | None, away: str | None) -> str:
    """Cle deterministe d'un match (date normalisee + equipes canonisees).

    Recalculable a l'identique dans transform_matches et transform_goals afin
    de relier les buts a leur match sans identifiant natif.
    """
    raw = f"{match_date}|{home}|{away}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]


def _to_date(series: pd.Series):
    """Serie -> vraie date (objets datetime.date, ecrits en Parquet date32)."""
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.date


def _norm_time(value):
    """"9:00" / "13:00" -> "09:00" (zero-pad) ; sinon None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return datetime.strptime(str(value).strip(), "%H:%M").strftime("%H:%M")
    except ValueError:
        return None


def _parse_kickoff(value):
    """"13:00 (22:00)" -> ("13:00", "22:00") ; "17:00" -> ("17:00", None).

    FBref/Kaggle notent l'heure locale du stade suivie, entre parentheses, de
    l'heure dans un autre fuseau. On garde les deux, normalisees en HH:MM.
    """
    if pd.isna(value):
        return None, None
    m = re.match(r"^\s*(\d{1,2}:\d{2})(?:\s*\((\d{1,2}:\d{2})\))?", str(value))
    if not m:
        return None, None
    return _norm_time(m.group(1)), _norm_time(m.group(2))


def _split_score(value):
    """"3–1" / "(4) 3–3 (2)" -> (3, 1) / (3, 3). Prend les 2 premiers entiers
    du temps reglementaire (les tirs au but entre parentheses sont ignores)."""
    if pd.isna(value):
        return None, None
    s = str(value)
    # Retire les scores de tirs au but "(4) ... (2)" pour ne garder que le score.
    s = re.sub(r"\(\d+\)", "", s)
    nums = re.findall(r"\d+", s)
    if len(nums) >= 2:
        return int(nums[0]), int(nums[1])
    return None, None


def _parse_minute(token: str):
    """"45" -> (45, 0, 45) ; "90+7" -> (90, 7, 97). Renvoie (base, add, abs)."""
    m = re.match(r"^(\d+)(?:\+(\d+))?$", token.strip())
    if not m:
        return None, None, None
    base = int(m.group(1))
    add = int(m.group(2)) if m.group(2) else 0
    return base, add, base + add


def _period_from_minute(base):
    """Periode de jeu a partir de la minute de base (hors temps additionnel)."""
    if base is None:
        return None
    if base <= 45:
        return "1re mi-temps"
    if base <= 90:
        return "2e mi-temps"
    return "prolongation"


def _parse_goal_cell(cell):
    """"Messi · 108|Di María · 36" -> [("Messi", "108"), ("Di María", "36")].

    Separateur de buts : "|". Separateur nom/minute : "·". Le marqueur de
    penalty "(P)" est retire du nom (le type de but est porte par la colonne).
    """
    if pd.isna(cell):
        return []
    out = []
    for chunk in str(cell).split("|"):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "·" in chunk:
            name, minute = chunk.rsplit("·", 1)
        else:
            name, minute = chunk, ""
        name = re.sub(r"\(P\)", "", name).strip()
        out.append((name, minute.strip()))
    return out


def _flatten_multiheader(raw: bytes, n_index: int) -> pd.DataFrame:
    """Aplati un CSV FBref a en-tete multi-niveaux (exporte par soccerdata).

    Les colonnes ont 2 niveaux (groupe, sous-stat) et l'index en a ``n_index``
    (league/season/team[/player]). On fusionne les niveaux non "Unnamed" puis
    on ``snake_case``. Cela desambiguise p.ex. Performance_Gls (total) de
    Per 90 Minutes_Gls (par 90 min).
    """
    df = pd.read_csv(io.BytesIO(raw), header=[0, 1], index_col=list(range(n_index)))
    df = df.reset_index()
    cols = []
    for col in df.columns:
        if isinstance(col, tuple):
            levels = [str(c) for c in col if not str(c).startswith("Unnamed")]
            cols.append(_snake("_".join(levels)) if levels else "col")
        else:
            cols.append(_snake(col))
    df.columns = cols
    return df


# ---------------------------------------------------------------------------
# Transformations PURES (DataFrame brut -> DataFrame Silver)
# ---------------------------------------------------------------------------

def transform_matches(df: pd.DataFrame) -> pd.DataFrame:
    """kaggle/matches_1930_2022.csv -> une ligne par match, typee."""
    out = pd.DataFrame()
    out["year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
    out["match_date"] = _to_date(df["Date"])
    out["round"] = df["Round"].astype("string").str.strip()
    out["home_team"] = df["home_team"].map(_norm_team).astype("string")
    out["away_team"] = df["away_team"].map(_norm_team).astype("string")
    out["home_score"] = pd.to_numeric(df["home_score"], errors="coerce").astype("Int64")
    out["away_score"] = pd.to_numeric(df["away_score"], errors="coerce").astype("Int64")
    out["home_xg"] = pd.to_numeric(df["home_xg"], errors="coerce")
    out["away_xg"] = pd.to_numeric(df["away_xg"], errors="coerce")
    out["penalty_home"] = pd.to_numeric(df["home_penalty"], errors="coerce").astype("Int64")
    out["penalty_away"] = pd.to_numeric(df["away_penalty"], errors="coerce").astype("Int64")
    out["went_to_penalties"] = out["penalty_home"].notna() & out["penalty_away"].notna()
    out["attendance"] = pd.to_numeric(df["Attendance"], errors="coerce").astype("Int64")

    venue = df["Venue"].astype("string").str.strip()
    split = venue.str.rsplit(",", n=1, expand=True)
    out["venue_name"] = split[0].str.strip()
    out["venue_city"] = (split[1].str.strip() if split.shape[1] > 1 else pd.NA)
    out["host"] = df["Host"].map(_norm_team).astype("string")
    out["referee"] = df["Referee"].astype("string").str.strip()

    out["match_key"] = [
        _match_key(d, h, a)
        for d, h, a in zip(out["match_date"], out["home_team"], out["away_team"])
    ]
    # match_key en tete
    cols = ["match_key"] + [c for c in out.columns if c != "match_key"]
    return out[cols]


def transform_goals(df: pd.DataFrame) -> pd.DataFrame:
    """kaggle/matches_1930_2022.csv -> une ligne par but (alimente FAIT_BUT).

    Reconstruit tous les buts d'un match a partir de 3 colonnes par cote :
    jeu ouvert (home_goal), penalty (home_penalty_goal), csc (home_own_goal).
    """
    # (colonne, cote, type de but). Le "cote" est l'equipe CREDITEE du but ;
    # pour un csc le buteur est un joueur adverse mais le but compte pour ce cote.
    sources = [
        ("home_goal", "home", "open_play"),
        ("away_goal", "away", "open_play"),
        ("home_penalty_goal", "home", "penalty"),
        ("away_penalty_goal", "away", "penalty"),
        ("home_own_goal", "home", "own_goal"),
        ("away_own_goal", "away", "own_goal"),
    ]
    rows = []
    for _, r in df.iterrows():
        match_date = pd.to_datetime(r["Date"], errors="coerce")
        match_date = match_date.date() if pd.notna(match_date) else None
        home = _norm_team(r["home_team"])
        away = _norm_team(r["away_team"])
        year = r["Year"]
        mkey = _match_key(match_date, home, away)
        for col, side, goal_type in sources:
            if col not in df.columns:
                continue
            for scorer, minute_tok in _parse_goal_cell(r[col]):
                base, add, absolute = _parse_minute(minute_tok)
                team = home if side == "home" else away
                opponent = away if side == "home" else home
                rows.append(
                    {
                        "match_key": mkey,
                        "year": year,
                        "match_date": match_date,
                        "team": team,
                        "opponent": opponent,
                        "side": side,
                        "scorer": scorer or None,
                        "goal_type": goal_type,
                        "is_penalty": goal_type == "penalty",
                        "is_own_goal": goal_type == "own_goal",
                        "minute": base,
                        "stoppage_time": add,
                        "minute_absolute": absolute,
                        "period": _period_from_minute(base),
                    }
                )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    for c in ("minute", "stoppage_time", "minute_absolute"):
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")
    for c in ("team", "opponent", "scorer", "goal_type", "period", "side"):
        out[c] = out[c].astype("string")
    return out.sort_values(["match_key", "minute_absolute"]).reset_index(drop=True)


def transform_editions(df: pd.DataFrame) -> pd.DataFrame:
    """kaggle/world_cup.csv -> une ligne par edition (alimente DIM_EDITION)."""
    out = pd.DataFrame()
    out["year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
    out["host"] = df["Host"].map(_norm_team).astype("string")
    out["teams"] = pd.to_numeric(df["Teams"], errors="coerce").astype("Int64")
    out["champion"] = df["Champion"].map(_norm_team).astype("string")
    out["runner_up"] = df["Runner-Up"].map(_norm_team).astype("string")

    # "Kylian Mbappé - 8" -> ("Kylian Mbappé", 8)
    scorer = df["TopScorrer"].astype("string").str.rsplit("-", n=1, expand=True)
    out["top_scorer"] = scorer[0].str.strip()
    out["top_scorer_goals"] = (
        pd.to_numeric(scorer[1].str.strip(), errors="coerce").astype("Int64")
        if scorer.shape[1] > 1 else pd.NA
    )
    out["attendance_total"] = pd.to_numeric(df["Attendance"], errors="coerce").astype("Int64")
    out["attendance_avg"] = pd.to_numeric(df["AttendanceAvg"], errors="coerce").astype("Int64")
    out["matches"] = pd.to_numeric(df["Matches"], errors="coerce").astype("Int64")
    return out.sort_values("year").reset_index(drop=True)


def transform_fifa_ranking(frames: list[tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    """fifa_ranking_<date>.csv (n snapshots) -> classement long, date incluse.

    ``frames`` : liste de (nom_de_fichier, DataFrame) ; la date du snapshot est
    extraite du nom de fichier (fifa_ranking_YYYY-MM-DD.csv).
    """
    parts = []
    for filename, df in frames:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
        snapshot = pd.to_datetime(m.group(1)).date() if m else None
        cur = pd.DataFrame()
        cur["snapshot_date"] = [snapshot] * len(df)
        cur["team"] = df["team"].map(_norm_team).astype("string")
        cur["team_code"] = df["team_code"].astype("string").str.strip()
        cur["confederation"] = df["association"].astype("string").str.strip()
        cur["rank"] = pd.to_numeric(df["rank"], errors="coerce").astype("Int64")
        cur["previous_rank"] = pd.to_numeric(df["previous_rank"], errors="coerce").astype("Int64")
        cur["points"] = pd.to_numeric(df["points"], errors="coerce")
        cur["previous_points"] = pd.to_numeric(df["previous_points"], errors="coerce")
        parts.append(cur)
    out = pd.concat(parts, ignore_index=True)
    return out.sort_values(["snapshot_date", "rank"]).reset_index(drop=True)


def transform_schedule_2026(df: pd.DataFrame) -> pd.DataFrame:
    """kaggle/schedule_2026.csv -> calendrier normalise (dates/heures)."""
    out = pd.DataFrame()
    out["year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
    out["round"] = df["Round"].astype("string").str.strip()
    out["match_date"] = _to_date(df["Date"])
    local, other = zip(*df["Time"].map(_parse_kickoff))
    out["kickoff_local_time"] = pd.array(local, dtype="string")
    out["kickoff_other_time"] = pd.array(other, dtype="string")
    out["kickoff_datetime_local"] = pd.to_datetime(
        df["Date"].astype("string") + " " + pd.Series(local, index=df.index).fillna(""),
        errors="coerce",
    )
    out["home_team"] = df["home_team"].map(_norm_team).astype("string")
    out["away_team"] = df["away_team"].map(_norm_team).astype("string")
    out["referee"] = df["Referee"].astype("string").str.strip()
    return out.reset_index(drop=True)


def transform_fbref_schedule(df: pd.DataFrame) -> pd.DataFrame:
    """soccerdata/schedule.csv -> calendrier historique (scores/dates/heures)."""
    out = pd.DataFrame()
    out["season"] = pd.to_numeric(df.get("season"), errors="coerce").astype("Int64") \
        if "season" in df.columns else pd.NA
    out["round"] = df["round"].astype("string").str.strip()
    out["week"] = pd.to_numeric(df["week"], errors="coerce").astype("Int64")
    out["match_date"] = _to_date(df["date"])
    local, other = zip(*df["time"].map(_parse_kickoff))
    out["kickoff_local_time"] = pd.array(local, dtype="string")
    out["kickoff_other_time"] = pd.array(other, dtype="string")
    out["kickoff_datetime_local"] = pd.to_datetime(
        df["date"].astype("string") + " " + pd.Series(local, index=df.index).fillna(""),
        errors="coerce",
    )
    out["home_team"] = df["home_team"].map(_norm_team).astype("string")
    out["away_team"] = df["away_team"].map(_norm_team).astype("string")
    home_s, away_s = zip(*df["score"].map(_split_score))
    out["home_score"] = pd.array(home_s, dtype="Int64")
    out["away_score"] = pd.array(away_s, dtype="Int64")
    out["attendance"] = pd.to_numeric(df["attendance"], errors="coerce").astype("Int64")
    # "Arena Corinthians (Neutral Site)" -> venue + drapeau site neutre
    venue = df["venue"].astype("string").str.strip()
    out["neutral_site"] = venue.str.contains(r"\(Neutral Site\)", na=False)
    out["venue"] = venue.str.replace(r"\s*\(Neutral Site\)", "", regex=True).str.strip()
    out["referee"] = df["referee"].astype("string").str.strip()
    out["game_id"] = df["game_id"].astype("string").str.strip()
    return out.reset_index(drop=True)


def _transform_flat(df: pd.DataFrame) -> pd.DataFrame:
    """Post-traitement commun des tables FBref aplaties : normalise league,
    season, team, player, nation ; type season en entier."""
    ren = {"league_": "league", "season_": "season", "team_": "team", "player_": "player"}
    df = df.rename(columns={k: v for k, v in ren.items() if k in df.columns})
    if "season" in df.columns:
        df["season"] = pd.to_numeric(df["season"], errors="coerce").astype("Int64")
    for c in ("team", "nation"):
        if c in df.columns:
            df[c] = df[c].map(_norm_team).astype("string")
    for c in ("player", "league", "pos"):
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# I/O S3
# ---------------------------------------------------------------------------

def _get_bytes(client, key: str) -> bytes:
    return client.get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read()


def _put_parquet(client, table: str, df: pd.DataFrame, ds: str, source: str) -> dict:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    data = buf.getvalue()
    key = f"{table}/ingest_date={ds}/{table}.parquet"
    client.put_object(
        Bucket=SILVER_BUCKET, Key=key, Body=data,
        ContentType="application/vnd.apache.parquet",
    )
    return {
        "table": table, "key": key, "rows": int(len(df)),
        "size_bytes": len(data), "source": source,
    }


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

# Cle = (prefixe bronze, nom de fichier) -> nom logique de la source.
_WANTED = {
    ("raw_kaggle", "matches_1930_2022.csv"): "matches",
    ("raw_kaggle", "world_cup.csv"): "editions",
    ("raw_kaggle", "schedule_2026.csv"): "schedule_2026",
    ("raw_soccerdata", "schedule.csv"): "fbref_schedule",
    ("raw_soccerdata", "player_stats.csv"): "player_stats",
    ("raw_soccerdata", "player_shooting.csv"): "player_shooting",
    ("raw_soccerdata", "team_stats.csv"): "team_stats",
}


@dag(
    dag_id="transformation_silver_worldcup",
    description="Transformation Silver : nettoyage/normalisation Bronze -> Parquet (SeaweedFS)",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["silver", "transformation", "worldcup", "projet4"],
)
def transformation_silver_worldcup():
    @task
    def ensure_bucket() -> str:
        """Cree le bucket silver s'il n'existe pas."""
        client = _s3_client()
        buckets = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
        if SILVER_BUCKET not in buckets:
            client.create_bucket(Bucket=SILVER_BUCKET)
        return SILVER_BUCKET

    @task
    def resolve_bronze_inputs() -> dict:
        """Localise, pour chaque source, la cle Bronze de la partition
        ingest_date la plus recente. fifa_ranking peut avoir plusieurs
        snapshots -> liste de cles.
        """
        client = _s3_client()
        keys, token = [], None
        while True:
            kw = {"Bucket": BRONZE_BUCKET}
            if token:
                kw["ContinuationToken"] = token
            resp = client.list_objects_v2(**kw)
            keys.extend(o["Key"] for o in resp.get("Contents", []))
            if resp.get("IsTruncated"):
                token = resp["NextContinuationToken"]
            else:
                break

        best: dict[str, tuple[str, str]] = {}          # logique -> (date, key)
        fifa: dict[str, tuple[str, str]] = {}          # fichier -> (date, key)
        pat = re.compile(r"^([^/]+)/ingest_date=(\d{4}-\d{2}-\d{2})/(.+)$")
        for key in keys:
            m = pat.match(key)
            if not m:
                continue
            prefix, date, base = m.group(1), m.group(2), m.group(3).split("/")[-1]
            logical = _WANTED.get((prefix, base))
            if logical:
                if logical not in best or date > best[logical][0]:
                    best[logical] = (date, key)
            elif prefix == "raw_kaggle" and re.match(r"fifa_ranking_.*\.csv$", base):
                if base not in fifa or date > fifa[base][0]:
                    fifa[base] = (date, key)

        resolved: dict = {k: v[1] for k, v in best.items()}
        resolved["fifa_ranking"] = [v[1] for v in fifa.values()]
        if not resolved.get("matches") and not resolved["fifa_ranking"]:
            raise FileNotFoundError(
                f"Aucun objet Bronze exploitable dans '{BRONZE_BUCKET}'. "
                "Le DAG ingestion_bronze_worldcup a-t-il tourne ?"
            )
        return resolved

    @task
    def clean_matches(inputs: dict, bucket: str) -> dict | None:
        key = inputs.get("matches")
        if not key:
            return None
        client = _s3_client()
        df = pd.read_csv(io.BytesIO(_get_bytes(client, key)))
        return _put_parquet(client, "matches", transform_matches(df), _ds(), key)

    @task
    def clean_goals(inputs: dict, bucket: str) -> dict | None:
        key = inputs.get("matches")
        if not key:
            return None
        client = _s3_client()
        df = pd.read_csv(io.BytesIO(_get_bytes(client, key)))
        return _put_parquet(client, "goals", transform_goals(df), _ds(), key)

    @task
    def clean_editions(inputs: dict, bucket: str) -> dict | None:
        key = inputs.get("editions")
        if not key:
            return None
        client = _s3_client()
        df = pd.read_csv(io.BytesIO(_get_bytes(client, key)))
        return _put_parquet(client, "editions", transform_editions(df), _ds(), key)

    @task
    def clean_fifa_ranking(inputs: dict, bucket: str) -> dict | None:
        keys = inputs.get("fifa_ranking") or []
        if not keys:
            return None
        client = _s3_client()
        frames = [
            (k.split("/")[-1], pd.read_csv(io.BytesIO(_get_bytes(client, k))))
            for k in keys
        ]
        return _put_parquet(
            client, "fifa_ranking", transform_fifa_ranking(frames), _ds(),
            ", ".join(keys),
        )

    @task
    def clean_schedule_2026(inputs: dict, bucket: str) -> dict | None:
        key = inputs.get("schedule_2026")
        if not key:
            return None
        client = _s3_client()
        df = pd.read_csv(io.BytesIO(_get_bytes(client, key)))
        return _put_parquet(client, "schedule_2026", transform_schedule_2026(df), _ds(), key)

    @task
    def clean_fbref_schedule(inputs: dict, bucket: str) -> dict | None:
        key = inputs.get("fbref_schedule")
        if not key:
            return None
        client = _s3_client()
        df = pd.read_csv(io.BytesIO(_get_bytes(client, key)))
        return _put_parquet(client, "fbref_schedule", transform_fbref_schedule(df), _ds(), key)

    @task
    def clean_player_stats(inputs: dict, bucket: str) -> dict | None:
        key = inputs.get("player_stats")
        if not key:
            return None
        client = _s3_client()
        df = _transform_flat(_flatten_multiheader(_get_bytes(client, key), n_index=4))
        return _put_parquet(client, "player_stats", df, _ds(), key)

    @task
    def clean_player_shooting(inputs: dict, bucket: str) -> dict | None:
        key = inputs.get("player_shooting")
        if not key:
            return None
        client = _s3_client()
        df = _transform_flat(_flatten_multiheader(_get_bytes(client, key), n_index=4))
        return _put_parquet(client, "player_shooting", df, _ds(), key)

    @task
    def clean_team_stats(inputs: dict, bucket: str) -> dict | None:
        key = inputs.get("team_stats")
        if not key:
            return None
        client = _s3_client()
        df = _transform_flat(_flatten_multiheader(_get_bytes(client, key), n_index=3))
        return _put_parquet(client, "team_stats", df, _ds(), key)

    @task
    def write_manifest(uploads: list, ds: str | None = None, run_id: str | None = None) -> str:
        """Trace de la transformation : liste des tables Silver produites."""
        objects = [u for u in uploads if u]
        manifest = {
            "dag_id": "transformation_silver_worldcup",
            "run_id": run_id,
            "ingest_date": ds,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "table_count": len(objects),
            "total_rows": sum(o["rows"] for o in objects),
            "total_size_bytes": sum(o["size_bytes"] for o in objects),
            "tables": objects,
        }
        client = _s3_client()
        key = f"_manifests/ingest_date={ds}/manifest.json"
        client.put_object(
            Bucket=SILVER_BUCKET, Key=key,
            Body=json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
        return key

    bucket_ready = ensure_bucket()
    inputs = resolve_bronze_inputs()

    uploads = [
        clean_matches(inputs, bucket_ready),
        clean_goals(inputs, bucket_ready),
        clean_editions(inputs, bucket_ready),
        clean_fifa_ranking(inputs, bucket_ready),
        clean_schedule_2026(inputs, bucket_ready),
        clean_fbref_schedule(inputs, bucket_ready),
        clean_player_stats(inputs, bucket_ready),
        clean_player_shooting(inputs, bucket_ready),
        clean_team_stats(inputs, bucket_ready),
    ]
    write_manifest(uploads)


def _ds() -> str:
    """Date logique du run (contexte Airflow), sinon date du jour.

    Permet aux fonctions ``clean_*`` d'ecrire dans la bonne partition sans que
    ``ds`` soit passe explicitement a chaque tache.
    """
    from airflow.operators.python import get_current_context

    try:
        return get_current_context()["ds"]
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d")


transformation_silver_worldcup()
