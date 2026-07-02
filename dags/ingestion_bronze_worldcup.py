"""DAG d'ingestion Bronze — Projet 4 (Lakehouse + text-to-SQL).

Ingestion batch de trois sources heterogenes vers le bucket S3 ``bronze``
(SeaweedFS, API S3 compatible) :

1. CSV  — datasets versionnes dans le repo (``Data/raw/kaggle`` et
          ``Data/raw/soccerdata``), montes en lecture seule dans le conteneur
          Airflow sous /opt/airflow/data (cf. docker-compose.yml).
          La liste des fichiers est decouverte a l'execution (task mappee) :
          un nouveau CSV depose dans Data/raw/ est ingere sans modifier le DAG.
2. JSON — ``worldcup.json`` consolide du depot GitHub jfjelstul/worldcup
          (~36 Mo, structure imbriquee).
3. API  — World Bank (metadonnees pays : region, capitale, niveau de revenu)
          pour enrichir la future DIM_EQUIPE. API publique sans cle
          (RestCountries, envisagee initialement, exige une cle depuis sa v5).

Principe de la couche Bronze : les donnees sont stockees BRUTES (aucune
transformation), partitionnees par date d'ingestion :

    bronze/
      raw_kaggle/ingest_date=YYYY-MM-DD/<fichier>.csv
      raw_soccerdata/ingest_date=YYYY-MM-DD/<fichier>.csv
      worldcup_json/ingest_date=YYYY-MM-DD/worldcup.json
      worldbank/ingest_date=YYYY-MM-DD/countries.json
      _manifests/ingest_date=YYYY-MM-DD/manifest.json

Le manifeste final liste tous les objets deposes (cle, taille, source) :
il servira de point d'entree au catalogue de donnees et a la couche Silver.

Re-executer le DAG pour une meme date ecrase les memes cles (idempotent).

NB : la connexion S3 est faite directement via boto3 pour rester lisible ;
en production on passerait par une Connection Airflow + S3Hook
(provider amazon), configurable depuis l'UI.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import requests
from airflow.decorators import dag, task
from botocore.config import Config

# --- Configuration (surchargable par variables d'environnement) -------------
# Les valeurs par defaut correspondent au docker-compose.yml du projet.
S3_ENDPOINT = os.getenv("BRONZE_S3_ENDPOINT", "http://seaweedfs:8333")
S3_ACCESS_KEY = os.getenv("BRONZE_S3_ACCESS_KEY", "minio")
S3_SECRET_KEY = os.getenv("BRONZE_S3_SECRET_KEY", "minio12345")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")

# Point de montage du dossier Data/ du repo dans le conteneur Airflow.
LOCAL_RAW_DIR = os.getenv("BRONZE_LOCAL_RAW_DIR", "/opt/airflow/data/raw")

# GITHUB_RAW = "https://raw.githubusercontent.com/jfjelstul/worldcup/master"

# Liste complete des pays en une page (295 entrees < per_page=400).
# WORLDBANK_URL = "https://api.worldbank.org/v2/country?format=json&per_page=400"

HTTP_TIMEOUT = 120  # worldcup.json fait ~36 Mo


def _s3_client():
    # addressing_style "path" obligatoire : le style "virtual-host" par defaut
    # de boto3 tenterait de resoudre bronze.seaweedfs, qui n'existe pas.
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


def _download(url: str) -> bytes:
    resp = requests.get(url, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.content


def _upload(key: str, content: bytes, content_type: str, source: str) -> dict:
    _s3_client().put_object(
        Bucket=BRONZE_BUCKET, Key=key, Body=content, ContentType=content_type
    )
    return {"key": key, "size_bytes": len(content), "source": source}


@dag(
    dag_id="ingestion_bronze_worldcup",
    description="Ingestion Bronze multi-sources (CSV locaux, JSON, API) vers SeaweedFS",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["bronze", "ingestion", "worldcup", "projet4"],
)
def ingestion_bronze_worldcup():
    @task
    def ensure_bucket() -> str:
        """Cree le bucket bronze s'il n'existe pas (filet de securite si
        seaweedfs-init n'a pas tourne)."""
        client = _s3_client()
        buckets = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
        if BRONZE_BUCKET not in buckets:
            client.create_bucket(Bucket=BRONZE_BUCKET)
        return BRONZE_BUCKET

    @task
    def list_local_csv() -> list[str]:
        """Decouvre les CSV deposes dans Data/raw/ (chemins relatifs).

        Seuls les .csv sont retenus : fifa-football-world-cup.zip contient
        les memes fichiers que ceux deja extraits a cote de lui.
        """
        root = Path(LOCAL_RAW_DIR)
        files = sorted(
            p.relative_to(root).as_posix() for p in root.rglob("*.csv")
        )
        if not files:
            raise FileNotFoundError(
                f"Aucun CSV trouve sous {root} — le volume ./Data est-il bien "
                "monte dans le conteneur Airflow (docker-compose.yml) ?"
            )
        return files

    @task
    def ingest_local_csv(relpath: str, ds: str | None = None) -> dict:
        """Source 1 — un CSV du repo (task mappee sur list_local_csv).

        kaggle/matches_1930_2022.csv -> raw_kaggle/ingest_date=.../matches_1930_2022.csv
        """
        src = Path(LOCAL_RAW_DIR) / relpath
        parts = Path(relpath).parts
        prefix = f"raw_{parts[0]}" if len(parts) > 1 else "raw_divers"
        filename = "/".join(parts[1:]) if len(parts) > 1 else parts[0]
        return _upload(
            key=f"{prefix}/ingest_date={ds}/{filename}",
            content=src.read_bytes(),
            content_type="text/csv",
            source=f"repo:Data/raw/{relpath}",
        )

    # @task
    # def ingest_worldcup_json(ds: str | None = None) -> dict:
    #     """Source 2 — JSON consolide du depot jfjelstul/worldcup."""
    #     url = f"{GITHUB_RAW}/data-json/worldcup.json"
    #     content = _download(url)
    #     return _upload(
    #         key=f"worldcup_json/ingest_date={ds}/worldcup.json",
    #         content=content,
    #         content_type="application/json",
    #         source=url,
    #     )

    # @task
    # def ingest_api_worldbank(ds: str | None = None) -> dict:
    #     """Source 3 — API REST (metadonnees pays pour DIM_EQUIPE).

    #     La reponse World Bank est une liste [pagination, [pays, ...]].
    #     """
    #     content = _download(WORLDBANK_URL)
    #     payload = json.loads(content)
    #     if not (isinstance(payload, list) and len(payload) == 2 and payload[1]):
    #         raise ValueError("Reponse World Bank vide ou inattendue")
    #     return _upload(
    #         key=f"worldbank/ingest_date={ds}/countries.json",
    #         content=content,
    #         content_type="application/json",
    #         source=WORLDBANK_URL,
    #     )

    @task
    def write_manifest(
        csv_uploads: list[dict],
        other_uploads: list[dict],
        ds: str | None = None,
        run_id: str | None = None,
    ) -> str:
        """Trace de l'ingestion : liste des objets deposes dans Bronze."""
        uploads = list(csv_uploads) + list(other_uploads)
        manifest = {
            "dag_id": "ingestion_bronze_worldcup",
            "run_id": run_id,
            "ingest_date": ds,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "object_count": len(uploads),
            "total_size_bytes": sum(u["size_bytes"] for u in uploads),
            "objects": uploads,
        }
        info = _upload(
            key=f"_manifests/ingest_date={ds}/manifest.json",
            content=json.dumps(manifest, indent=2).encode("utf-8"),
            content_type="application/json",
            source="(genere par le DAG)",
        )
        return info["key"]

    bucket_ready = ensure_bucket()

    local_files = list_local_csv()
    csv_uploads = ingest_local_csv.expand(relpath=local_files)
    # json_upload = ingest_worldcup_json()
    # api_upload = ingest_api_worldbank()

    # bucket_ready >> [local_files, json_upload, api_upload]

    # write_manifest(
    #     csv_uploads=csv_uploads,
    #     other_uploads=[json_upload, api_upload],
    # )


ingestion_bronze_worldcup()
