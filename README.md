# Projet 4 — Data Lakehouse historique + exploration en langage naturel

**Promotion :** MIA 26.2

**Étudiants :**

- Léa DRUFFIN
- Adrien FOUQUET
- Amaury TISSOT
- Satya MINGUEZ

## Prérequis

> ⚠️ **Le `docker-compose.yml` nécessite un GPU NVIDIA.** Le service `ollama` réserve
> un GPU (`driver: nvidia`) pour accélérer l'inférence du modèle. Sur une machine sans
> GPU NVIDIA (ou sans le [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)),
> Docker refuse de démarrer le conteneur avec l'erreur :
>
> ```
> could not select device driver "nvidia" with capabilities: [[gpu]]
> ```
>
> Pour utiliser le GPU, il faut donc :
> - un GPU NVIDIA + pilotes à jour ;
> - le **NVIDIA Container Toolkit** installé et configuré pour Docker.

### Basculer en mode CPU (sans GPU NVIDIA)

Ollama fonctionne aussi **sans GPU** (inférence sur le CPU, plus lente). Pour cela,
commentez le bloc `deploy` du service `ollama` dans le `docker-compose.yml` :

```yaml
  ollama:
    image: ollama/ollama:latest
    ports: ["11434:11434"]
    volumes: ["ollama_data:/root/.ollama"]
    # GPU NVIDIA active -- a commenter sur une machine sans GPU NVIDIA :
    # deploy:
    #   resources:
    #     reservations:
    #       devices: [{ driver: nvidia, count: 1, capabilities: [gpu] }]
```

Relancez ensuite `docker compose up -d` normalement.

## Démarrage de la stack

### 1. Lancer les conteneurs

```powershell
docker compose up -d
```

Au **premier** démarrage, le service `ollama-init` télécharge automatiquement le modèle
`qwen3:8b` (~5 Go). `docker compose up -d` rend la main immédiatement : le téléchargement
se poursuit en arrière-plan. Les démarrages suivants sont instantanés (modèle mis en cache
dans le volume `ollama_data`).

### 2. Vérifier l'état de la stack

Un script de contrôle teste chaque service et affiche un statut `[OK]` / `[KO]` :

```powershell
./healthcheck.ps1
```

Résultat attendu quand tout est prêt : `OK : 16   KO : 0`.

### 3. Si le test « Ollama modèle » est en KO

C'est normal tant que le téléchargement de `qwen3:8b` n'est pas terminé. Vérifie la
progression en consultant les logs du conteneur d'init :

```powershell
docker compose logs -f ollama-init
```

Vous y verrez la progression du `pull` (pourcentage, vitesse). Une fois la ligne `success`
affichée et le conteneur terminé en `exit 0`, relancez le healthcheck :

```powershell
./healthcheck.ps1
```

### Services et interfaces

| Service | URL / Accès | Identifiants |
|---|---|---|
| SeaweedFS (API S3) | http://localhost:8333 | `minio` / `minio12345` |
| SeaweedFS (UI master) | http://localhost:9333 | — |
| SeaweedFS (filer) | http://localhost:8888 | — |
| PostgreSQL | `localhost:5432` (base `gold`) | `app` / `app12345` |
| Airflow | http://localhost:8080 | `datalake` / `datalake` (voir ci-dessous) |
| Superset | http://localhost:8088 | admin créé à l'init |
| Ollama | http://localhost:11434 | — |

### Créer l'utilisateur Airflow

Une fois le conteneur Airflow démarré, créer un utilisateur pour se connecter à
l'interface (http://localhost:8080) :

```powershell
docker compose exec airflow airflow users create --username datalake --firstname Data --lastname Lake --role Admin --email datalake@example.com --password datalake
```

On peut ensuite se connecter avec l'identifiant `datalake` et le mot de passe
`datalake`.

## Architecture de la pipeline (médaillon)

Les données suivent les trois couches classiques d'un lakehouse. L'orchestration
est assurée par Airflow (un DAG par couche, dans `dags/`).

| Couche | DAG | Sortie | Contenu |
|---|---|---|---|
| **Bronze** | `ingestion_bronze_worldcup` | SeaweedFS, bucket `bronze` | CSV bruts, partitionnés par date d'ingestion |
| **Silver** | `transformation_silver_worldcup` | SeaweedFS, bucket `silver` | Parquet nettoyé, normalisé et typé (dates/heures/minutes) |
| **Gold** | `transformation_gold_worldcup` | PostgreSQL, schéma `gold` | Schéma en étoile (dimensions + faits) + tables métier (marts) |

- Le dossier `Data/` du dépôt est monté en lecture seule dans le conteneur Airflow
  (`/opt/airflow/data`) : le DAG bronze y lit les CSV sources.
- La couche Gold est la base **servie** : Superset et l'application text-to-SQL
  l'interrogent.

## Lancer la pipeline

Les trois DAGs sont indépendants au sens d'Airflow ; il faut donc les exécuter
**dans l'ordre** Bronze → Silver → Gold (chaque couche lit ce que la précédente a
écrit).

### Option A — Interface Airflow (http://localhost:8080)

1. Activer les trois DAGs (interrupteur à gauche de leur nom).
2. Cliquer sur ▶ (*Trigger DAG*) pour chacun, **dans l'ordre**, en attendant que le
   précédent soit au vert :
   1. `ingestion_bronze_worldcup`
   2. `transformation_silver_worldcup`
   3. `transformation_gold_worldcup`

### Option B — Ligne de commande

```powershell
docker compose exec airflow airflow dags test ingestion_bronze_worldcup 2026-07-02
docker compose exec airflow airflow dags test transformation_silver_worldcup 2026-07-02
docker compose exec airflow airflow dags test transformation_gold_worldcup 2026-07-02
```

> La date passée aux commandes sert de date d'ingestion (partition
> `ingest_date=…`). Adaptez-la si besoin ; ré-exécuter pour la même date écrase les
> mêmes données (idempotent).

## Explorer les résultats

### Buckets Bronze / Silver (SeaweedFS)

Parcourir les objets déposés via l'interface filer : http://localhost:8888
(dossiers `bronze/` et `silver/`).

### Base Gold (PostgreSQL)

```powershell
docker compose exec postgres psql -U app -d gold
```

Puis, dans psql :

```sql
\dt gold.*                                   -- lister les tables
SELECT * FROM gold.mart_classement_buteurs ORDER BY rang LIMIT 10;
SELECT annee, pays_hote, nb_matchs, nb_buts, moyenne_buts_par_match
FROM gold.mart_stats_edition ORDER BY annee DESC;
```

On peut aussi s'y connecter avec un client graphique (DBeaver, pgAdmin…) sur
`localhost:5432`, base `gold`, schéma `gold` (`app` / `app12345`).
