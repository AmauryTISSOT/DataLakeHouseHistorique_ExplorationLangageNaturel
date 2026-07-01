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
| Airflow | http://localhost:8080 | voir `docker compose logs airflow` |
| Superset | http://localhost:8088 | admin créé à l'init |
| Ollama | http://localhost:11434 | — |
