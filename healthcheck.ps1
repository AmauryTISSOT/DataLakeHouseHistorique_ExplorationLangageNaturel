<#
.SYNOPSIS
    Verifie l'etat de sante de tous les conteneurs de la stack.
.DESCRIPTION
    Teste chaque service (SeaweedFS, PostgreSQL, Airflow, Superset, Ollama)
    et affiche un statut OK / KO par service, plus un recapitulatif final.
.EXAMPLE
    ./healthcheck.ps1
#>

$ErrorActionPreference = "SilentlyContinue"

# Compteurs globaux
$script:okCount = 0
$script:koCount = 0

function Write-Result {
    param(
        [string]$Name,
        [bool]$Ok,
        [string]$Detail = ""
    )
    if ($Ok) {
        Write-Host ("[OK]  {0,-22}" -f $Name) -ForegroundColor Green -NoNewline
        $script:okCount++
    } else {
        Write-Host ("[KO]  {0,-22}" -f $Name) -ForegroundColor Red -NoNewline
        $script:koCount++
    }
    if ($Detail) { Write-Host $Detail -ForegroundColor DarkGray } else { Write-Host "" }
}

function Test-HttpEndpoint {
    param([string]$Url, [int]$TimeoutSec = 5, [int[]]$OkCodes = @(200))
    try {
        $resp = Invoke-WebRequest -Uri $Url -TimeoutSec $TimeoutSec -UseBasicParsing -Method Get
        return @{ Ok = ($OkCodes -contains [int]$resp.StatusCode); Code = [int]$resp.StatusCode }
    } catch {
        # Certains endpoints repondent 401/403/302 = le service est vivant
        $code = $null
        if ($_.Exception.Response) { $code = [int]$_.Exception.Response.StatusCode }
        return @{ Ok = ($code -ne $null -and $OkCodes -contains $code); Code = $code }
    }
}

Write-Host ""
Write-Host "=== Verification de la stack Docker ===" -ForegroundColor Cyan
Write-Host ""

# --- 0. Docker dispo ? ---
$dockerOk = $false
try { docker info *> $null; $dockerOk = ($LASTEXITCODE -eq 0) } catch {}
if (-not $dockerOk) {
    Write-Host "[KO]  Docker n'est pas accessible. Demarre Docker Desktop puis relance." -ForegroundColor Red
    exit 1
}

# --- 1. Etat des conteneurs (docker compose) ---
Write-Host "--- Conteneurs ---" -ForegroundColor Cyan
$running = docker compose ps --services --filter "status=running" 2>$null
$allServices = docker compose ps --services 2>$null

# Jobs ponctuels : ne doivent pas etre "running", on teste leur exit code plus bas
$initJobs = @("seaweedfs-init", "ollama-init")

foreach ($svc in $allServices) {
    if ($initJobs -contains $svc) { continue }
    $isUp = $running -contains $svc
    Write-Result -Name $svc -Ok $isUp -Detail $(if ($isUp) { "running" } else { "arrete" })
}

# Cas particulier : jobs d'init, doivent se terminer en exit 0
foreach ($job in $initJobs) {
    $jobExit = docker inspect --format "{{.State.ExitCode}}" (docker compose ps -a -q $job 2>$null) 2>$null
    Write-Result -Name "$job (job)" -Ok ($jobExit -eq "0") -Detail "exit=$jobExit (0 attendu)"
}

Write-Host ""
Write-Host "--- Services ---" -ForegroundColor Cyan

# --- 2. SeaweedFS : UI master + API S3 + buckets ---
$master = Test-HttpEndpoint -Url "http://localhost:9333/cluster/status"
Write-Result -Name "SeaweedFS master" -Ok $master.Ok -Detail "http://localhost:9333 (code $($master.Code))"

$s3 = Test-HttpEndpoint -Url "http://localhost:8333" -OkCodes @(200, 403, 400)
Write-Result -Name "SeaweedFS S3 API" -Ok $s3.Ok -Detail "http://localhost:8333 (code $($s3.Code))"

# Buckets attendus via un client mc jetable
# On resout l'ID reel du conteneur seaweedfs (nom prefixe par le projet compose)
$swId = docker compose ps -q seaweedfs 2>$null
$buckets = docker run --rm --entrypoint /bin/sh --network "container:$swId" minio/mc `
    -c "mc alias set s http://localhost:8333 minio minio12345 >/dev/null 2>&1 && mc ls s 2>/dev/null" 2>$null
$hasBuckets = ($buckets -match "bronze") -and ($buckets -match "silver") -and ($buckets -match "gold")
Write-Result -Name "S3 buckets" -Ok $hasBuckets -Detail "bronze / silver / gold"

# --- 3. PostgreSQL ---
docker compose exec -T postgres pg_isready -U app -d gold *> $null
Write-Result -Name "PostgreSQL" -Ok ($LASTEXITCODE -eq 0) -Detail "pg_isready sur la base gold"

# --- 4. Airflow ---
$airflow = Test-HttpEndpoint -Url "http://localhost:8080/health" -OkCodes @(200)
if (-not $airflow.Ok) { $airflow = Test-HttpEndpoint -Url "http://localhost:8080" -OkCodes @(200, 302, 401) }
Write-Result -Name "Airflow" -Ok $airflow.Ok -Detail "http://localhost:8080 (code $($airflow.Code))"

# --- 5. Superset ---
$superset = Test-HttpEndpoint -Url "http://localhost:8088/health" -OkCodes @(200)
if (-not $superset.Ok) { $superset = Test-HttpEndpoint -Url "http://localhost:8088/login/" -OkCodes @(200, 302) }
Write-Result -Name "Superset" -Ok $superset.Ok -Detail "http://localhost:8088 (code $($superset.Code))"

# --- 6. Ollama (+ GPU) ---
$ollama = Test-HttpEndpoint -Url "http://localhost:11434/api/tags" -OkCodes @(200)
Write-Result -Name "Ollama API" -Ok $ollama.Ok -Detail "http://localhost:11434"

docker compose exec -T ollama nvidia-smi *> $null
$gpuOk = ($LASTEXITCODE -eq 0)
Write-Result -Name "Ollama GPU (nvidia)" -Ok $gpuOk -Detail $(if ($gpuOk) { "GPU NVIDIA detecte" } else { "GPU non detecte (CPU only)" })

# Modele attendu present dans Ollama
$expectedModel = "qwen3:8b"
$models = docker compose exec -T ollama ollama list 2>$null
$hasModel = ($models -match [regex]::Escape($expectedModel))
Write-Result -Name "Ollama modele" -Ok $hasModel -Detail "$expectedModel present"

# --- Recapitulatif ---
Write-Host ""
Write-Host "=== Recapitulatif ===" -ForegroundColor Cyan
Write-Host ("OK : {0}   KO : {1}" -f $script:okCount, $script:koCount) -ForegroundColor $(if ($script:koCount -eq 0) { "Green" } else { "Yellow" })
Write-Host ""

if ($script:koCount -gt 0) { exit 1 } else { exit 0 }
