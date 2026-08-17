#!/usr/bin/env bash
set -euo pipefail

# ── Required ─────────────────────────────────────────────────────────────────
# PROJECT_ID  – GCP project (pass via environment)
# GOOGLE_API_KEY – Google AI Studio key (required only when USE_VERTEX_AI=false; stored in Secret Manager)
# AI_CO_SCIENTIST_POSTGRES_DSN – existing Postgres connection string on the first durable deploy
# ── Optional overrides ───────────────────────────────────────────────────────
# REGION, SERVICE_NAME, REPO_NAME, IMAGE_NAME, USE_VERTEX_AI, GA4_MEASUREMENT_ID, CONCURRENCY, CPU,
# MIN_INSTANCES, MAX_INSTANCES, SERVICE_ACCOUNT, AI_CO_SCIENTIST_SESSION_SECRET,
# ALLOW_EPHEMERAL_STATE, RATE_LIMIT_TRUSTED_PROXY_HOPS, RATE_LIMIT_MAX_KEYS,
# ADK_MAX_RETAINED_COMPLETED_RUNS, ADK_MAX_RETAINED_REPORTS
# ─────────────────────────────────────────────────────────────────────────────

PROJECT_ID="${PROJECT_ID:-}"
REGION="${REGION:-us-central1}"
SERVICE_NAME="${SERVICE_NAME:-ai-co-scientist}"
REPO_NAME="${REPO_NAME:-co-scientist-images}"
IMAGE_NAME="${IMAGE_NAME:-ai-co-scientist}"
USE_VERTEX_AI="${USE_VERTEX_AI:-}"
GOOGLE_API_KEY="${GOOGLE_API_KEY:-}"
BIOGRID_ACCESS_KEY="${BIOGRID_ACCESS_KEY:-}"
BIOGRID_ORCS_ACCESS_KEY="${BIOGRID_ORCS_ACCESS_KEY:-}"
AI_CO_SCIENTIST_POSTGRES_DSN="${AI_CO_SCIENTIST_POSTGRES_DSN:-}"
POSTGRES_DSN="${POSTGRES_DSN:-}"
DATABASE_URL="${DATABASE_URL:-}"
AI_CO_SCIENTIST_SESSION_SECRET="${AI_CO_SCIENTIST_SESSION_SECRET:-}"
ALLOW_EPHEMERAL_STATE="${ALLOW_EPHEMERAL_STATE:-false}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-}"
GA4_MEASUREMENT_ID="${GA4_MEASUREMENT_ID:-G-NTCXHW3B2G}"
CONCURRENCY="${CONCURRENCY:-8}"
CPU="${CPU:-2}"
MIN_INSTANCES="${MIN_INSTANCES:-0}"
MAX_INSTANCES="${MAX_INSTANCES:-1}"
# Google ingress appends the client followed by its proxy address to X-Forwarded-For.
RATE_LIMIT_TRUSTED_PROXY_HOPS="${RATE_LIMIT_TRUSTED_PROXY_HOPS:-1}"
RATE_LIMIT_MAX_KEYS="${RATE_LIMIT_MAX_KEYS:-10000}"
ADK_MAX_RETAINED_COMPLETED_RUNS="${ADK_MAX_RETAINED_COMPLETED_RUNS:-200}"
ADK_MAX_RETAINED_REPORTS="${ADK_MAX_RETAINED_REPORTS:-100}"

ENV_FILE="adk-agent/.env"

load_env_var_from_file() {
  local var_name="$1"
  local env_file="$2"
  local line=""
  local value=""

  if [[ -n "${!var_name:-}" || ! -f "${env_file}" ]]; then
    return 0
  fi

  line="$(grep -E "^${var_name}=" "${env_file}" | tail -n 1 || true)"
  if [[ -z "${line}" ]]; then
    return 0
  fi

  value="${line#*=}"
  if [[ ${#value} -ge 2 ]]; then
    if [[ "${value:0:1}" == '"' && "${value: -1}" == '"' ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
      value="${value:1:${#value}-2}"
    fi
  fi

  export "${var_name}=${value}"
}

load_env_var_from_file "GOOGLE_API_KEY" "${ENV_FILE}"
load_env_var_from_file "BIOGRID_ACCESS_KEY" "${ENV_FILE}"
load_env_var_from_file "BIOGRID_ORCS_ACCESS_KEY" "${ENV_FILE}"
load_env_var_from_file "AI_CO_SCIENTIST_POSTGRES_DSN" "${ENV_FILE}"
load_env_var_from_file "POSTGRES_DSN" "${ENV_FILE}"
load_env_var_from_file "DATABASE_URL" "${ENV_FILE}"
load_env_var_from_file "AI_CO_SCIENTIST_SESSION_SECRET" "${ENV_FILE}"

if [[ -z "${AI_CO_SCIENTIST_POSTGRES_DSN}" ]]; then
  AI_CO_SCIENTIST_POSTGRES_DSN="${POSTGRES_DSN:-${DATABASE_URL:-}}"
fi

if [[ -z "${PROJECT_ID}" ]]; then
  echo "Error: PROJECT_ID is required."
  echo "Usage:"
  echo "  PROJECT_ID=my-project GOOGLE_API_KEY=AIza... bash scripts/deploy_cloud_run.sh"
  echo ""
  echo "Options:"
  echo "  USE_VERTEX_AI=true   – use Vertex AI instead of AI Studio API key"
  echo "  REGION=us-central1   – GCP region (default: us-central1)"
  exit 1
fi

# Default to Vertex AI for Cloud Run deploys. Vertex AI uses project-level
# quotas that are separate from the AI Studio API key, preventing deployed
# services from competing with local development for TPM quota.
# Pass USE_VERTEX_AI=false to explicitly opt into AI Studio API key backend.
if [[ -z "${USE_VERTEX_AI}" ]]; then
  USE_VERTEX_AI="true"
fi

if [[ "${USE_VERTEX_AI}" != "true" && -z "${GOOGLE_API_KEY}" ]]; then
  echo "Error: GOOGLE_API_KEY is required when USE_VERTEX_AI is not true."
  echo "Either set GOOGLE_API_KEY or set USE_VERTEX_AI=true to use Vertex AI."
  exit 1
fi

case "${ALLOW_EPHEMERAL_STATE}" in
  true|TRUE|1|yes|YES)
    ALLOW_EPHEMERAL_STATE="true"
    ;;
  false|FALSE|0|no|NO|"")
    ALLOW_EPHEMERAL_STATE="false"
    ;;
  *)
    echo "Error: ALLOW_EPHEMERAL_STATE must be true or false."
    exit 1
    ;;
esac

if [[ -n "${AI_CO_SCIENTIST_SESSION_SECRET}" && ${#AI_CO_SCIENTIST_SESSION_SECRET} -lt 32 ]]; then
  echo "Error: AI_CO_SCIENTIST_SESSION_SECRET must contain at least 32 characters."
  exit 1
fi

# ── Persistence and secret preflight ─────────────────────────────────────────

GOOGLE_SECRET_NAME="ai-co-scientist-api-key"
BIOGRID_SECRET_NAME="ai-co-scientist-biogrid-access-key"
BIOGRID_ORCS_SECRET_NAME="ai-co-scientist-biogrid-orcs-access-key"
POSTGRES_SECRET_NAME="ai-co-scientist-postgres-dsn"
SESSION_SECRET_NAME="ai-co-scientist-session-secret"
PROJECT_NUMBER=""
RUNTIME_SERVICE_ACCOUNT=""

secret_exists() {
  local secret_name="$1"
  gcloud secrets describe "${secret_name}" \
    --project "${PROJECT_ID}" >/dev/null 2>&1
}

latest_enabled_secret_version() {
  local secret_name="$1"
  gcloud secrets versions list "${secret_name}" \
    --project "${PROJECT_ID}" \
    --filter='state=ENABLED' \
    --sort-by='~createTime' \
    --limit=1 \
    --format='value(name)' 2>/dev/null | awk -F/ 'NF { print $NF; exit }'
}

secret_has_enabled_version() {
  local secret_name="$1"
  secret_exists "${secret_name}" && [[ -n "$(latest_enabled_secret_version "${secret_name}")" ]]
}

if [[ -z "${AI_CO_SCIENTIST_SESSION_SECRET}" ]] && ! secret_has_enabled_version "${SESSION_SECRET_NAME}"; then
  if ! command -v openssl >/dev/null 2>&1; then
    echo "Error: openssl is required to generate the initial browser-session secret."
    echo "Set AI_CO_SCIENTIST_SESSION_SECRET to a random value of at least 32 characters and retry."
    exit 1
  fi
  AI_CO_SCIENTIST_SESSION_SECRET="$(openssl rand -hex 32)"
fi

PERSISTENCE_ENABLED="false"
if [[ -n "${AI_CO_SCIENTIST_POSTGRES_DSN}" ]] || secret_has_enabled_version "${POSTGRES_SECRET_NAME}"; then
  PERSISTENCE_ENABLED="true"
elif [[ "${ALLOW_EPHEMERAL_STATE}" != "true" ]]; then
  echo "Error: durable Cloud Run persistence is not configured."
  echo "Provide AI_CO_SCIENTIST_POSTGRES_DSN for the first deploy, or create"
  echo "Secret Manager secret ${POSTGRES_SECRET_NAME} with an enabled version."
  echo "To intentionally deploy without durable state, set ALLOW_EPHEMERAL_STATE=true."
  exit 1
fi

echo "Using project=${PROJECT_ID} region=${REGION} service=${SERVICE_NAME}"
echo "LLM backend: $([ "${USE_VERTEX_AI}" = "true" ] && echo "Vertex AI" || echo "AI Studio API key")"
echo "Cloud Run concurrency: ${CONCURRENCY}"
echo "Cloud Run CPU: ${CPU}"
echo "Cloud Run min instances: ${MIN_INSTANCES}"
echo "Cloud Run max instances: ${MAX_INSTANCES}"
echo "Cloud Run CPU allocation: request-based"
if [[ "${PERSISTENCE_ENABLED}" == "true" ]]; then
  echo "Conversation persistence: Postgres"
else
  echo "Conversation persistence: ephemeral (explicit override)"
fi

# ── Artifact Registry ────────────────────────────────────────────────────────

if ! gcloud artifacts repositories describe "${REPO_NAME}" \
  --location "${REGION}" \
  --project "${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud artifacts repositories create "${REPO_NAME}" \
    --repository-format=docker \
    --location "${REGION}" \
    --description="Container images for AI Co-Scientist" \
    --project "${PROJECT_ID}"
fi

# ── Store secrets in Secret Manager ──────────────────────────────────────────

resolve_runtime_service_account() {
  if [[ -n "${RUNTIME_SERVICE_ACCOUNT}" ]]; then
    return 0
  fi
  if [[ -n "${SERVICE_ACCOUNT}" ]]; then
    RUNTIME_SERVICE_ACCOUNT="${SERVICE_ACCOUNT}"
    return 0
  fi
  RUNTIME_SERVICE_ACCOUNT="$(
    gcloud run services describe "${SERVICE_NAME}" \
      --project "${PROJECT_ID}" \
      --region "${REGION}" \
      --format='value(spec.template.spec.serviceAccountName)' 2>/dev/null || true
  )"
  if [[ -n "${RUNTIME_SERVICE_ACCOUNT}" ]]; then
    return 0
  fi
  PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"
  RUNTIME_SERVICE_ACCOUNT="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
}

secret_matches_value() {
  local secret_name="$1"
  local secret_value="$2"
  local current_value=""
  current_value="$(
    gcloud secrets versions access latest \
      --secret "${secret_name}" \
      --project "${PROJECT_ID}" 2>/dev/null || true
  )"
  [[ -n "${current_value}" && "${current_value}" == "${secret_value}" ]]
}

ensure_secret_value() {
  local secret_name="$1"
  local secret_value="$2"
  local version=""

  if [[ -n "${secret_value}" ]] && ! secret_exists "${secret_name}"; then
    printf '%s' "${secret_value}" | gcloud secrets create "${secret_name}" \
      --project "${PROJECT_ID}" \
      --replication-policy=automatic \
      --data-file=- >/dev/null
  elif [[ -n "${secret_value}" ]] && ! secret_matches_value "${secret_name}" "${secret_value}"; then
    printf '%s' "${secret_value}" | gcloud secrets versions add "${secret_name}" \
      --project "${PROJECT_ID}" \
      --data-file=- >/dev/null
  elif [[ -z "${secret_value}" ]] && ! secret_has_enabled_version "${secret_name}"; then
    echo "Error: Secret Manager secret ${secret_name} has no enabled version." >&2
    return 1
  fi

  version="$(latest_enabled_secret_version "${secret_name}")"
  if [[ -z "${version}" ]]; then
    echo "Error: could not resolve an enabled version for secret ${secret_name}." >&2
    return 1
  fi

  resolve_runtime_service_account
  gcloud secrets add-iam-policy-binding "${secret_name}" \
    --project "${PROJECT_ID}" \
    --member="serviceAccount:${RUNTIME_SERVICE_ACCOUNT}" \
    --role="roles/secretmanager.secretAccessor" \
    --quiet >/dev/null

  printf '%s' "${version}"
}

resolve_runtime_service_account

SESSION_SECRET_VERSION="$(
  ensure_secret_value "${SESSION_SECRET_NAME}" "${AI_CO_SCIENTIST_SESSION_SECRET}"
)"

POSTGRES_SECRET_VERSION=""
if [[ "${PERSISTENCE_ENABLED}" == "true" ]]; then
  POSTGRES_SECRET_VERSION="$(
    ensure_secret_value "${POSTGRES_SECRET_NAME}" "${AI_CO_SCIENTIST_POSTGRES_DSN}"
  )"
fi

GOOGLE_SECRET_VERSION=""
if [[ "${USE_VERTEX_AI}" != "true" ]]; then
  GOOGLE_SECRET_VERSION="$(ensure_secret_value "${GOOGLE_SECRET_NAME}" "${GOOGLE_API_KEY}")"
fi
BIOGRID_SECRET_VERSION=""
if [[ -n "${BIOGRID_ACCESS_KEY}" ]] || secret_has_enabled_version "${BIOGRID_SECRET_NAME}"; then
  BIOGRID_SECRET_VERSION="$(ensure_secret_value "${BIOGRID_SECRET_NAME}" "${BIOGRID_ACCESS_KEY}")"
fi
BIOGRID_ORCS_SECRET_VERSION=""
if [[ -n "${BIOGRID_ORCS_ACCESS_KEY}" ]] || secret_has_enabled_version "${BIOGRID_ORCS_SECRET_NAME}"; then
  BIOGRID_ORCS_SECRET_VERSION="$(ensure_secret_value "${BIOGRID_ORCS_SECRET_NAME}" "${BIOGRID_ORCS_ACCESS_KEY}")"
fi

# ── Build ────────────────────────────────────────────────────────────────────

IMAGE_TAG="$(date +%Y%m%d-%H%M%S)"
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:${IMAGE_TAG}"

gcloud builds submit \
  --project "${PROJECT_ID}" \
  --tag "${IMAGE_URI}" \
  --suppress-logs \
  .

# ── Deploy ───────────────────────────────────────────────────────────────────

BQ_ALLOWLIST="bigquery-public-data.open_targets_platform,bigquery-public-data.ebi_chembl,bigquery-public-data.gnomAD,bigquery-public-data.fda_drug,bigquery-public-data.human_variant_annotation,bigquery-public-data.human_genome_variants,bigquery-public-data.umiami_lincs,bigquery-public-data.nlm_rxnorm,bigquery-public-data.ebi_surechembl"

join_with() {
  local delimiter="$1"
  shift
  local result=""
  local item=""
  for item in "$@"; do
    if [[ -n "${result}" ]]; then
      result+="${delimiter}"
    fi
    result+="${item}"
  done
  printf '%s' "${result}"
}

ENV_VAR_MAPPINGS=(
  "GOOGLE_CLOUD_PROJECT=${PROJECT_ID}"
  "GOOGLE_CLOUD_LOCATION=${REGION}"
  "BQ_PROJECT_ID=${PROJECT_ID}"
  "BQ_LOCATION=US"
  "ADK_NATIVE_PREFER_BIGQUERY=1"
  "RATE_LIMIT_TRUSTED_PROXY_HOPS=${RATE_LIMIT_TRUSTED_PROXY_HOPS}"
  "RATE_LIMIT_MAX_KEYS=${RATE_LIMIT_MAX_KEYS}"
  "ADK_MAX_RETAINED_COMPLETED_RUNS=${ADK_MAX_RETAINED_COMPLETED_RUNS}"
  "ADK_MAX_RETAINED_REPORTS=${ADK_MAX_RETAINED_REPORTS}"
  "BQ_DATASET_ALLOWLIST=${BQ_ALLOWLIST}"
)

SECRET_MAPPINGS=(
  "AI_CO_SCIENTIST_SESSION_SECRET=${SESSION_SECRET_NAME}:${SESSION_SECRET_VERSION}"
)

if [[ "${PERSISTENCE_ENABLED}" == "true" ]]; then
  SECRET_MAPPINGS+=(
    "AI_CO_SCIENTIST_POSTGRES_DSN=${POSTGRES_SECRET_NAME}:${POSTGRES_SECRET_VERSION}"
  )
fi

if [[ -n "${GA4_MEASUREMENT_ID}" ]]; then
  ENV_VAR_MAPPINGS+=("GA4_MEASUREMENT_ID=${GA4_MEASUREMENT_ID}")
fi

if [[ "${USE_VERTEX_AI}" == "true" ]]; then
  ENV_VAR_MAPPINGS+=("GOOGLE_GENAI_USE_VERTEXAI=true")
else
  ENV_VAR_MAPPINGS+=("GOOGLE_GENAI_USE_VERTEXAI=false")
  SECRET_MAPPINGS+=("GOOGLE_API_KEY=${GOOGLE_SECRET_NAME}:${GOOGLE_SECRET_VERSION}")
fi

if [[ -n "${BIOGRID_SECRET_VERSION}" ]]; then
  SECRET_MAPPINGS+=("BIOGRID_ACCESS_KEY=${BIOGRID_SECRET_NAME}:${BIOGRID_SECRET_VERSION}")
fi

if [[ -n "${BIOGRID_ORCS_SECRET_VERSION}" ]]; then
  SECRET_MAPPINGS+=("BIOGRID_ORCS_ACCESS_KEY=${BIOGRID_ORCS_SECRET_NAME}:${BIOGRID_ORCS_SECRET_VERSION}")
fi

DEPLOY_FLAGS=(
  --project "${PROJECT_ID}"
  --region "${REGION}"
  --image "${IMAGE_URI}"
  --platform managed
  --allow-unauthenticated
  --port 8080
  --cpu "${CPU}"
  --memory 4Gi
  --min-instances "${MIN_INSTANCES}"
  --max-instances "${MAX_INSTANCES}"
  --concurrency "${CONCURRENCY}"
  --service-account "${RUNTIME_SERVICE_ACCOUNT}"
  --cpu-throttling
  --timeout 900
  --set-env-vars "^||^$(join_with '||' "${ENV_VAR_MAPPINGS[@]}")"
  --set-secrets "$(join_with ',' "${SECRET_MAPPINGS[@]}")"
)

gcloud run deploy "${SERVICE_NAME}" "${DEPLOY_FLAGS[@]}"

# ── Output ───────────────────────────────────────────────────────────────────

SERVICE_URL="$(
  gcloud run services describe "${SERVICE_NAME}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --format='value(status.url)'
)"

echo ""
echo "Deployment complete."
echo "Service URL: ${SERVICE_URL}"
echo "Web UI: ${SERVICE_URL}/"
echo "Health endpoint: ${SERVICE_URL}/api/health"
echo "Query endpoint: ${SERVICE_URL}/api/query"
