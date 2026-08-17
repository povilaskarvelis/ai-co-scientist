from __future__ import annotations

import os
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parent
DEPLOY_SCRIPT = ROOT / "scripts" / "deploy_cloud_run.sh"
SESSION_SECRET = "0123456789abcdef0123456789abcdef"


def _write_fake_commands(tmp_path: Path) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_gcloud = bin_dir / "gcloud"
    fake_gcloud.write_text(
        """#!/usr/bin/env bash
set -euo pipefail

if [[ "$1 $2" == "secrets describe" ]]; then
  [[ "${FAKE_EXISTING_SECRETS:-false}" == "true" ]]
  exit
fi
if [[ "$1 $2 $3" == "secrets versions list" ]]; then
  echo "projects/test-project/secrets/test/versions/${FAKE_SECRET_VERSION:-3}"
  exit
fi
if [[ "$1 $2 $3" == "artifacts repositories describe" ]]; then
  exit 1
fi
if [[ "$1 $2 $3" == "run services describe" ]]; then
  if [[ "$*" == *"serviceAccountName"* ]]; then
    echo "runtime@test-project.iam.gserviceaccount.com"
  else
    echo "https://example.run.app"
  fi
  exit
fi
if [[ "$1 $2" == "run deploy" ]]; then
  printf 'DEPLOY_ARGS:%s\n' "$*"
fi
""",
        encoding="utf-8",
    )
    fake_gcloud.chmod(0o755)
    fake_openssl = bin_dir / "openssl"
    fake_openssl.write_text(
        f"#!/usr/bin/env bash\nprintf '%s\\n' '{SESSION_SECRET}'\n",
        encoding="utf-8",
    )
    fake_openssl.chmod(0o755)
    return bin_dir


def _run_deploy(tmp_path: Path, **overrides: str) -> subprocess.CompletedProcess[str]:
    bin_dir = _write_fake_commands(tmp_path)
    env = {
        "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}",
        "PROJECT_ID": "test-project",
        "USE_VERTEX_AI": "true",
    }
    env.update(overrides)
    return subprocess.run(
        ["bash", str(DEPLOY_SCRIPT)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def test_cloud_run_deploy_fails_before_build_without_durable_state(tmp_path):
    result = _run_deploy(
        tmp_path,
        AI_CO_SCIENTIST_SESSION_SECRET=SESSION_SECRET,
    )

    assert result.returncode != 0
    assert "durable Cloud Run persistence is not configured" in result.stdout
    assert "Using project=" not in result.stdout


def test_cloud_run_first_durable_deploy_binds_pinned_secrets(tmp_path):
    result = _run_deploy(
        tmp_path,
        AI_CO_SCIENTIST_POSTGRES_DSN="postgresql://user:password@example.invalid/database",
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.count("--set-env-vars") == 1
    assert result.stdout.count("--set-secrets") == 1
    assert "AI_CO_SCIENTIST_SESSION_SECRET=ai-co-scientist-session-secret:3" in result.stdout
    assert "AI_CO_SCIENTIST_POSTGRES_DSN=ai-co-scientist-postgres-dsn:3" in result.stdout
    assert "--service-account runtime@test-project.iam.gserviceaccount.com" in result.stdout


def test_cloud_run_deploy_reuses_existing_persistence_secrets(tmp_path):
    result = _run_deploy(
        tmp_path,
        FAKE_EXISTING_SECRETS="true",
        FAKE_SECRET_VERSION="7",
    )

    assert result.returncode == 0, result.stderr
    assert "Conversation persistence: Postgres" in result.stdout
    assert "ai-co-scientist-session-secret:7" in result.stdout
    assert "ai-co-scientist-postgres-dsn:7" in result.stdout


def test_cloud_run_ephemeral_state_requires_explicit_override(tmp_path):
    result = _run_deploy(
        tmp_path,
        AI_CO_SCIENTIST_SESSION_SECRET=SESSION_SECRET,
        ALLOW_EPHEMERAL_STATE="true",
        MAX_INSTANCES="2",
    )

    assert result.returncode == 0, result.stderr
    assert "Conversation persistence: ephemeral (explicit override)" in result.stdout
    assert "Ephemeral state requires MAX_INSTANCES=1" in result.stdout
    assert "--max-instances 1" in result.stdout
    assert "AI_CO_SCIENTIST_POSTGRES_DSN=" not in result.stdout


def test_cloud_run_durable_state_preserves_multi_instance_limit(tmp_path):
    result = _run_deploy(
        tmp_path,
        AI_CO_SCIENTIST_POSTGRES_DSN="postgresql://user:password@example.invalid/database",
        MAX_INSTANCES="2",
    )

    assert result.returncode == 0, result.stderr
    assert "Ephemeral state requires MAX_INSTANCES=1" not in result.stdout
    assert "--max-instances 2" in result.stdout
