from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"
SQL_DIR = BASE_DIR / "sql"
TERRAFORM_DIR = BASE_DIR / "terraform"
TASK2_BASE_DIR = (
    Path(__file__).resolve().parents[4] / "task_2" / "grupo_2" / "final"
)
TASK2_TERRAFORM_DIR = TASK2_BASE_DIR / "terraform"
DEFAULT_ENV = {
    "AWS_REGION": "us-east-1",
    "GLUE_DATABASE": "classicmodels_analytics",
    "ATHENA_WORKGROUP": "classicmodels-analytics",
    "ATHENA_RESULTS_PREFIX": "athena-results/",
}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def load_environment() -> None:
    if ENV_PATH.exists():
        load_dotenv(ENV_PATH)


def terraform_output(terraform_dir: Path, name: str) -> str | None:
    if not terraform_dir.exists():
        return None

    try:
        result = subprocess.run(
            ["terraform", f"-chdir={terraform_dir}", "output", "-json"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None

    try:
        outputs = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None

    payload = outputs.get(name)
    if not isinstance(payload, dict):
        return None

    value = payload.get("value")
    if value in (None, ""):
        return None
    return str(value)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value

    task3_outputs = {
        "DATA_LAKE_BUCKET": "data_lake_bucket",
        "GLUE_DATABASE": "glue_database_name",
        "ATHENA_WORKGROUP": "athena_workgroup_name",
        "ATHENA_OUTPUT_S3": "athena_output_location",
    }
    output_name = task3_outputs.get(name)
    if output_name:
        output_value = terraform_output(TERRAFORM_DIR, output_name)
        if output_value:
            return output_value

    if name == "DATA_LAKE_BUCKET":
        output_value = terraform_output(
            TASK2_TERRAFORM_DIR, "analytics_bucket_name"
        )
        if output_value:
            return output_value

    default_value = DEFAULT_ENV.get(name)
    if default_value:
        return default_value

    raise RuntimeError(f"Missing required environment variable: {name}")


def athena_output_s3() -> str:
    configured = require_env("ATHENA_OUTPUT_S3")
    return configured.rstrip("/") + "/"


def optional_env(name: str) -> str | None:
    value = os.getenv(name)
    if value:
        return value
    return DEFAULT_ENV.get(name)


def terraform_apply_env() -> dict[str, str]:
    env = os.environ.copy()
    env["TF_VAR_aws_region"] = require_env("AWS_REGION")
    env["TF_VAR_glue_database_name"] = require_env("GLUE_DATABASE")
    env["TF_VAR_athena_workgroup_name"] = require_env("ATHENA_WORKGROUP")
    env["TF_VAR_data_lake_bucket"] = require_env("DATA_LAKE_BUCKET")
    env["TF_VAR_athena_results_prefix"] = (
        optional_env("ATHENA_RESULTS_PREFIX") or "athena-results/"
    )
    return env


def sql_text(filename: str) -> str:
    return (SQL_DIR / filename).read_text(encoding="utf-8").strip()
