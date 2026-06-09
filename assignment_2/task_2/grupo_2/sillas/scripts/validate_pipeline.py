from __future__ import annotations

import logging
import sys

import awswrangler as wr
import boto3
import mysql.connector
import pandas as pd

from common import configure_logging, load_environment, require_env


EXPECTED_PREFIXES = [
    "analytics/fact_orders/",
    "analytics/dim_customers/",
    "analytics/dim_products/",
    "analytics/dim_dates/",
    "analytics/dim_countries/",
]
PARTITION_PREFIX = "analytics/fact_orders/order_year="


def latest_job_state(glue_client, job_name: str) -> str:
    response = glue_client.get_job_runs(JobName=job_name, MaxResults=1)
    runs = response.get("JobRuns", [])
    if not runs:
        raise RuntimeError(f"Nenhuma execucao encontrada para o job {job_name}")
    return runs[0]["JobRunState"]


def prefix_has_parquet_files(s3_client, bucket: str, prefix: str) -> bool:
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for item in response.get("Contents", []):
        if item["Key"].endswith(".parquet"):
            return True
    return False


def fact_has_partitioned_files(s3_client, bucket: str) -> bool:
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=PARTITION_PREFIX)
    for item in response.get("Contents", []):
        key = item["Key"]
        if "/order_month=" in key and key.endswith(".parquet"):
            return True
    return False


def validate_watermark() -> list[str]:
    failures: list[str] = []
    connection = mysql.connector.connect(
        host=require_env("DB_HOST"),
        port=int(require_env("DB_PORT")),
        user=require_env("DB_USER"),
        password=require_env("DB_PASSWORD"),
        database=require_env("DB_NAME"),
    )
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT last_processed_order_date, last_run_at, last_run_status
            FROM etl_watermark
            WHERE pipeline_name = 'classicmodels_sales'
            """
        )
        row = cursor.fetchone()
    finally:
        connection.close()

    if not row:
        failures.append("Registro etl_watermark classicmodels_sales nao encontrado")
        return failures

    if row["last_processed_order_date"] is None:
        failures.append("Watermark last_processed_order_date esta NULL")
    if row["last_run_status"] != "SUCCEEDED":
        failures.append(f"Watermark last_run_status esperado SUCCEEDED, recebido {row['last_run_status']}")
    if row["last_run_at"] is None:
        failures.append("Watermark last_run_at esta NULL")

    logging.info(
        "Watermark: last_processed_order_date=%s last_run_status=%s",
        row["last_processed_order_date"],
        row["last_run_status"],
    )
    return failures


def validate_fact_integrity(base_path: str) -> list[str]:
    failures: list[str] = []

    fact_orders = wr.s3.read_parquet(f"{base_path}/fact_orders/")
    dim_customers = wr.s3.read_parquet(f"{base_path}/dim_customers/")
    dim_products = wr.s3.read_parquet(f"{base_path}/dim_products/")
    dim_dates = wr.s3.read_parquet(f"{base_path}/dim_dates/")
    dim_countries = wr.s3.read_parquet(f"{base_path}/dim_countries/")

    if fact_orders.empty:
        failures.append("Tabela fact_orders esta vazia")
        return failures

    logging.info("fact_orders possui %s registros", len(fact_orders))

    if "order_year" not in fact_orders.columns or "order_month" not in fact_orders.columns:
        failures.append("fact_orders nao possui colunas de particao order_year/order_month")
    else:
        invalid_months = fact_orders[~fact_orders["order_month"].between(1, 12)]
        if invalid_months.empty:
            logging.info("Particoes order_year/order_month presentes e validas")
        else:
            failures.append(f"Encontradas {len(invalid_months)} linhas com order_month invalido")

    invalid_customers = set(fact_orders["customer_id"]) - set(dim_customers["customer_id"])
    if invalid_customers:
        failures.append(f"fact_orders possui {len(invalid_customers)} customer_id orfaos")
    else:
        logging.info("Integridade OK para customer_id")

    invalid_products = set(fact_orders["product_id"]) - set(dim_products["product_id"])
    if invalid_products:
        failures.append(f"fact_orders possui {len(invalid_products)} product_id orfaos")
    else:
        logging.info("Integridade OK para product_id")

    invalid_dates = set(fact_orders["order_date_key"]) - set(dim_dates["date_key"])
    if invalid_dates:
        failures.append(f"fact_orders possui {len(invalid_dates)} order_date_key orfaos")
    else:
        logging.info("Integridade OK para order_date_key")

    invalid_countries = set(fact_orders["country_key"]) - set(dim_countries["country_key"])
    if invalid_countries:
        failures.append(f"fact_orders possui {len(invalid_countries)} country_key orfaos")
    else:
        logging.info("Integridade OK para country_key")

    fact_orders["quantity_ordered"] = pd.to_numeric(fact_orders["quantity_ordered"])
    fact_orders["price_each"] = pd.to_numeric(fact_orders["price_each"])
    fact_orders["sales_amount"] = pd.to_numeric(fact_orders["sales_amount"])

    recalculated_sales = (fact_orders["quantity_ordered"] * fact_orders["price_each"]).round(2)
    stored_sales = fact_orders["sales_amount"].round(2)
    invalid_sales = fact_orders[stored_sales != recalculated_sales]

    if invalid_sales.empty:
        logging.info("Regra de negocio OK para sales_amount")
    else:
        failures.append(f"Foram encontradas {len(invalid_sales)} inconsistencias em sales_amount")

    return failures


def main() -> int:
    configure_logging()
    load_environment()

    region = require_env("AWS_REGION")
    bucket = require_env("S3_BUCKET_NAME")
    job_name = require_env("GLUE_JOB_NAME")
    base_path = f"s3://{bucket}/analytics"

    glue = boto3.client("glue", region_name=region)
    s3 = boto3.client("s3", region_name=region)

    state = latest_job_state(glue, job_name)
    logging.info("Ultimo estado do Glue job: %s", state)
    if state != "SUCCEEDED":
        logging.error("O Glue job nao finalizou com SUCCEEDED")
        return 1

    failures: list[str] = []
    for prefix in EXPECTED_PREFIXES:
        if not prefix_has_parquet_files(s3, bucket, prefix):
            failures.append(f"Prefixo sem arquivos Parquet: s3://{bucket}/{prefix}")
        else:
            logging.info("Saida validada em s3://%s/%s", bucket, prefix)

    if not fact_has_partitioned_files(s3, bucket):
        failures.append("fact_orders sem arquivos particionados em order_year/order_month")
    else:
        logging.info("Particionamento Hive validado em s3://%s/%s", bucket, PARTITION_PREFIX)

    failures.extend(validate_fact_integrity(base_path))
    failures.extend(validate_watermark())

    if failures:
        for failure in failures:
            logging.error(failure)
        return 1

    logging.info("Validacao do pipeline concluida com sucesso")
    return 0


if __name__ == "__main__":
    sys.exit(main())
