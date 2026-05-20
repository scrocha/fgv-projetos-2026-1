from __future__ import annotations

import logging
import sys
from dataclasses import dataclass

import awswrangler as wr
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from common import (
    athena_output_s3,
    configure_logging,
    load_environment,
    require_env,
)


@dataclass(frozen=True)
class TableSpec:
    name: str
    location_suffix: str


TABLE_SPECS = [
    TableSpec(name="fact_orders", location_suffix="analytics/fact_orders/"),
    TableSpec(
        name="dim_customers", location_suffix="analytics/dim_customers/"
    ),
    TableSpec(name="dim_products", location_suffix="analytics/dim_products/"),
    TableSpec(name="dim_dates", location_suffix="analytics/dim_dates/"),
    TableSpec(
        name="dim_countries", location_suffix="analytics/dim_countries/"
    ),
]


def run_ddl(sql: str, database: str, session: boto3.Session) -> None:
    wr.athena.start_query_execution(
        sql=sql,
        database=database,
        boto3_session=session,
        workgroup=require_env("ATHENA_WORKGROUP"),
        s3_output=athena_output_s3(),
        wait=True,
    )


def ensure_database(database: str, session: boto3.Session) -> None:
    logging.info("Garantindo database %s no Glue/Athena", database)
    run_ddl(
        f"CREATE DATABASE IF NOT EXISTS {database}",
        database="default",
        session=session,
    )


def first_parquet_key(s3_client, bucket: str, prefix: str) -> str:
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for item in response.get("Contents", []):
        key = item["Key"]
        if key.endswith(".parquet"):
            return key
    raise RuntimeError(
        f"Nenhum arquivo Parquet encontrado em s3://{bucket}/{prefix}"
    )


def athena_type(field_type: pa.DataType) -> str:
    if pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
        return "string"
    if (
        pa.types.is_int8(field_type)
        or pa.types.is_int16(field_type)
        or pa.types.is_int32(field_type)
    ):
        return "int"
    if pa.types.is_int64(field_type):
        return "bigint"
    if (
        pa.types.is_uint8(field_type)
        or pa.types.is_uint16(field_type)
        or pa.types.is_uint32(field_type)
    ):
        return "bigint"
    if pa.types.is_uint64(field_type):
        return "decimal(20,0)"
    if pa.types.is_float32(field_type):
        return "float"
    if pa.types.is_float64(field_type):
        return "double"
    if pa.types.is_boolean(field_type):
        return "boolean"
    if pa.types.is_date32(field_type) or pa.types.is_date64(field_type):
        return "date"
    if pa.types.is_timestamp(field_type):
        return "timestamp"
    if pa.types.is_decimal(field_type):
        return f"decimal({field_type.precision},{field_type.scale})"
    raise RuntimeError(f"Tipo Parquet nao suportado para Athena: {field_type}")


def infer_columns_sql(s3_client, bucket: str, prefix: str) -> str:
    key = first_parquet_key(s3_client, bucket, prefix)
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    schema = pq.read_schema(pa.BufferReader(body))

    columns: list[str] = []
    for field in schema:
        columns.append(f"{field.name} {athena_type(field.type)}")
    return ",\n        ".join(columns)


def ensure_table(
    database: str, bucket: str, session: boto3.Session, spec: TableSpec
) -> None:
    s3_client = session.client("s3")
    location = f"s3://{bucket}/{spec.location_suffix}"
    columns_sql = infer_columns_sql(s3_client, bucket, spec.location_suffix)

    logging.info("Recriando tabela %s em %s", spec.name, location)
    run_ddl(
        f"DROP TABLE IF EXISTS {database}.{spec.name}",
        database=database,
        session=session,
    )

    ddl = f"""
    CREATE EXTERNAL TABLE {database}.{spec.name} (
        {columns_sql}
    )
    STORED AS PARQUET
    LOCATION '{location}'
    """
    run_ddl(ddl, database=database, session=session)


def main() -> int:
    configure_logging()
    load_environment()

    region = require_env("AWS_REGION")
    database = require_env("GLUE_DATABASE")
    bucket = require_env("DATA_LAKE_BUCKET")
    session = boto3.Session(region_name=region)

    ensure_database(database, session)
    for spec in TABLE_SPECS:
        ensure_table(database, bucket, session, spec)

    logging.info("Catalogo Athena/Glue configurado com sucesso")
    return 0


if __name__ == "__main__":
    sys.exit(main())
