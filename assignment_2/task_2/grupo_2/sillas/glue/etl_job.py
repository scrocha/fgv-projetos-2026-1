from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from py4j.java_gateway import java_import
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PIPELINE_NAME = "classicmodels_sales"


def read_table(glue_context: GlueContext, jdbc_url: str, dbtable: str, user: str, password: str) -> DataFrame:
    return (
        glue_context.spark_session.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", dbtable)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
    )


def execute_sql(spark: SparkSession, jdbc_url: str, user: str, password: str, sql: str) -> None:
    java_import(spark._sc._gateway.jvm, "java.sql.DriverManager")
    connection = spark._sc._gateway.jvm.DriverManager.getConnection(jdbc_url, user, password)
    try:
        statement = connection.createStatement()
        try:
            statement.execute(sql)
        finally:
            statement.close()
    finally:
        connection.close()


def mark_failed(spark: SparkSession, jdbc_url: str, user: str, password: str) -> None:
    execute_sql(
        spark,
        jdbc_url,
        user,
        password,
        f"""
        UPDATE etl_watermark
        SET last_run_at = UTC_TIMESTAMP(),
            last_run_status = 'FAILED'
        WHERE pipeline_name = '{PIPELINE_NAME}'
        """,
    )


def advance_watermark(spark: SparkSession, jdbc_url: str, user: str, password: str, max_order_date: str) -> None:
    execute_sql(
        spark,
        jdbc_url,
        user,
        password,
        f"""
        UPDATE etl_watermark
        SET last_processed_order_date = DATE('{max_order_date}'),
            last_run_at = UTC_TIMESTAMP(),
            last_run_status = 'SUCCEEDED'
        WHERE pipeline_name = '{PIPELINE_NAME}'
        """,
    )


def mark_succeeded_without_delta(spark: SparkSession, jdbc_url: str, user: str, password: str) -> None:
    execute_sql(
        spark,
        jdbc_url,
        user,
        password,
        f"""
        UPDATE etl_watermark
        SET last_run_at = UTC_TIMESTAMP(),
            last_run_status = 'SUCCEEDED'
        WHERE pipeline_name = '{PIPELINE_NAME}'
        """,
    )


def s3_path_exists(spark: SparkSession, path: str) -> bool:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    uri = spark._sc._gateway.jvm.java.net.URI(path)
    fs = spark._sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    return fs.exists(spark._sc._gateway.jvm.org.apache.hadoop.fs.Path(path))


def write_parquet(df: DataFrame, bucket: str, prefix: str, table_name: str) -> None:
    output_path = f"s3://{bucket}/{prefix}/{table_name}/"
    df.write.mode("overwrite").parquet(output_path)


def write_fact_delta(spark: SparkSession, fact_delta: DataFrame, bucket: str, prefix: str) -> None:
    fact_path = f"s3://{bucket}/{prefix}/fact_orders/"
    partition_cols = ["order_year", "order_month"]
    key_cols = ["order_id", "product_id"]

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if s3_path_exists(spark, fact_path):
        touched = fact_delta.select(*partition_cols).dropDuplicates()
        existing_touched = spark.read.parquet(fact_path).join(touched, on=partition_cols, how="inner")
        existing_without_delta = existing_touched.join(fact_delta.select(*key_cols), on=key_cols, how="left_anti")
        merged = existing_without_delta.unionByName(fact_delta).cache()
    else:
        merged = fact_delta.cache()

    merged.count()
    merged.write.mode("overwrite").partitionBy(*partition_cols).parquet(fact_path)


def require_non_empty(df: DataFrame, table_name: str) -> None:
    if df.limit(1).count() == 0:
        raise RuntimeError(f"{table_name} is empty")


def ensure_no_orphans(
    fact_df: DataFrame,
    dim_customers: DataFrame,
    dim_products: DataFrame,
    dim_dates: DataFrame,
    dim_countries: DataFrame,
) -> None:
    customer_orphans = fact_df.join(dim_customers.select("customer_id"), on="customer_id", how="left_anti").count()
    product_orphans = fact_df.join(dim_products.select("product_id"), on="product_id", how="left_anti").count()
    date_orphans = fact_df.join(
        dim_dates.select("date_key"),
        fact_df.order_date_key == dim_dates.date_key,
        "left_anti",
    ).count()
    country_orphans = fact_df.join(dim_countries.select("country_key"), on="country_key", how="left_anti").count()

    if any([customer_orphans, product_orphans, date_orphans, country_orphans]):
        raise RuntimeError(
            "Referential integrity validation failed: "
            f"customer_orphans={customer_orphans}, "
            f"product_orphans={product_orphans}, "
            f"date_orphans={date_orphans}, "
            f"country_orphans={country_orphans}"
        )


def ensure_sales_amount(df: DataFrame) -> None:
    invalid_rows = (
        df.filter(
            F.col("sales_amount")
            != F.round(F.col("quantity_ordered").cast("double") * F.col("price_each").cast("double"), 2)
        ).count()
    )
    if invalid_rows > 0:
        raise RuntimeError(f"sales_amount validation failed for {invalid_rows} rows")


def build_star_schema(glue_context: GlueContext, jdbc_url: str, user: str, password: str) -> tuple[DataFrame, ...]:
    watermark = read_table(
        glue_context,
        jdbc_url,
        f"(SELECT last_processed_order_date FROM etl_watermark WHERE pipeline_name = '{PIPELINE_NAME}') wm",
        user,
        password,
    ).collect()
    if not watermark or watermark[0]["last_processed_order_date"] is None:
        raise RuntimeError(f"Watermark {PIPELINE_NAME} missing or NULL. Run scripts/init_watermark.py first.")

    last_processed_order_date = watermark[0]["last_processed_order_date"].isoformat()

    orders_delta = read_table(
        glue_context,
        jdbc_url,
        f"(SELECT * FROM orders WHERE orderDate > DATE('{last_processed_order_date}')) orders_delta",
        user,
        password,
    )
    orderdetails = read_table(glue_context, jdbc_url, "orderdetails", user, password)
    customers = read_table(glue_context, jdbc_url, "customers", user, password)
    products = read_table(glue_context, jdbc_url, "products", user, password)
    productlines = read_table(glue_context, jdbc_url, "productlines", user, password)
    employees = read_table(glue_context, jdbc_url, "employees", user, password)
    offices = read_table(glue_context, jdbc_url, "offices", user, password)
    all_orders = read_table(glue_context, jdbc_url, "orders", user, password)

    if orders_delta.limit(1).count() == 0:
        return orders_delta, None, None, None, None, None

    customer_locations = (
        customers.alias("c")
        .join(
            employees.select(
                F.col("employeeNumber").alias("employee_number"),
                F.col("officeCode").alias("office_code"),
            ).alias("e"),
            F.col("c.salesRepEmployeeNumber") == F.col("e.employee_number"),
            "left",
        )
        .join(
            offices.select(
                F.col("officeCode").alias("office_code"),
                F.col("territory").alias("territory"),
            ).alias("o"),
            F.col("e.office_code") == F.col("o.office_code"),
            "left",
        )
        .select(
            F.col("c.customerNumber").alias("customer_id"),
            F.col("c.customerName").alias("customer_name"),
            F.concat_ws(" ", F.col("c.contactFirstName"), F.col("c.contactLastName")).alias("contact_name"),
            F.col("c.city").alias("city"),
            F.trim(F.col("c.country")).alias("country"),
            F.coalesce(F.col("o.territory"), F.lit("Unknown")).alias("territory"),
        )
    )

    dim_customers = customer_locations.select(
        "customer_id",
        "customer_name",
        "contact_name",
        "city",
        "country",
    ).dropDuplicates(["customer_id"])

    dim_products = (
        products.alias("p")
        .join(
            productlines.select(F.col("productLine").alias("product_line")).alias("pl"),
            F.col("p.productLine") == F.col("pl.product_line"),
            "left",
        )
        .select(
            F.col("p.productCode").alias("product_id"),
            F.col("p.productName").alias("product_name"),
            F.coalesce(F.col("pl.product_line"), F.col("p.productLine")).alias("product_line"),
            F.col("p.productVendor").alias("product_vendor"),
        )
        .dropDuplicates(["product_id"])
    )

    dim_dates = (
        all_orders.select(F.col("orderDate").alias("full_date"))
        .dropDuplicates(["full_date"])
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .select("date_key", "full_date", "year", "quarter", "month", "day")
    )

    dim_countries = (
        customer_locations.select("country", "territory")
        .dropDuplicates(["country", "territory"])
        .withColumn("country_key", F.sha2(F.concat_ws("|", F.col("country"), F.col("territory")), 256))
        .select("country_key", "country", "territory")
    )

    fact_delta = (
        orders_delta.alias("o")
        .join(orderdetails.alias("od"), F.col("o.orderNumber") == F.col("od.orderNumber"), "inner")
        .join(customer_locations.alias("cl"), F.col("o.customerNumber") == F.col("cl.customer_id"), "inner")
        .select(
            F.col("o.orderNumber").alias("order_id"),
            F.col("o.customerNumber").alias("customer_id"),
            F.col("od.productCode").alias("product_id"),
            F.date_format(F.col("o.orderDate"), "yyyyMMdd").cast("int").alias("order_date_key"),
            F.sha2(F.concat_ws("|", F.col("cl.country"), F.col("cl.territory")), 256).alias("country_key"),
            F.col("od.quantityOrdered").alias("quantity_ordered"),
            F.round(F.col("od.priceEach"), 2).alias("price_each"),
            F.round(F.col("od.quantityOrdered") * F.col("od.priceEach"), 2).alias("sales_amount"),
            F.year(F.col("o.orderDate")).alias("order_year"),
            F.month(F.col("o.orderDate")).alias("order_month"),
        )
    )

    max_order_date = orders_delta.agg(F.max("orderDate").cast("string").alias("max_order_date")).collect()[0][
        "max_order_date"
    ]
    return fact_delta, dim_customers, dim_products, dim_dates, dim_countries, max_order_date


def run() -> None:
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "db_host",
            "db_port",
            "db_name",
            "db_user",
            "db_password",
            "output_bucket",
            "output_prefix",
        ],
    )

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    jdbc_url = f"jdbc:mysql://{args['db_host']}:{args['db_port']}/{args['db_name']}"
    user = args["db_user"]
    password = args["db_password"]

    try:
        fact_delta, dim_customers, dim_products, dim_dates, dim_countries, max_order_date = build_star_schema(
            glue_context,
            jdbc_url,
            user,
            password,
        )

        if fact_delta.limit(1).count() == 0:
            mark_succeeded_without_delta(spark, jdbc_url, user, password)
            job.commit()
            return

        require_non_empty(fact_delta, "fact_orders delta")
        require_non_empty(dim_customers, "dim_customers")
        require_non_empty(dim_products, "dim_products")
        require_non_empty(dim_dates, "dim_dates")
        require_non_empty(dim_countries, "dim_countries")

        ensure_no_orphans(fact_delta, dim_customers, dim_products, dim_dates, dim_countries)
        ensure_sales_amount(fact_delta)

        write_fact_delta(spark, fact_delta, args["output_bucket"], args["output_prefix"])
        write_parquet(dim_customers, args["output_bucket"], args["output_prefix"], "dim_customers")
        write_parquet(dim_products, args["output_bucket"], args["output_prefix"], "dim_products")
        write_parquet(dim_dates, args["output_bucket"], args["output_prefix"], "dim_dates")
        write_parquet(dim_countries, args["output_bucket"], args["output_prefix"], "dim_countries")

        advance_watermark(spark, jdbc_url, user, password, max_order_date)
    except Exception:
        mark_failed(spark, jdbc_url, user, password)
        raise

    job.commit()


if __name__ == "__main__":
    run()
