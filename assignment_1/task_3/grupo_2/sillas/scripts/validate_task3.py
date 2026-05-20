from __future__ import annotations

import logging
import sys

from analytics_dashboard import (
    build_session,
    query_detailed_sales,
    query_dim_products,
    query_sales_by_country,
)
from common import configure_logging, load_environment, require_env


def main() -> int:
    configure_logging()
    load_environment()

    database = require_env("GLUE_DATABASE")
    session = build_session()

    logging.info("Executando consulta exploratoria em dim_products")
    dim_products_df = query_dim_products(database, session)
    if dim_products_df.empty:
        logging.error("A consulta em dim_products retornou zero linhas")
        return 1
    logging.info("dim_products retornou %s linhas", len(dim_products_df))

    logging.info("Executando consulta de vendas totais por pais")
    sales_by_country_df = query_sales_by_country(database, session)
    if sales_by_country_df.empty:
        logging.error("A consulta de vendas por pais retornou zero linhas")
        return 1
    logging.info(
        "Consulta de vendas por pais retornou %s linhas",
        len(sales_by_country_df),
    )

    logging.info("Executando consulta detalhada para o dashboard")
    detailed_sales_df = query_detailed_sales(database, session)
    if detailed_sales_df.empty:
        logging.error("A consulta detalhada retornou zero linhas")
        return 1
    logging.info(
        "Consulta detalhada retornou %s linhas", len(detailed_sales_df)
    )

    logging.info(
        "Consultas Athena validadas com sucesso no database %s", database
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
