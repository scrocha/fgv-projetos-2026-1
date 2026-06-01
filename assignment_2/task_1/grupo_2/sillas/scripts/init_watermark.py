import logging

from common import configure_logging, get_db_connection, load_environment


def init_watermark():
    configure_logging()
    load_environment()

    logger = logging.getLogger(__name__)

    table_ddl = """
    CREATE TABLE IF NOT EXISTS etl_watermark (
        pipeline_name VARCHAR(64) PRIMARY KEY,
        last_processed_order_date DATE,
        last_run_at DATETIME,
        last_run_status VARCHAR(32)
    );
    """

    # Busca a data máxima atual dos pedidos
    get_max_date_sql = "SELECT MAX(orderDate) FROM orders;"

    # Upsert do registro de controle
    upsert_sql = """
    INSERT INTO etl_watermark (pipeline_name, last_processed_order_date, last_run_at, last_run_status)
    VALUES (%s, %s, NOW(), 'NEVER_RUN')
    ON DUPLICATE KEY UPDATE 
        last_processed_order_date = VALUES(last_processed_order_date);
    """

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            logger.info("Criando tabela etl_watermark (se não existir)...")
            cursor.execute(table_ddl)

            logger.info("Buscando a data máxima de pedidos atual...")
            cursor.execute(get_max_date_sql)
            max_date = cursor.fetchone()[0]

            if max_date is None:
                logger.warning(
                    "Nenhum pedido encontrado na tabela 'orders'. Inicializando com None."
                )
            else:
                logger.info(f"Data máxima encontrada: {max_date}")

            logger.info("Inicializando/Atualizando registro de watermark...")
            cursor.execute(upsert_sql, ("classicmodels_sales", max_date))

            conn.commit()
            logger.info("Watermark inicializado com sucesso!")

    except Exception as e:
        logger.error(f"Erro ao inicializar watermark: {e}")
        raise


if __name__ == "__main__":
    init_watermark()
