import logging
import sys

from common import configure_logging, get_db_connection, load_environment


def validate():
    configure_logging()
    load_environment()
    logger = logging.getLogger(__name__)

    success = True

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor(dictionary=True)

            # 1. Verifica etl_watermark
            logger.info("Checando tabela etl_watermark")
            cursor.execute(
                "SELECT * FROM etl_watermark WHERE pipeline_name = 'classicmodels_sales';"
            )
            watermark = cursor.fetchone()

            if not watermark:
                logger.error(
                    "Registro 'classicmodels_sales' não encontrado em etl_watermark."
                )
                success = False
            else:
                logger.info(
                    f"Watermark encontrado: {watermark['last_processed_order_date']}"
                )
                if watermark["last_processed_order_date"] is None:
                    logger.error(
                        "Erro: last_processed_order_date está NULL. A tabela de watermark deve ser inicializada com dados históricos."
                    )
                    success = False

            # 2. Verifica se há dados novos
            cursor.execute("SELECT MAX(orderDate) as max_date FROM orders;")
            max_order_date = cursor.fetchone()["max_date"]

            if watermark and watermark["last_processed_order_date"] is not None and max_order_date:
                if max_order_date > watermark["last_processed_order_date"]:
                    logger.info(
                        f"Sucesso: Há dados novos pendentes! (Max orders: {max_order_date} > Watermark: {watermark['last_processed_order_date']})"
                    )
                elif max_order_date == watermark["last_processed_order_date"]:
                    logger.info(
                        f"Status: Não há dados novos em relação ao watermark (Ambos: {max_order_date})."
                    )
                else:
                    logger.error(
                        f"Erro: Anomalia detectada! A data máxima de pedidos ({max_order_date}) é menor que o watermark ({watermark['last_processed_order_date']})."
                    )
                    success = False

            # 3. Verifica integridade (pedidos sem itens)
            cursor.execute("""
                SELECT COUNT(*) as hollow_orders 
                FROM orders o 
                LEFT JOIN orderdetails od ON o.orderNumber = od.orderNumber 
                WHERE od.orderNumber IS NULL;
            """)
            hollow = cursor.fetchone()["hollow_orders"]
            if hollow > 0:
                logger.error(
                    f"Integridade falhou: {hollow} pedidos não possuem itens em orderdetails."
                )
                success = False
            else:
                logger.info("Integridade de pedidos e itens ok.")

    except Exception as e:
        logger.error(f"Erro durante a validação: {e}")
        success = False

    if not success:
        logger.error("Validação falhou!")
        sys.exit(1)

    logger.info("Validação concluída com sucesso!")
    sys.exit(0)


if __name__ == "__main__":
    validate()
