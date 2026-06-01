import argparse
import logging
import random
from datetime import datetime, timedelta

from common import configure_logging, get_db_connection, load_environment


def simulate_new_orders(count: int, seed: int | None = None):
    configure_logging()
    load_environment()
    logger = logging.getLogger(__name__)

    if seed is not None:
        random.seed(seed)
        logger.info(f"Usando seed: {seed}")

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor(dictionary=True)

            # 1. Pegar referências válidas
            cursor.execute("SELECT customerNumber FROM customers LIMIT 50;")
            customers = [r["customerNumber"] for r in cursor.fetchall()]

            cursor.execute(
                "SELECT productCode, buyPrice FROM products LIMIT 50;"
            )
            products = cursor.fetchall()

            # 2. Pegar a data máxima atual e o último orderNumber
            cursor.execute("SELECT MAX(orderDate) as max_date, MAX(orderNumber) as max_number FROM orders;")
            result = cursor.fetchone()
            last_date = result["max_date"]
            max_order_number = result["max_number"]

            if not last_date:
                last_date = datetime.now().date()
            if not max_order_number:
                max_order_number = 10000

            logger.info(
                f"Iniciando simulação de {count} pedidos a partir de {last_date} (último ID: {max_order_number})"
            )

            order_ids = []
            total_details = 0

            for i in range(1, count + 1):
                # Como a tabela orders do classicmodels não possui AUTO_INCREMENT,
                # geramos o orderNumber de forma manual e incremental.
                order_number = max_order_number + i
                
                # Data incremental (1 dia após o último ou anterior simulado)
                new_date = last_date + timedelta(days=i)
                customer = random.choice(customers)

                # Mock status e datas adicionais
                # requiredDate é +7 dias, shippedDate é +2 dias (simplificado)
                required_date = new_date + timedelta(days=7)
                shipped_date = new_date + timedelta(days=2)

                # Inserir Order
                cursor.execute(
                    """
                    INSERT INTO orders (orderNumber, orderDate, requiredDate, shippedDate, status, customerNumber)
                    VALUES (%s, %s, %s, %s, 'Shipped', %s)
                """,
                    (order_number, new_date, required_date, shipped_date, customer),
                )

                order_ids.append(order_number)

                # Inserir OrderDetails (1 a 3 itens por pedido)
                num_items = random.randint(1, 3)
                for line in range(1, num_items + 1):
                    prod = random.choice(products)
                    qty = random.randint(1, 10)
                    price = float(prod["buyPrice"]) * 1.2  # markup simulado

                    cursor.execute(
                        """
                        INSERT INTO orderdetails (orderNumber, productCode, quantityOrdered, priceEach, orderLineNumber)
                        VALUES (%s, %s, %s, %s, %s)
                    """,
                        (order_number, prod["productCode"], qty, price, line),
                    )
                    total_details += 1

            conn.commit()

            logger.info("--- Resumo da Simulação ---")
            logger.info(
                f"Pedidos criados: {len(order_ids)} ({min(order_ids)} a {max(order_ids)})"
            )
            logger.info(
                f"Faixa de datas: {last_date + timedelta(days=1)} até {new_date}"
            )
            logger.info(f"Total de linhas em orderdetails: {total_details}")

    except Exception as e:
        logger.error(f"Erro na simulação: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simula a criação de novos pedidos no ClassicModels."
    )
    parser.add_argument(
        "--count", type=int, default=5, help="Número de pedidos a criar"
    )
    parser.add_argument(
        "--seed", type=int, help="Semente para geração aleatória"
    )
    args = parser.parse_args()

    simulate_new_orders(args.count, args.seed)
