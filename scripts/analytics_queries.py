from pathlib import Path
import duckdb

DB_PATH = Path("database/ecommerce.duckdb")


def main():
    con = duckdb.connect(str(DB_PATH))

    print("\n=== 1. Faturamento total por mês ===")
    query_1 = """
        SELECT
            order_month,
            ROUND(SUM(total_item_value), 2) AS total_revenue
        FROM analytics_sales
        GROUP BY order_month
        ORDER BY order_month
    """
    print(con.execute(query_1).fetchdf())

    print("\n=== 2. Faturamento por categoria ===")
    query_2 = """
        SELECT
            category,
            ROUND(SUM(total_item_value), 2) AS revenue
        FROM analytics_sales
        GROUP BY category
        ORDER BY revenue DESC
    """
    print(con.execute(query_2).fetchdf())

    print("\n=== 3. Quantidade de pedidos por estado ===")
    query_3 = """
        SELECT
            state,
            COUNT(DISTINCT order_id) AS total_orders
        FROM analytics_sales
        GROUP BY state
        ORDER BY total_orders DESC
    """
    print(con.execute(query_3).fetchdf())

    print("\n=== 4. Ticket médio por cliente ===")
    query_4 = """
        WITH customer_orders AS (
            SELECT
                customer_id,
                customer_name,
                order_id,
                SUM(total_item_value) AS order_total
            FROM analytics_sales
            GROUP BY customer_id, customer_name, order_id
        )
        SELECT
            customer_id,
            customer_name,
            ROUND(AVG(order_total), 2) AS avg_ticket
        FROM customer_orders
        GROUP BY customer_id, customer_name
        ORDER BY avg_ticket DESC
        LIMIT 20
    """
    print(con.execute(query_4).fetchdf())

    print("\n=== 5. Top 10 produtos mais vendidos ===")
    query_5 = """
        SELECT
            product_id,
            product_name,
            SUM(quantity) AS total_quantity_sold
        FROM analytics_sales
        GROUP BY product_id, product_name
        ORDER BY total_quantity_sold DESC
        LIMIT 10
    """
    print(con.execute(query_5).fetchdf())

    con.close()


if __name__ == "__main__":
    main()