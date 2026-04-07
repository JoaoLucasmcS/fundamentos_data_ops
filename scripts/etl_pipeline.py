from pathlib import Path
import pandas as pd
import duckdb

RAW_PATH = Path("data/raw")
DB_PATH = Path("database/ecommerce.duckdb")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def extract():
    customers = pd.read_csv(RAW_PATH / "customers.csv")
    products = pd.read_csv(RAW_PATH / "products.csv")
    orders = pd.read_csv(RAW_PATH / "orders.csv")
    order_items = pd.read_csv(RAW_PATH / "order_items.csv")

    return customers, products, orders, order_items


def transform(customers, products, orders, order_items):

    # -----------------------------
    # 1. Ajuste de tipos
    # -----------------------------
    customers["customer_id"] = customers["customer_id"].astype(int)
    customers["customer_name"] = customers["customer_name"].astype(str)
    customers["city"] = customers["city"].astype(str)
    customers["state"] = customers["state"].astype(str)
    customers["signup_date"] = pd.to_datetime(customers["signup_date"], errors="coerce")

    products["product_id"] = products["product_id"].astype(int)
    products["product_name"] = products["product_name"].astype(str)
    products["category"] = products["category"].astype(str)
    products["price"] = pd.to_numeric(products["price"], errors="coerce")

    orders["order_id"] = orders["order_id"].astype(int)
    orders["customer_id"] = orders["customer_id"].astype(int)
    orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")
    orders["status"] = orders["status"].astype(str)

    order_items["order_item_id"] = order_items["order_item_id"].astype(int)
    order_items["order_id"] = order_items["order_id"].astype(int)
    order_items["product_id"] = order_items["product_id"].astype(int)
    order_items["quantity"] = pd.to_numeric(order_items["quantity"], errors="coerce")
    order_items["unit_price"] = pd.to_numeric(order_items["unit_price"], errors="coerce")

    # -----------------------------
    # 2. Tratamento de nulos
    # -----------------------------
    customers["customer_name"] = customers["customer_name"].fillna("Desconhecido")
    customers["city"] = customers["city"].fillna("Desconhecida")
    customers["state"] = customers["state"].fillna("ND")

    products["product_name"] = products["product_name"].fillna("Produto sem nome")
    products["category"] = products["category"].fillna("Sem categoria")

    order_items["quantity"] = order_items["quantity"].fillna(1)
    order_items["unit_price"] = order_items["unit_price"].fillna(0)

    # Remove linhas onde a data ficou inválida
    customers = customers.dropna(subset=["signup_date"])
    orders = orders.dropna(subset=["order_date"])
    products = products.dropna(subset=["price"])

    # -----------------------------
    # 3. Cálculo do valor total do item
    # -----------------------------
    order_items["total_item_value"] = order_items["quantity"] * order_items["unit_price"]

    # -----------------------------
    # 4. Base tratada intermediária
    # -----------------------------
    trusted_order_items = order_items.copy()

    # -----------------------------
    # 5. Junção entre tabelas
    # -----------------------------
    analytics_sales = order_items.merge(
        orders,
        on="order_id",
        how="left"
    ).merge(
        products,
        on="product_id",
        how="left"
    ).merge(
        customers,
        on="customer_id",
        how="left"
    )

    # -----------------------------
    # 6. Colunas auxiliares para análise
    # -----------------------------
    analytics_sales["order_month"] = analytics_sales["order_date"].dt.to_period("M").astype(str)

    return customers, products, orders, order_items, trusted_order_items, analytics_sales


# =========================================================
# CARGA
# =========================================================

def load(customers, products, orders, order_items, trusted_order_items, analytics_sales):
    con = duckdb.connect(str(DB_PATH))

    # Registrar dataframes temporariamente
    con.register("customers_df", customers)
    con.register("products_df", products)
    con.register("orders_df", orders)
    con.register("order_items_df", order_items)
    con.register("trusted_order_items_df", trusted_order_items)
    con.register("analytics_sales_df", analytics_sales)

    # Camada raw
    con.execute("CREATE OR REPLACE TABLE raw_customers AS SELECT * FROM customers_df")
    con.execute("CREATE OR REPLACE TABLE raw_products AS SELECT * FROM products_df")
    con.execute("CREATE OR REPLACE TABLE raw_orders AS SELECT * FROM orders_df")
    con.execute("CREATE OR REPLACE TABLE raw_order_items AS SELECT * FROM order_items_df")

    # Camada tratada
    con.execute("CREATE OR REPLACE TABLE trusted_order_items AS SELECT * FROM trusted_order_items_df")

    # Camada analítica final
    con.execute("CREATE OR REPLACE TABLE analytics_sales AS SELECT * FROM analytics_sales_df")

    con.close()


# =========================================================
# FUNÇÃO PRINCIPAL
# =========================================================

def main():
    print("Iniciando extração...")
    customers, products, orders, order_items = extract()

    print("Iniciando transformação...")
    customers, products, orders, order_items, trusted_order_items, analytics_sales = transform(
        customers, products, orders, order_items
    )

    print("Carregando dados no DuckDB...")
    load(customers, products, orders, order_items, trusted_order_items, analytics_sales)

    print("Pipeline ETL executado com sucesso.")
    print(f"Tabela final analytics_sales com {len(analytics_sales)} registros.")


if __name__ == "__main__":
    main()