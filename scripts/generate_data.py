from pathlib import Path
import random
from faker import Faker
import pandas as pd

fake = Faker("pt_BR")
random.seed(42)
Faker.seed(42)

RAW_PATH = Path("data/raw")
RAW_PATH.mkdir(parents=True, exist_ok=True)

N_CUSTOMERS = 3000
N_PRODUCTS = 300
N_ORDERS = 10000
N_ORDER_ITEMS = 20000

def random_date(start_date: str, end_date: str):
    """
    Gera uma data aleatória entre duas datas.
    O Faker já devolve um objeto date, que depois o pandas consegue salvar.
    """
    return fake.date_between(start_date=start_date, end_date=end_date)


def generate_customers(n: int = N_CUSTOMERS) -> pd.DataFrame:
    states = [
        "PE", "PB", "CE", "RN", "AL", "BA", "SP", "RJ",
        "MG", "PR", "SC", "RS", "GO", "PA", "AM", "DF"
    ]

    data = []
    for customer_id in range(1, n + 1):
        data.append({
            "customer_id": customer_id,
            "customer_name": fake.name(),
            "city": fake.city(),
            "state": random.choice(states),
            "signup_date": random_date("-3y", "today")
        })

    return pd.DataFrame(data)


def generate_products(n: int = N_PRODUCTS) -> pd.DataFrame:
    categories = [
        "Eletrônicos", "Livros", "Casa", "Moda",
        "Esportes", "Beleza", "Informática", "Brinquedos"
    ]

    product_adjectives = [
        "Premium", "Compacto", "Smart", "Ultra",
        "Eco", "Max", "Plus", "Flex"
    ]

    product_nouns = [
        "Notebook", "Mouse", "Teclado", "Livro", "Camiseta",
        "Tênis", "Liquidificador", "Fone", "Monitor", "Mochila",
        "Bicicleta", "Perfume", "Cadeira", "Relógio"
    ]

    data = []
    for product_id in range(1, n + 1):
        name = f"{random.choice(product_nouns)} {random.choice(product_adjectives)}"
        price = round(random.uniform(15, 5000), 2)

        data.append({
            "product_id": product_id,
            "product_name": name,
            "category": random.choice(categories),
            "price": price
        })

    return pd.DataFrame(data)


def generate_orders(customers_df: pd.DataFrame, n: int = N_ORDERS) -> pd.DataFrame:
    statuses = ["pending", "shipped", "delivered", "cancelled"]

    customer_ids = customers_df["customer_id"].tolist()

    data = []
    for order_id in range(1, n + 1):
        data.append({
            "order_id": order_id,
            "customer_id": random.choice(customer_ids),
            "order_date": random_date("-2y", "today"),
            "status": random.choice(statuses)
        })

    return pd.DataFrame(data)


def generate_order_items(
    orders_df: pd.DataFrame,
    products_df: pd.DataFrame,
    n: int = N_ORDER_ITEMS
) -> pd.DataFrame:
    order_ids = orders_df["order_id"].tolist()

    # Vamos montar um dicionário product_id -> preço
    product_price_map = dict(zip(products_df["product_id"], products_df["price"]))
    product_ids = products_df["product_id"].tolist()

    data = []
    for order_item_id in range(1, n + 1):
        product_id = random.choice(product_ids)

        data.append({
            "order_item_id": order_item_id,
            "order_id": random.choice(order_ids),
            "product_id": product_id,
            "quantity": random.randint(1, 5),
            "unit_price": product_price_map[product_id]
        })

    return pd.DataFrame(data)


def main():
    print("Gerando customers...")
    customers_df = generate_customers()
    customers_df.to_csv(RAW_PATH / "customers.csv", index=False)

    print("Gerando products...")
    products_df = generate_products()
    products_df.to_csv(RAW_PATH / "products.csv", index=False)

    print("Gerando orders...")
    orders_df = generate_orders(customers_df)
    orders_df.to_csv(RAW_PATH / "orders.csv", index=False)

    print("Gerando order_items...")
    order_items_df = generate_order_items(orders_df, products_df)
    order_items_df.to_csv(RAW_PATH / "order_items.csv", index=False)

    print("Arquivos gerados com sucesso em data/raw/")
    print(f"customers: {len(customers_df)}")
    print(f"products: {len(products_df)}")
    print(f"orders: {len(orders_df)}")
    print(f"order_items: {len(order_items_df)}")


if __name__ == "__main__":
    main()