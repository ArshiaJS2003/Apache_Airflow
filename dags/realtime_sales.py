"""
Real-time Sales Data Updater DAG
Keeps 25,000 rows fixed and updates values randomly every run.
"""

import random
from datetime import datetime, timedelta, timezone

import duckdb

from airflow.decorators import dag, task
from airflow.timetables.trigger import DeltaTriggerTimetable

DB_PATH = "/tmp/sales_data.duckdb"
RECORDS = 25000  # fixed rows

PRODUCTS = [
    ("Laptop", 799.99, 1299.99),
    ("Phone", 299.99, 999.99),
    ("Headphones", 29.99, 349.99),
    ("Tablet", 199.99, 799.99),
    ("Smartwatch", 99.99, 499.99),
    ("Monitor", 149.99, 899.99),
    ("Keyboard", 19.99, 179.99),
    ("Mouse", 9.99, 89.99),
]

REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America"]
CHANNELS = ["Online", "In-Store", "Mobile App"]


@dag(
    dag_id="realtime_sales_update",
    description="Maintains 25K rows and updates them like real-time",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=DeltaTriggerTimetable(timedelta(seconds=10)),
    catchup=False,
    max_active_runs=1,
    tags=["sales", "realtime"],
)
def realtime_sales():

    @task
    def init_database():
        con = duckdb.connect(DB_PATH)

        # Create table
        con.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                sale_id INTEGER,
                sale_time TIMESTAMP,
                product VARCHAR,
                price DOUBLE,
                quantity INTEGER,
                total DOUBLE,
                region VARCHAR,
                channel VARCHAR
            )
        """)

        # Insert only once if empty
        count = con.execute("SELECT COUNT(*) FROM sales").fetchone()[0]

        if count == 0:
            records = []
            now = datetime.now(timezone.utc)

            for i in range(RECORDS):
                product, min_price, max_price = random.choice(PRODUCTS)
                price = round(random.uniform(min_price, max_price), 2)
                quantity = random.randint(1, 5)

                records.append((
                    i,
                    now,
                    product,
                    price,
                    quantity,
                    round(price * quantity, 2),
                    random.choice(REGIONS),
                    random.choice(CHANNELS)
                ))

            con.executemany("""
                INSERT INTO sales VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, records)

        con.close()

    @task
    def update_sales():
        """Update all 25K rows with new random values"""
        con = duckdb.connect(DB_PATH)

        now = datetime.now(timezone.utc)

        # Fetch all IDs
        ids = con.execute("SELECT sale_id FROM sales").fetchall()

        updated_records = []

        for (sale_id,) in ids:
            product, min_price, max_price = random.choice(PRODUCTS)
            price = round(random.uniform(min_price, max_price), 2)
            quantity = random.randint(1, 5)

            updated_records.append((
                now,
                product,
                price,
                quantity,
                round(price * quantity, 2),
                random.choice(REGIONS),
                random.choice(CHANNELS),
                sale_id
            ))

        # Bulk update
        con.executemany("""
            UPDATE sales
            SET sale_time = ?,
                product = ?,
                price = ?,
                quantity = ?,
                total = ?,
                region = ?,
                channel = ?
            WHERE sale_id = ?
        """, updated_records)

        con.close()

        return "Updated 25,000 rows with new random values"

    init_database() >> update_sales()


realtime_sales()
