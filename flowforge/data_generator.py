"""
FlowForge E-Commerce Data Generator

Generates realistic streaming e-commerce events and pushes them to:
- PostgreSQL (new orders, updates)
- Kafka topics (event stream)
- MongoDB (clickstream events)

Simulates real-world patterns including:
- Burst ordering periods
- Schema drift (adding new columns)
- Occasional failures for self-healing testing
"""

import json
import random
import time
import logging
from datetime import datetime, timezone
from typing import Generator

import psycopg2
from confluent_kafka import Producer

from flowforge.config import config

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Sample Data Pools
# ─────────────────────────────────────────────

REGIONS = ["us-east", "us-west", "eu-west", "eu-east", "ap-south", "ap-southeast"]
CATEGORIES = ["Electronics", "Sports", "Kitchen", "Home", "Books", "Fashion"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "crypto"]
ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]
PRODUCT_NAMES = [
    "Wireless Headphones", "Running Shoes", "Coffee Maker", "Laptop Stand",
    "Yoga Mat", "Water Bottle", "Bluetooth Speaker", "Desk Lamp",
    "Backpack", "Smartwatch", "Kindle", "Mechanical Keyboard",
    "Webcam HD", "USB-C Hub", "Noise Canceller", "Standing Desk",
]
FIRST_NAMES = ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank",
               "Ivy", "Jack", "Karen", "Leo", "Maya", "Nick", "Olivia", "Pete"]
LAST_NAMES = ["Johnson", "Smith", "Williams", "Brown", "Davis", "Miller",
              "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson"]
PAGES = ["/home", "/products", "/cart", "/checkout", "/search", "/category", "/deals"]
REFERRERS = ["google.com", "facebook.com", "twitter.com", "direct", "email", "instagram.com"]


def generate_order_event() -> dict:
    """Generate a realistic e-commerce order event."""
    customer_id = random.randint(1, 5)
    region = random.choice(REGIONS)
    num_items = random.randint(1, 4)
    items = []
    total = 0.0

    for _ in range(num_items):
        product_id = random.randint(1, 8)
        quantity = random.randint(1, 3)
        unit_price = round(random.uniform(9.99, 199.99), 2)
        items.append({
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": unit_price,
        })
        total += unit_price * quantity

    return {
        "event_type": "order_created",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": customer_id,
        "region": region,
        "total_amount": round(total, 2),
        "items": items,
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": "pending",
    }


def generate_clickstream_event() -> dict:
    """Generate a clickstream / user behavior event."""
    event_types = ["page_view", "add_to_cart", "remove_from_cart", "search",
                   "checkout_start", "checkout_complete", "product_view"]
    event_type = random.choice(event_types)

    event = {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": random.randint(1, 100),
        "session_id": f"sess_{random.randint(1000, 9999)}",
        "data": {
            "page": random.choice(PAGES),
            "referrer": random.choice(REFERRERS),
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "browser": random.choice(["chrome", "safari", "firefox", "edge"]),
        },
    }

    if event_type == "product_view":
        event["data"]["product_id"] = random.randint(1, 8)
        event["data"]["category"] = random.choice(CATEGORIES)
    elif event_type == "add_to_cart":
        event["data"]["product_id"] = random.randint(1, 8)
        event["data"]["quantity"] = random.randint(1, 3)
        event["data"]["price"] = round(random.uniform(9.99, 199.99), 2)
    elif event_type == "search":
        event["data"]["query"] = random.choice(PRODUCT_NAMES).lower()
        event["data"]["results_count"] = random.randint(0, 50)

    return event


def generate_status_update_event() -> dict:
    """Generate an order status update event."""
    return {
        "event_type": "order_status_updated",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": random.randint(1, 5),
        "old_status": random.choice(ORDER_STATUSES[:3]),
        "new_status": random.choice(ORDER_STATUSES[1:]),
        "updated_by": "system",
    }


def event_generator(events_per_second: float = 2.0) -> Generator[dict, None, None]:
    """Infinite generator of mixed e-commerce events."""
    weights = [0.3, 0.5, 0.2]  # order, clickstream, status_update
    generators = [generate_order_event, generate_clickstream_event, generate_status_update_event]

    while True:
        gen_func = random.choices(generators, weights=weights, k=1)[0]
        yield gen_func()
        time.sleep(1.0 / events_per_second)


class DataProducer:
    """Pushes generated events to Kafka topics."""

    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": config.kafka.bootstrap_servers,
        })

    def produce_to_kafka(
        self,
        topic: str = "ecommerce.events",
        count: int = 100,
        events_per_second: float = 5.0,
    ) -> int:
        """Produce a batch of events to a Kafka topic."""
        produced = 0
        gen = event_generator(events_per_second)

        for _ in range(count):
            event = next(gen)
            try:
                self.producer.produce(
                    topic=topic,
                    key=event.get("event_type", "unknown").encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                )
                produced += 1

                if produced % 10 == 0:
                    self.producer.flush()
                    logger.info(f"Produced {produced}/{count} events to {topic}")

            except Exception as e:
                logger.error(f"Error producing event: {e}")

        self.producer.flush()
        logger.info(f"Completed: {produced}/{count} events to {topic}")
        return produced

    def produce_to_postgres(self, count: int = 20) -> int:
        """Insert new orders directly into PostgreSQL."""
        conn = psycopg2.connect(
            host=config.postgres.host,
            port=config.postgres.port,
            database=config.postgres.database,
            user=config.postgres.user,
            password=config.postgres.password,
        )
        inserted = 0
        try:
            with conn.cursor() as cur:
                for _ in range(count):
                    event = generate_order_event()
                    cur.execute(
                        """INSERT INTO orders (customer_id, status, total_amount, region)
                           VALUES (%s, %s, %s, %s) RETURNING order_id""",
                        (event["customer_id"], event["status"],
                         event["total_amount"], event["region"]),
                    )
                    order_id = cur.fetchone()[0]

                    for item in event["items"]:
                        cur.execute(
                            """INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                               VALUES (%s, %s, %s, %s)""",
                            (order_id, item["product_id"], item["quantity"], item["unit_price"]),
                        )
                    inserted += 1

                conn.commit()
                logger.info(f"Inserted {inserted} orders into PostgreSQL")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting into PostgreSQL: {e}")
        finally:
            conn.close()

        return inserted

    def simulate_schema_drift(self) -> dict:
        """Add a new column to the orders table to simulate schema drift."""
        conn = psycopg2.connect(
            host=config.postgres.host,
            port=config.postgres.port,
            database=config.postgres.database,
            user=config.postgres.user,
            password=config.postgres.password,
        )
        try:
            with conn.cursor() as cur:
                column_name = f"discount_pct"
                cur.execute(f"""
                    ALTER TABLE orders
                    ADD COLUMN IF NOT EXISTS {column_name} DECIMAL(5,2) DEFAULT 0.00;
                """)
                conn.commit()
                logger.info(f"Schema drift simulated: added column '{column_name}' to orders")
                return {
                    "status": "schema_drift_simulated",
                    "table": "orders",
                    "column_added": column_name,
                    "column_type": "DECIMAL(5,2)",
                }
        except Exception as e:
            conn.rollback()
            logger.error(f"Error simulating schema drift: {e}")
            return {"error": str(e)}
        finally:
            conn.close()
