"""
FlowForge Demo Data Seeder

Seeds realistic e-commerce data into all infrastructure components:
- PostgreSQL: 200 orders with order_items
- Kafka: 500 e-commerce events (orders, clickstream, status updates)
- MongoDB: 50 clickstream documents

Run this after `docker compose up -d` and services are healthy.
"""

import json
import logging
import sys
import time
from pathlib import Path

# Allow running from repo root without installing
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("seed_demo")


def seed_postgres(count: int = 200) -> int:
    from flowforge.data_generator import DataProducer
    logger.info(f"Seeding PostgreSQL with {count} orders...")
    producer = DataProducer()
    inserted = producer.produce_to_postgres(count=count)
    logger.info(f"PostgreSQL: {inserted} orders inserted")
    return inserted


def seed_kafka(count: int = 500) -> int:
    from flowforge.data_generator import DataProducer
    logger.info(f"Seeding Kafka with {count} events (topic: ecommerce.events)...")
    producer = DataProducer()
    # High rate to avoid slow sleep in generator during bulk seeding
    produced = producer.produce_to_kafka(
        topic="ecommerce.events",
        count=count,
        events_per_second=500.0,
    )
    logger.info(f"Kafka: {produced} events produced")
    return produced


def seed_mongodb(count: int = 50) -> int:
    from flowforge.config import config
    from flowforge.data_generator import generate_clickstream_event

    logger.info(f"Seeding MongoDB with {count} clickstream documents...")
    try:
        import pymongo
        client = pymongo.MongoClient(config.mongo.uri, serverSelectionTimeoutMS=5000)
        db = client[config.mongo.database]
        docs = [generate_clickstream_event() for _ in range(count)]
        result = db["events"].insert_many(docs)
        inserted = len(result.inserted_ids)
        client.close()
        logger.info(f"MongoDB: {inserted} documents inserted")
        return inserted
    except Exception as e:
        logger.warning(f"MongoDB seeding skipped: {e}")
        return 0


def wait_for_postgres(retries: int = 10, delay: float = 3.0) -> bool:
    from flowforge.config import config
    import psycopg2

    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(config.postgres.dsn, connect_timeout=3)
            conn.close()
            return True
        except Exception:
            logger.info(f"Waiting for PostgreSQL... ({attempt}/{retries})")
            time.sleep(delay)
    return False


def wait_for_kafka(retries: int = 10, delay: float = 3.0) -> bool:
    from flowforge.config import config
    from confluent_kafka.admin import AdminClient

    for attempt in range(1, retries + 1):
        try:
            admin = AdminClient({"bootstrap.servers": config.kafka.bootstrap_servers, "socket.timeout.ms": 3000})
            admin.list_topics(timeout=3)
            return True
        except Exception:
            logger.info(f"Waiting for Kafka... ({attempt}/{retries})")
            time.sleep(delay)
    return False


def main():
    print("\nFlowForge Demo Data Seeder")
    print("=" * 40)

    if not wait_for_postgres():
        logger.error("PostgreSQL is not reachable. Is docker compose running?")
        sys.exit(1)

    if not wait_for_kafka():
        logger.warning("Kafka is not reachable — skipping Kafka seed.")
        kafka_ok = False
    else:
        kafka_ok = True

    pg_count = seed_postgres(count=200)
    kafka_count = seed_kafka(count=500) if kafka_ok else 0
    mongo_count = seed_mongodb(count=50)

    print("\nSeed Summary")
    print("=" * 40)
    print(f"  PostgreSQL orders : {pg_count}")
    print(f"  Kafka events      : {kafka_count}")
    print(f"  MongoDB documents : {mongo_count}")
    print("\nDemo data ready. Start the API server:")
    print("  uvicorn flowforge.api_server:app --port 8000 --reload")
    print()


if __name__ == "__main__":
    main()
