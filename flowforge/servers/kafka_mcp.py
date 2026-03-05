"""
FlowForge Kafka MCP Server
Exposes tools for Kafka topic management, producing, consuming, and monitoring.
"""

import json
import logging
import uuid
from typing import Any

from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from mcp.server.fastmcp import FastMCP

from flowforge.config import config

logger = logging.getLogger(__name__)

kafka_mcp = FastMCP(
    "FlowForge Kafka MCP",
    instructions="MCP server for Apache Kafka — topic management, produce, consume, monitor",
)


def _get_admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": config.kafka.bootstrap_servers})


def _get_producer() -> Producer:
    return Producer({"bootstrap.servers": config.kafka.bootstrap_servers})


def _get_consumer(group_id: str | None = None) -> Consumer:
    return Consumer({
        "bootstrap.servers": config.kafka.bootstrap_servers,
        "group.id": group_id or f"flowforge-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


@kafka_mcp.tool()
def list_topics() -> str:
    """List all Kafka topics with partition counts and configuration."""
    admin = _get_admin()
    try:
        metadata = admin.list_topics(timeout=10)
        topics = []
        for name, topic_meta in metadata.topics.items():
            if not name.startswith("_"):  # Skip internal topics
                topics.append({
                    "name": name,
                    "partitions": len(topic_meta.partitions),
                })
        return json.dumps({"topics": topics, "count": len(topics)}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@kafka_mcp.tool()
def create_topic(
    topic_name: str,
    num_partitions: int = 3,
    replication_factor: int = 1,
) -> str:
    """Create a new Kafka topic.

    Args:
        topic_name: Name of the topic to create
        num_partitions: Number of partitions (default 3)
        replication_factor: Replication factor (default 1 for local dev)
    """
    admin = _get_admin()
    try:
        new_topic = NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        futures = admin.create_topics([new_topic])
        for topic, future in futures.items():
            future.result()  # Block until complete
        return json.dumps({
            "status": "created",
            "topic": topic_name,
            "partitions": num_partitions,
            "replication_factor": replication_factor,
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


@kafka_mcp.tool()
def delete_topic(topic_name: str) -> str:
    """Delete a Kafka topic.

    Args:
        topic_name: Name of the topic to delete
    """
    admin = _get_admin()
    try:
        futures = admin.delete_topics([topic_name])
        for topic, future in futures.items():
            future.result()
        return json.dumps({"status": "deleted", "topic": topic_name})
    except Exception as e:
        return json.dumps({"error": str(e)})


@kafka_mcp.tool()
def produce_message(topic: str, value: str, key: str | None = None) -> str:
    """Produce a message to a Kafka topic.

    Args:
        topic: Kafka topic name
        value: Message value (JSON string)
        key: Optional message key for partitioning
    """
    producer = _get_producer()
    try:
        producer.produce(
            topic=topic,
            value=value.encode("utf-8"),
            key=key.encode("utf-8") if key else None,
        )
        producer.flush(timeout=10)
        return json.dumps({"status": "produced", "topic": topic, "key": key})
    except Exception as e:
        return json.dumps({"error": str(e)})


@kafka_mcp.tool()
def produce_batch(topic: str, messages: list[dict[str, str]]) -> str:
    """Produce a batch of messages to a Kafka topic.

    Args:
        topic: Kafka topic name
        messages: List of dicts with 'key' (optional) and 'value' fields
    """
    producer = _get_producer()
    produced_count = 0
    errors = []
    try:
        for msg in messages:
            value = msg.get("value", "")
            key = msg.get("key")
            try:
                producer.produce(
                    topic=topic,
                    value=value.encode("utf-8"),
                    key=key.encode("utf-8") if key else None,
                )
                produced_count += 1
            except Exception as e:
                errors.append(str(e))
        producer.flush(timeout=30)
        return json.dumps({
            "status": "produced",
            "topic": topic,
            "count": produced_count,
            "errors": errors,
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


@kafka_mcp.tool()
def consume_messages(topic: str, max_messages: int = 10, timeout_seconds: float = 5.0) -> str:
    """Consume messages from a Kafka topic (for inspection/debugging).

    Args:
        topic: Kafka topic name
        max_messages: Maximum messages to consume (default 10)
        timeout_seconds: Timeout in seconds (default 5)
    """
    consumer = _get_consumer()
    messages = []
    try:
        consumer.subscribe([topic])
        remaining = max_messages
        while remaining > 0:
            msg = consumer.poll(timeout=timeout_seconds)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                else:
                    return json.dumps({"error": msg.error().str()})
            messages.append({
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "value": msg.value().decode("utf-8") if msg.value() else None,
                "timestamp": msg.timestamp()[1],
            })
            remaining -= 1
        return json.dumps({"topic": topic, "messages": messages, "count": len(messages)}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        consumer.close()


@kafka_mcp.tool()
def get_topic_info(topic_name: str) -> str:
    """Get detailed information about a Kafka topic including offsets and consumer groups.

    Args:
        topic_name: Name of the topic to inspect
    """
    admin = _get_admin()
    try:
        metadata = admin.list_topics(topic=topic_name, timeout=10)
        if topic_name not in metadata.topics:
            return json.dumps({"error": f"Topic '{topic_name}' not found"})

        topic_meta = metadata.topics[topic_name]
        partitions = []
        for pid, part_meta in topic_meta.partitions.items():
            partitions.append({
                "id": pid,
                "leader": part_meta.leader,
                "replicas": list(part_meta.replicas),
            })

        return json.dumps({
            "topic": topic_name,
            "partitions": partitions,
            "partition_count": len(partitions),
        }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


def create_server() -> FastMCP:
    return kafka_mcp


if __name__ == "__main__":
    kafka_mcp.run()
