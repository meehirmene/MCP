"""
FlowForge MongoDB MCP Server
Exposes tools for querying, aggregation, and change stream operations on MongoDB.
"""

import json
import logging
from datetime import datetime
from typing import Any

from pymongo import MongoClient
from bson import json_util, ObjectId
from mcp.server.fastmcp import FastMCP

from flowforge.config import config

logger = logging.getLogger(__name__)

mongodb_mcp = FastMCP(
    "FlowForge MongoDB MCP",
    instructions="MCP server for MongoDB — query, aggregate, change streams, collection management",
)


def _get_client() -> MongoClient:
    return MongoClient(config.mongo.uri)


def _get_db():
    client = _get_client()
    return client[config.mongo.database]


def _serialize(obj: Any) -> Any:
    """Make MongoDB results JSON-serializable."""
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(item) for item in obj]
    return obj


@mongodb_mcp.tool()
def list_collections() -> str:
    """List all collections in the MongoDB database with document counts."""
    db = _get_db()
    try:
        collections = []
        for name in db.list_collection_names():
            count = db[name].estimated_document_count()
            collections.append({"name": name, "document_count": count})
        return json.dumps({"collections": collections, "count": len(collections)}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mongodb_mcp.tool()
def find_documents(
    collection: str,
    filter_query: str = "{}",
    limit: int = 10,
    sort_field: str | None = None,
    sort_order: int = -1,
) -> str:
    """Find documents in a MongoDB collection.

    Args:
        collection: Collection name
        filter_query: JSON string of MongoDB filter query (default: all documents)
        limit: Maximum documents to return (default 10)
        sort_field: Optional field to sort by
        sort_order: Sort order — 1 for ascending, -1 for descending (default -1)
    """
    db = _get_db()
    try:
        query = json.loads(filter_query)
        cursor = db[collection].find(query).limit(limit)
        if sort_field:
            cursor = cursor.sort(sort_field, sort_order)
        docs = [_serialize(doc) for doc in cursor]
        return json.dumps({"collection": collection, "documents": docs, "count": len(docs)}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mongodb_mcp.tool()
def insert_document(collection: str, document: str) -> str:
    """Insert a document into a MongoDB collection.

    Args:
        collection: Collection name
        document: JSON string of the document to insert
    """
    db = _get_db()
    try:
        doc = json.loads(document)
        result = db[collection].insert_one(doc)
        return json.dumps({
            "status": "inserted",
            "collection": collection,
            "inserted_id": str(result.inserted_id),
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


@mongodb_mcp.tool()
def aggregate(collection: str, pipeline: str) -> str:
    """Run an aggregation pipeline on a MongoDB collection.

    Args:
        collection: Collection name
        pipeline: JSON string of the aggregation pipeline (array of stages)
    """
    db = _get_db()
    try:
        stages = json.loads(pipeline)
        results = list(db[collection].aggregate(stages))
        docs = [_serialize(doc) for doc in results]
        return json.dumps({"collection": collection, "results": docs, "count": len(docs)}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mongodb_mcp.tool()
def get_collection_schema(collection: str, sample_size: int = 100) -> str:
    """Infer the schema of a MongoDB collection by sampling documents.

    Args:
        collection: Collection name
        sample_size: Number of documents to sample for schema inference (default 100)
    """
    db = _get_db()
    try:
        docs = list(db[collection].find().limit(sample_size))
        if not docs:
            return json.dumps({"collection": collection, "schema": {}, "message": "Collection is empty"})

        # Infer schema from documents
        schema = {}
        for doc in docs:
            for key, value in doc.items():
                type_name = type(value).__name__
                if key not in schema:
                    schema[key] = {"types": set(), "nullable": False, "sample": None}
                schema[key]["types"].add(type_name)
                if value is None:
                    schema[key]["nullable"] = True
                elif schema[key]["sample"] is None:
                    schema[key]["sample"] = str(value)[:100]

        # Convert sets to lists for JSON serialization
        schema_out = {}
        for key, info in schema.items():
            schema_out[key] = {
                "types": list(info["types"]),
                "nullable": info["nullable"],
                "sample": info["sample"],
            }

        return json.dumps({
            "collection": collection,
            "schema": schema_out,
            "documents_sampled": len(docs),
        }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mongodb_mcp.tool()
def create_collection(collection_name: str) -> str:
    """Create a new MongoDB collection.

    Args:
        collection_name: Name of the collection to create
    """
    db = _get_db()
    try:
        db.create_collection(collection_name)
        return json.dumps({"status": "created", "collection": collection_name})
    except Exception as e:
        return json.dumps({"error": str(e)})


def create_server() -> FastMCP:
    return mongodb_mcp


if __name__ == "__main__":
    mongodb_mcp.run()
