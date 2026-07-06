import logging
from functools import lru_cache
from pathlib import Path

import yaml
import chromadb
from chromadb.utils import embedding_functions
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_MCP_DIR = Path(__file__).parent.parent / "mcp_server"
_METADATA_PATH = _MCP_DIR / "schema_metadata.yaml"
_SQL_SAMPLES_PATH = _MCP_DIR / "sql_samples.yaml"
_STORE_PATH = Path(__file__).parent.parent / "schema_store"

SCHEMA_COLLECTION = "lakehouse_schemas"
SQL_SAMPLE_COLLECTION = "sql_samples"


@lru_cache(maxsize=1)
def _get_embedding_function():
    return embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="all-MiniLM-L6-v2",
        device="cpu"
    )


@lru_cache(maxsize=1)
def _get_client() -> chromadb.PersistentClient:
    _STORE_PATH.mkdir(parents=True, exist_ok=True)
    return chromadb.PersistentClient(path=str(_STORE_PATH))


def _build_schema_document(table_name: str, table_meta: dict) -> tuple[str, dict]:
    cols = table_meta.get("columns", {})
    col_lines = [
        f"  - {col_name}: {col_info.get('description', '')}"
        for col_name, col_info in cols.items()
    ]
    domain_str = ", ".join(table_meta.get("business_domain", []))
    joins_str = "\n".join(table_meta.get("join_hints", []))

    doc = (
        f"[TABLE] {table_name}\n"
        f"[TYPE] {table_meta.get('type', '')}\n"
        f"[DESCRIPTION] {table_meta.get('description', '')}\n"
        f"[DOMAIN] {domain_str}\n"
        f"[COLUMNS]\n{chr(10).join(col_lines)}\n"
        f"[JOINS]\n{joins_str}"
    )
    meta = {
        "table_name": table_name,
        "table_type": table_meta.get("type", ""),
        "business_domain": domain_str,
    }
    return doc, meta


def build_schema_index(
    client: chromadb.PersistentClient,
    ef,
    force_rebuild: bool = False,
) -> chromadb.Collection:
    existing = [c.name for c in client.list_collections()]

    if SCHEMA_COLLECTION in existing:
        if force_rebuild:
            logger.info("Xóa collection cũ: %s", SCHEMA_COLLECTION)
            client.delete_collection(SCHEMA_COLLECTION)
        else:
            logger.info("'%s' đã tồn tại. Dùng lại.", SCHEMA_COLLECTION)
            return client.get_collection(SCHEMA_COLLECTION, embedding_function=ef)

    collection = client.create_collection(
        name=SCHEMA_COLLECTION,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )

    with open(_METADATA_PATH, "r", encoding="utf-8") as f:
        yaml_data = yaml.safe_load(f)

    tables = yaml_data.get("tables", {})
    logger.info("Index %d bảng vào '%s'...", len(tables), SCHEMA_COLLECTION)

    ids, docs, metas = [], [], []
    for table_name, table_meta in tables.items():
        doc_text, meta = _build_schema_document(table_name, table_meta)
        ids.append(table_name)
        docs.append(doc_text)
        metas.append(meta)
        logger.debug("  ✓ %s", table_name)

    collection.add(ids=ids, documents=docs, metadatas=metas)
    logger.info("Schema index: %d bảng", len(ids))
    return collection


def build_sql_sample_index(
    client: chromadb.PersistentClient,
    ef,
    force_rebuild: bool = False,
) -> chromadb.Collection:
    existing = [c.name for c in client.list_collections()]

    if SQL_SAMPLE_COLLECTION in existing:
        if force_rebuild:
            logger.info("Xóa collection cũ: %s", SQL_SAMPLE_COLLECTION)
            client.delete_collection(SQL_SAMPLE_COLLECTION)
        else:
            logger.info("'%s' đã tồn tại. Dùng lại.", SQL_SAMPLE_COLLECTION)
            return client.get_collection(SQL_SAMPLE_COLLECTION, embedding_function=ef)

    collection = client.create_collection(
        name=SQL_SAMPLE_COLLECTION,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )

    with open(_SQL_SAMPLES_PATH, "r", encoding="utf-8") as f:
        yaml_data = yaml.safe_load(f)

    samples = yaml_data.get("samples", [])
    logger.info("Index %d SQL samples vào '%s'...", len(samples), SQL_SAMPLE_COLLECTION)

    ids, docs, metas = [], [], []
    for sample in samples:
        sample_id = sample["id"]
        question = sample["question"]
        sql = sample["sql"].strip()
        tables = ", ".join(sample.get("tables", []))

        ids.append(sample_id)
        docs.append(question)
        metas.append({
            "sample_id": sample_id,
            "question": question,
            "sql": sql,
            "tables": tables,
        })
        logger.debug("  ✓ [%s] %s", sample_id, question[:50])

    collection.add(ids=ids, documents=docs, metadatas=metas)
    logger.info("SQL samples index: %d examples", len(ids))
    return collection


def get_collection() -> chromadb.Collection:
    client = _get_client()
    ef = _get_embedding_function()
    return client.get_or_create_collection(
        name=SCHEMA_COLLECTION,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )


def get_sql_sample_collection() -> chromadb.Collection:
    client = _get_client()
    ef = _get_embedding_function()
    return client.get_or_create_collection(
        name=SQL_SAMPLE_COLLECTION,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )


def ensure_index_ready(force_rebuild: bool = False) -> dict:
    client = _get_client()
    ef = _get_embedding_function()
    if force_rebuild:
        build_index(force_rebuild=True)
    else:
        schema_collection = get_collection()
        sql_collection = get_sql_sample_collection()
        if schema_collection.count() == 0 or sql_collection.count() == 0:
            build_index(force_rebuild=True)

    schema_collection = client.get_collection(SCHEMA_COLLECTION, embedding_function=ef)
    sql_collection = client.get_collection(SQL_SAMPLE_COLLECTION, embedding_function=ef)
    return {
        "schema_count": schema_collection.count(),
        "sql_sample_count": sql_collection.count(),
    }


def build_index(force_rebuild: bool = False):
    client = _get_client()
    ef = _get_embedding_function()

    logger.info("=" * 60)
    logger.info("Lakehouse ChromaDB Indexer")
    logger.info("=" * 60)

    build_schema_index(client, ef, force_rebuild=force_rebuild)
    build_sql_sample_index(client, ef, force_rebuild=force_rebuild)

    logger.info("Hoàn thành! Đã lưu vào: %s", _STORE_PATH)


if __name__ == "__main__":
    build_index(force_rebuild=True)
