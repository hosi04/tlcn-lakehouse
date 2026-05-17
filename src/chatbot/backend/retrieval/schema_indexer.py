import yaml
from pathlib import Path

import chromadb
from chromadb.utils import embedding_functions
from dotenv import load_dotenv

load_dotenv()

_MCP_DIR = Path(__file__).parent.parent / "mcp_server"
_METADATA_PATH = _MCP_DIR / "schema_metadata.yaml"
_SQL_SAMPLES_PATH = _MCP_DIR / "sql_samples.yaml"
_STORE_PATH = Path(__file__).parent.parent / "schema_store"

SCHEMA_COLLECTION = "lakehouse_schemas"
SQL_SAMPLE_COLLECTION = "sql_samples"
_embedding_function_instance = None
_client_instance = None

def _get_embedding_function():
    global _embedding_function_instance
    if _embedding_function_instance is None:
        _embedding_function_instance = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name="all-MiniLM-L6-v2"
        )
    return _embedding_function_instance


def _get_client() -> chromadb.PersistentClient:
    global _client_instance
    if _client_instance is None:
        _STORE_PATH.mkdir(parents=True, exist_ok=True)
        _client_instance = chromadb.PersistentClient(path=str(_STORE_PATH))
    return _client_instance


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
            print(f"[Indexer] Xóa collection cũ: {SCHEMA_COLLECTION}")
            client.delete_collection(SCHEMA_COLLECTION)
        else:
            print(f"[Indexer] '{SCHEMA_COLLECTION}' đã tồn tại. Dùng lại.")
            return client.get_collection(SCHEMA_COLLECTION, embedding_function=ef)

    collection = client.create_collection(
        name=SCHEMA_COLLECTION,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )

    with open(_METADATA_PATH, "r", encoding="utf-8") as f:
        yaml_data = yaml.safe_load(f)

    tables = yaml_data.get("tables", {})
    print(f"[Indexer] Index {len(tables)} bảng vào '{SCHEMA_COLLECTION}'...")

    ids, docs, metas = [], [], []
    for table_name, table_meta in tables.items():
        doc_text, meta = _build_schema_document(table_name, table_meta)
        ids.append(table_name)
        docs.append(doc_text)
        metas.append(meta)
        print(f"  ✓ {table_name}")

    collection.add(ids=ids, documents=docs, metadatas=metas)
    print(f"[Indexer] ✅ Schema index: {len(ids)} bảng")
    return collection


def build_sql_sample_index(
    client: chromadb.PersistentClient,
    ef,
    force_rebuild: bool = False,
) -> chromadb.Collection:
    existing = [c.name for c in client.list_collections()]

    if SQL_SAMPLE_COLLECTION in existing:
        if force_rebuild:
            print(f"[Indexer] Xóa collection cũ: {SQL_SAMPLE_COLLECTION}")
            client.delete_collection(SQL_SAMPLE_COLLECTION)
        else:
            print(f"[Indexer] '{SQL_SAMPLE_COLLECTION}' đã tồn tại. Dùng lại.")
            return client.get_collection(SQL_SAMPLE_COLLECTION, embedding_function=ef)

    collection = client.create_collection(
        name=SQL_SAMPLE_COLLECTION,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )

    with open(_SQL_SAMPLES_PATH, "r", encoding="utf-8") as f:
        yaml_data = yaml.safe_load(f)

    samples = yaml_data.get("samples", [])
    print(f"[Indexer] Index {len(samples)} SQL samples vào '{SQL_SAMPLE_COLLECTION}'...")

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
        print(f"  ✓ [{sample_id}] {question[:50]}")

    collection.add(ids=ids, documents=docs, metadatas=metas)
    print(f"[Indexer] ✅ SQL samples index: {len(ids)} examples")
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


def build_index(force_rebuild: bool = False):
    client = _get_client()
    ef = _get_embedding_function()

    print("=" * 60)
    print("Lakehouse ChromaDB Indexer")
    print("=" * 60)

    build_schema_index(client, ef, force_rebuild=force_rebuild)
    print()
    build_sql_sample_index(client, ef, force_rebuild=force_rebuild)

    print()
    print("=" * 60)
    print(f"✅ Hoàn thành! Đã lưu vào: {_STORE_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    build_index(force_rebuild=True)
