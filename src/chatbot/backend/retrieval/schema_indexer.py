"""
Schema Indexer — Index schema metadata VÀ SQL samples vào ChromaDB
- Collection 1: 'lakehouse_schemas'  → embed table metadata
- Collection 2: 'sql_samples'        → embed câu hỏi mẫu

Chạy một lần để khởi tạo, hoặc chạy lại khi thêm bảng / samples mới.

Usage:
    python -m src.chatbot.backend.retrieval.schema_indexer
"""

import yaml
from pathlib import Path

import chromadb
from chromadb.utils import embedding_functions
from dotenv import load_dotenv

load_dotenv()

# ── Paths ────────────────────────────────────────────────────────────────────
_MCP_DIR = Path(__file__).parent.parent / "mcp_server"
_METADATA_PATH = _MCP_DIR / "schema_metadata.yaml"
_SQL_SAMPLES_PATH = _MCP_DIR / "sql_samples.yaml"
_STORE_PATH = Path(__file__).parent.parent / "schema_store"

# ── ChromaDB collection names ─────────────────────────────────────────────────
SCHEMA_COLLECTION = "lakehouse_schemas"
SQL_SAMPLE_COLLECTION = "sql_samples"


# ── Embedding function (shared) ───────────────────────────────────────────────
def _get_embedding_function():
    return embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="all-MiniLM-L6-v2"
    )


def _get_client() -> chromadb.PersistentClient:
    _STORE_PATH.mkdir(parents=True, exist_ok=True)
    return chromadb.PersistentClient(path=str(_STORE_PATH))


# ══════════════════════════════════════════════════════════════════════════════
# Collection 1: Schema Metadata
# ══════════════════════════════════════════════════════════════════════════════

def _build_schema_document(table_name: str, table_meta: dict) -> tuple[str, dict]:
    """Chuyển metadata của 1 bảng thành chuỗi text để embed."""
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
    """Index schema_metadata.yaml → collection 'lakehouse_schemas'"""
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


# ══════════════════════════════════════════════════════════════════════════════
# Collection 2: SQL Samples (few-shot)
# ══════════════════════════════════════════════════════════════════════════════

def build_sql_sample_index(
    client: chromadb.PersistentClient,
    ef,
    force_rebuild: bool = False,
) -> chromadb.Collection:
    """
    Index sql_samples.yaml → collection 'sql_samples'.
    Embed CÂU HỎI (không phải SQL) để tìm example tương tự khi query.
    """
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

        # Document = câu hỏi (để embed và tìm similarity)
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


# ══════════════════════════════════════════════════════════════════════════════
# Public API — get_collection helpers
# ══════════════════════════════════════════════════════════════════════════════

def get_collection() -> chromadb.Collection:
    """Lấy schema collection (dùng bởi schema_retriever)."""
    client = _get_client()
    ef = _get_embedding_function()
    return client.get_or_create_collection(
        name=SCHEMA_COLLECTION,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )


def get_sql_sample_collection() -> chromadb.Collection:
    """Lấy SQL samples collection (dùng bởi sql_sample_retriever)."""
    client = _get_client()
    ef = _get_embedding_function()
    return client.get_or_create_collection(
        name=SQL_SAMPLE_COLLECTION,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )


# ══════════════════════════════════════════════════════════════════════════════
# Entry point — build cả 2 collections
# ══════════════════════════════════════════════════════════════════════════════

def build_index(force_rebuild: bool = False):
    """Build cả Schema index VÀ SQL Samples index."""
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
