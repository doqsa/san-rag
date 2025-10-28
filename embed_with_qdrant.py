
import os
import json
import argparse
from typing import Dict, Any, Iterable, List, Tuple, Optional

# OpenAI SDK (v1)
from openai import OpenAI

# Qdrant client
from qdrant_client import QdrantClient
from qdrant_client.http import models as qm

# Simple chunking utility (no external deps)
def split_text(text: str, *, chunk_size: int = 1200, chunk_overlap: int = 200) -> List[str]:
    if not text:
        return []
    text = text.strip()
    if len(text) <= chunk_size:
        return [text]
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunk = text[start:end]
        chunks.append(chunk)
        if end == len(text):
            break
        start = end - chunk_overlap
        if start < 0:
            start = 0
    return chunks

def read_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"JSON error in {path} at line {i}: {e}") from e

def compose_content(rec: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    Build the text to embed + metadata payload.
    Priority: use "text" if present; optionally prepend a title/context line.
    """
    title = rec.get("title") or ""
    text = rec.get("text") or ""
    # Fallbacks if someone stored text under "content"
    if not text and "content" in rec:
        text = rec["content"]
    # Minimal content guard
    base = f"{title}\n{text}".strip() if title else (text or "")
    payload_meta = {
        "id": rec.get("id"),
        "type": rec.get("type"),
        "title": title,
        "source": rec.get("source"),
        "hierarchy": rec.get("hierarchy"),
        "locators": rec.get("locators"),
        "chunking": rec.get("chunking"),
    }
    return base, payload_meta

def ensure_collection(client: QdrantClient, collection: str, vector_size: int, distance: str = "Cosine"):
    dist_map = {
        "Cosine": qm.Distance.COSINE,
        "Dot": qm.Distance.DOT,
        "Euclid": qm.Distance.EUCLID,
    }
    distance_enum = dist_map.get(distance, qm.Distance.COSINE)
    exists = False
    try:
        info = client.get_collection(collection_name=collection)
        exists = info is not None
    except Exception:
        exists = False

    if not exists:
        client.recreate_collection(
            collection_name=collection,
            vectors_config=qm.VectorParams(size=vector_size, distance=distance_enum),
        )

def batched(iterable, batch_size: int):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

def main():
    parser = argparse.ArgumentParser(description="Embed JSONL (documents/provisions) and upsert into Qdrant.")
    parser.add_argument("--documents", type=str, default="kr-osh-act_documents.jsonl", help="Path to documents JSONL")
    parser.add_argument("--provisions", type=str, default="kr-osh-act_provisions.jsonl", help="Path to provisions JSONL")
    parser.add_argument("--edges", type=str, default="kr-osh-act_edges.jsonl", help="(Optional) Path to edges JSONL (not embedded)")
    parser.add_argument("--collection", type=str, default="kr_law_provisions", help="Qdrant collection name")
    parser.add_argument("--qdrant-host", type=str, default="localhost", help="Qdrant host")
    parser.add_argument("--qdrant-port", type=int, default=6333, help="Qdrant port")
    parser.add_argument("--qdrant-api-key", type=str, default=None, help="Qdrant API key if needed")
    parser.add_argument("--openai-emb-model", type=str, default="text-embedding-3-small", help="OpenAI embedding model")
    parser.add_argument("--batch-size", type=int, default=64, help="Embedding batch size")
    parser.add_argument("--chunk-size", type=int, default=1200, help="Character chunk size")
    parser.add_argument("--chunk-overlap", type=int, default=200, help="Character chunk overlap")
    parser.add_argument("--distance", type=str, default="Cosine", choices=["Cosine", "Dot", "Euclid"], help="Vector distance metric")
    parser.add_argument("--dry-run", action="store_true", help="Parse + print counts only, don't call APIs")
    args = parser.parse_args()

    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not args.dry_run and not openai_api_key:
        raise RuntimeError("Please set OPENAI_API_KEY in your environment.")

    # Collect records to embed: provisions + documents (documents are often top-level summaries).
    items: List[Dict[str, Any]] = []
    if os.path.exists(args.provisions):
        items.extend(read_jsonl(args.provisions))
    if os.path.exists(args.documents):
        items.extend(read_jsonl(args.documents))

    # Prepare texts and metadata
    prepared: List[Tuple[str, Dict[str, Any]]] = []
    for rec in items:
        content, meta = compose_content(rec)
        if not content:
            # Skip empty
            continue
        chunks = split_text(content, chunk_size=args.chunk_size, chunk_overlap=args.chunk_overlap)
        for idx, ch in enumerate(chunks):
            payload = dict(meta)
            payload["chunk_index"] = idx
            payload["chunk_total"] = len(chunks)
            prepared.append((ch, payload))

    print(f"Total records (provisions+documents): {len(items)}")
    print(f"Total chunks to embed: {len(prepared)}")

    if args.dry_run:
        return

    # Initialize clients
    oai = OpenAI()
    # Prime a single embedding to get vector size
    sample_vec = oai.embeddings.create(model=args.openai_emb_model, input="ping").data[0].embedding
    vector_size = len(sample_vec)

    qdrant = QdrantClient(host=args.qdrant_host, port=args.qdrant_port, api_key=args.qdrant_api_key)
    ensure_collection(qdrant, args.collection, vector_size=vector_size, distance=args.distance)

    # Upsert in batches
    point_id_seq = 0
    for batch in batched(prepared, args.batch_size):
        texts = [t for t, _ in batch]
        # Call embeddings
        emb = oai.embeddings.create(model=args.openai_emb_model, input=texts)
        vectors = [d.embedding for d in emb.data]

        # Build Qdrant points
        points = []
        for (_, meta), vec in zip(batch, vectors):
            # Stable point ids: combine base id + chunk index if available
            base_id = meta.get("id") or "unknown"
            chunk_idx = meta.get("chunk_index", 0)
            pid = f"{base_id}::chunk::{chunk_idx}"
            points.append(
                qm.PointStruct(id=pid, vector=vec, payload=meta)
            )

        qdrant.upsert(collection_name=args.collection, points=points)

        point_id_seq += len(points)
        print(f"Upserted {len(points)} points. Total so far: {point_id_seq}")

    print("✅ Embedding + upsert completed.")

if __name__ == "__main__":
    main()
