# pip install llama-index qdrant-client sentence-transformers
import json, pandas as pd
from llama_index.core import Document, VectorStoreIndex, StorageContext, Settings
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.vector_stores.qdrant import QdrantVectorStore
from qdrant_client import QdrantClient

Settings.embed_model = HuggingFaceEmbedding(model_name="BAAI/bge-m3")

def load_jsonl(path):
    return [json.loads(l) for l in open(path,"r",encoding="utf-8")]

def to_docs(rows, source_type):
    docs=[]
    for r in rows:
        text = (f"제{r.get('article')}조 {r.get('title')}\n\n{r.get('content')}"
                if r.get("article") else f"{r.get('title')}\n\n{r.get('content')}")
        meta = {
            "source_type": source_type,
            "law_name": r.get("law_name"),
            "law_number": r.get("law_number"),
            "version": r.get("version") or r.get("enforcement_date"),
            "chapter": r.get("chapter"), "section": r.get("section"),
            "article": r.get("article"), "title": r.get("title"),
            "anchor": r.get("anchor"),
        }
        doc_id = r.get("anchor") or f"{source_type}::{r.get('title')}"
        docs.append(Document(text=text.strip(), metadata=meta, doc_id=doc_id))
    return docs

def build_index(collection_name, docs):
    qdr = QdrantClient(host="localhost", port=6333)
    vs  = QdrantVectorStore(client=qdr, collection_name=collection_name)
    sc  = StorageContext.from_defaults(vector_store=vs)
    return VectorStoreIndex.from_documents(docs, storage_context=sc)

# 파일 경로 맵 (필요에 맞게 수정)
paths = {
  "osh_rule": "osh_rule_ready.jsonl",
  "osh_decree": "osh_enforcement_ready.jsonl",
  "osh_safety": "osh_safety_rules_ready.jsonl",
  # "osh_law": "osh_law_ready.jsonl",  # 법 본문 파일명만 지정하면 동일하게 처리
}

indices = {}
for coll, path in paths.items():
    rows  = load_jsonl(path)
    docs  = to_docs(rows, coll)
    idx   = build_index(coll, docs)
    indices[coll] = idx
