#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from llama_index.core import Document, VectorStoreIndex, StorageContext
import json
from pathlib import Path

SRC = Path("laws_index.jsonl")

# 1️⃣ JSONL 로드 → Document 리스트 구성
docs = []
with SRC.open("r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        rec = json.loads(line)
        text = rec.get("text", "").strip()
        metadata = {k: v for k, v in rec.items() if k != "text"}
        docs.append(Document(text=text, metadata=metadata))

print(f"✅ 문서 수: {len(docs)}개")

# 2️⃣ 인덱스 생성
index = VectorStoreIndex.from_documents(docs)

# 3️⃣ 로컬 저장
index.storage_context.persist(persist_dir="./index_store")

print("💾 인덱스 생성 완료: ./index_store/")
