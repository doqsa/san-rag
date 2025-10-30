# 완전 로컬 무료 임베딩 기반 법령 RAG 구축 예제

import os
from llama_index.core import SimpleDirectoryReader, VectorStoreIndex, Settings
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core.node_parser import SentenceSplitter

# 0) 전역 설정: HuggingFace 한국어 임베딩, 보수적 청킹
Settings.embed_model = HuggingFaceEmbedding(model_name="jhgan/ko-sbert-nli")
Settings.node_parser = SentenceSplitter(chunk_size=1200, chunk_overlap=80)

# 1) 법령 텍스트 문서 로딩 (폴더 내 모든 .txt 자동 인식)
documents = SimpleDirectoryReader("./laws").load_data()

# 2) 문서 인덱싱 (벡터 색인 생성)
index = VectorStoreIndex.from_documents(documents)

# 3) 쿼리 엔진 설정 (top-5 chunk 유사도 검색, compact 응답 모드)
query_engine = index.as_query_engine(similarity_top_k=5, response_mode="compact")

# 4) 질의 예시
query = "지붕에서 작업할 때 필요한 안전난간 설치 기준을 알려줘"
response = query_engine.query(query)

# 5) 결과 출력
print(str(response))
