#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
산업안전보건 영역 법령 PDF → RAG용 JSONL 자동 생성기 (멀티문서 배치 지원)
- provisions.jsonl  : 조문(장/절/조/항/호/목/세목) 노드들
- documents.jsonl   : 문서(법률/시행령/시행규칙/기준/지침) 메타
- edges.jsonl       : (옵션) 위임/세부화/준용/참조 후보 관계

특징
- 한 폴더에 여러 문서 결과를 저장: 파일명에 doc_id 접두사 자동 부여
- "문서" 레벨(서문 등)은 저장하지 않음
- 배치 처리: JOBS 리스트에 여러 문서를 추가해 한 번에 변환
"""

import re
import os
import json
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Tuple
from pypdf import PdfReader

# ===========================================================
# 0) 변환할 문서 작업 목록 (여기에 문서를 추가하세요)
#    - doc_id는 고유하게(예: kr-osh-act/dec/rul/std/gui-...)
#    - doc_type: "법률" / "시행령" / "시행규칙" / "규칙" / "지침"
# ===========================================================
JOBS = [
    # 예시) 산업안전보건법
    {
        "pdf": r"산업안전보건법_20251001.pdf",
        "doc_id": "kr-osh-act",
        "doc_type": "법률",
        "official_name_ko": "산업안전보건법",
        "abbrev": "산안법",
        "promulgation_date": "2024-12-31",
        "enforcement_date": "2025-10-01",
        "emit_edges": True
    },
    # 예시) 시행령
    {
        "pdf": r"산업안전보건법 시행령_20250621.pdf",
        "doc_id": "kr-osh-dec",
        "doc_type": "시행령",
        "official_name_ko": "산업안전보건법 시행령",
        "abbrev": "산안법 시행령",
        "promulgation_date": "2024-12-31",
        "enforcement_date": "2025-06-21",
        "emit_edges": True
    },
    # 예시) 시행규칙
    {
        "pdf": r"산업안전보건법 시행규칙_20251001.pdf",
        "doc_id": "kr-osh-rul",
        "doc_type": "시행규칙",
        "official_name_ko": "산업안전보건법 시행규칙",
        "abbrev": "산안법 시행규칙",
        "promulgation_date": "2024-12-31",
        "enforcement_date": "2025-07-01",
        "emit_edges": True
    },
    # 예시) 기준(산업안전보건기준에 관한 규칙)
    {
        "pdf": r"산업안전보건기준에관한규칙_20251001.pdf",
        "doc_id": "kr-osh-std",
        "doc_type": "규칙",
        "official_name_ko": "산업안전보건기준에 관한 규칙",
        "abbrev": "산안기준규칙",
        "promulgation_date": "2025-07-01",
        "enforcement_date": "2025-07-15",
        "emit_edges": True
    },
    # 예시) 안전보건공단 지침(KOSHA Guide 등)
    # {
    #     "pdf": r"KOSHA_G-21-2024.pdf",
    #     "doc_id": "kr-osh-gui-g21-2024",
    #     "doc_type": "지침",
    #     "official_name_ko": "KOSHA 가이드 G-21 (2024)",
    #     "abbrev": "KOSHA G-21",
    #     "promulgation_date": "2024-01-10",
    #     "enforcement_date": "2024-01-10",
    #     "emit_edges": False
    # },
]

# 결과를 저장할 **단일 폴더**
OUTDIR = "./out"  # 한 폴더로 통일 저장

# ===========================================================
# 1) 패턴 정의
# ===========================================================
PATTERN_JANG = re.compile(r"^제\s*(\d+)\s*장")
PATTERN_JEOL = re.compile(r"^제\s*(\d+)\s*절")
PATTERN_JO   = re.compile(r"^제\s*(\d+)\s*조")
PATTERN_HANG = re.compile(r"^([①-⑳])")
PATTERN_HO   = re.compile(r"^(\d+)\.")
PATTERN_MOK  = re.compile(r"^([가-힣])\.")
PATTERN_SEMOK = re.compile(r"^(\d+)\)")

PATTERN_TITLE = re.compile(r"^(제\s*\d+\s*조)\s*\(([^)]+)\)")

EDGE_RULES = [
    ("위임", r"대통령령으로 정한다|시행령으로 정한다"),
    ("세부화", r"시행규칙으로 정한다|규칙으로 정한다"),
    ("준용", r"준용한다"),
    ("단순참조", r"제\d+조(의\d+)?(제\d+항)?(제\d+호)?(에|를)?\s*(참조|따른다)")
]
EDGE_RULES_COMPILED = [(t, re.compile(p)) for t, p in EDGE_RULES]

CIRCLED_MAP = {
    "①": 1, "②": 2, "③": 3, "④": 4, "⑤": 5, "⑥": 6, "⑦": 7, "⑧": 8, "⑨": 9, "⑩": 10,
    "⑪": 11, "⑫": 12, "⑬": 13, "⑭": 14, "⑮": 15, "⑯": 16, "⑰": 17, "⑱": 18, "⑲": 19, "⑳": 20
}

def normalize_mok(ch: str) -> str:
    base = ord('가')
    idx = ord(ch) - base
    if idx < 0 or idx > 50:
        return 'a'
    return chr(ord('a') + idx)

# ===========================================================
# 2) 데이터 구조
# ===========================================================
@dataclass
class SourceMeta:
    file_name: str
    page_range: List[int]

@dataclass
class ProvisionNode:
    id: str
    doc_id: str
    doc_type: str
    law_name_ko: str
    abbrev: Optional[str]
    title: Optional[str]
    level: str
    path_display: List[str]
    path_norm: List[str]
    label_display: Optional[str]
    label_norm: Optional[str]
    text_raw: Optional[str]
    text_clean: Optional[str]
    parent_ids: List[str]
    article_no: Optional[int]
    chapter_no: Optional[int]
    section_no: Optional[int]
    unit_index: Dict[str, object]
    effective_from: str
    effective_to: Optional[str]
    is_current: bool
    promulgation_date: Optional[str]
    enforcement_date: Optional[str]
    source: SourceMeta

    def to_json(self) -> str:
        d = asdict(self)
        d["source"] = asdict(self.source)
        return json.dumps(d, ensure_ascii=False)

@dataclass
class DocumentMeta:
    doc_id: str
    doc_type: str
    official_name_ko: str
    abbrev: Optional[str]
    issuer: Optional[str]
    promulgation_no: Optional[str]
    promulgation_date: Optional[str]
    enforcement_date: Optional[str]
    source_files: List[Dict[str, str]]

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

@dataclass
class EdgeRecord:
    edge_id: str
    edge_type: str
    from_id: str
    to_id: Optional[str]
    anchors: List[str]
    match_confidence: float

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

@dataclass
class ParseState:
    chapter_no: Optional[int] = None
    section_no: Optional[int] = None
    article_no: Optional[int] = None
    article_title: Optional[str] = None
    hang: Optional[int] = None
    ho: Optional[int] = None
    mok: Optional[str] = None
    semok: Optional[int] = None
    buffer_lines: List[str] = field(default_factory=list)
    start_page: Optional[int] = None
    end_page: Optional[int] = None

    def reset_lower(self, level: str):
        order = ["장", "절", "조", "항", "호", "목", "세목"]
        idx = order.index(level)
        for lv in order[idx:]:
            if lv == "장": self.chapter_no = None
            elif lv == "절": self.section_no = None
            elif lv == "조":
                self.article_no = None
                self.article_title = None
            elif lv == "항": self.hang = None
            elif lv == "호": self.ho = None
            elif lv == "목": self.mok = None
            elif lv == "세목": self.semok = None
        self.buffer_lines.clear()
        self.start_page = None
        self.end_page = None

# ===========================================================
# 3) 헬퍼
# ===========================================================
def make_id(doc_id: str, state: ParseState, enforcement_date: str) -> str:
    parts = [doc_id]
    if state.article_no is not None: parts.append(str(state.article_no))
    if state.hang is not None: parts.append(str(state.hang))
    if state.ho is not None: parts.append(str(state.ho))
    if state.mok is not None: parts.append(state.mok)
    if state.semok is not None: parts.append(f"{state.semok}p")
    parts.append(enforcement_date.replace("-", ""))
    return ":".join(parts)

def display_hang(n: int) -> str:
    for k, v in CIRCLED_MAP.items():
        if v == n:
            return k
    return f"({n})"

def display_mok(a: str) -> str:
    idx = ord(a) - ord('a')
    return f"{chr(ord('가') + idx)}."

def build_path_display(state: ParseState) -> List[str]:
    path = []
    if state.chapter_no: path.append(f"제{state.chapter_no}장")
    if state.section_no: path.append(f"제{state.section_no}절")
    if state.article_no: path.append(f"제{state.article_no}조")
    if state.hang: path.append(display_hang(state.hang))
    if state.ho: path.append(f"{state.ho}.")
    if state.mok: path.append(display_mok(state.mok))
    if state.semok: path.append(f"{state.semok})")
    return path

def build_path_norm(state: ParseState) -> List[str]:
    path = []
    if state.chapter_no: path.append(f"jang:{state.chapter_no}")
    if state.section_no: path.append(f"jeol:{state.section_no}")
    if state.article_no: path.append(f"jo:{state.article_no}")
    if state.hang: path.append(f"hang:{state.hang}")
    if state.ho: path.append(f"ho:{state.ho}")
    if state.mok: path.append(f"mok:{state.mok}")
    if state.semok: path.append(f"semok:{state.semok}")
    return path

def current_level(state: ParseState) -> str:
    if state.semok is not None: return "세목"
    if state.mok is not None: return "목"
    if state.ho is not None: return "호"
    if state.hang is not None: return "항"
    if state.article_no is not None: return "조"
    if state.section_no is not None: return "절"
    if state.chapter_no is not None: return "장"
    return "문서"

# ===========================================================
# 4) 노드 flush (문서 레벨은 저장하지 않음)
# ===========================================================
def flush_node(nodes: List[ProvisionNode], job: dict, state: ParseState):
    if not state.buffer_lines:
        return
    lvl = current_level(state)
    if lvl == "문서":
        # 서문/머리말 등은 저장하지 않음
        state.buffer_lines.clear()
        state.start_page = None
        state.end_page = None
        return

    label_display = label_norm = None
    if lvl == "항":
        label_display = display_hang(state.hang)
        label_norm = str(state.hang)
    elif lvl == "호":
        label_display = f"{state.ho}."
        label_norm = str(state.ho)
    elif lvl == "목":
        label_display = display_mok(state.mok)
        label_norm = state.mok
    elif lvl == "세목":
        label_display = f"{state.semok})"
        label_norm = f"{state.semok}p"

    title = f"제{state.article_no}조({state.article_title})" if state.article_no and state.article_title else None
    text_raw = "\n".join(state.buffer_lines).strip()
    text_clean = re.sub(r"\s+", " ", text_raw).strip()

    # parent_ids 간단 구성(필요 시 상세 상향식 구성 추가 가능)
    parent_ids: List[str] = []

    nodes.append(ProvisionNode(
        id=make_id(job["doc_id"], state, job["enforcement_date"]),
        doc_id=job["doc_id"],
        doc_type=job["doc_type"],
        law_name_ko=job["official_name_ko"],
        abbrev=job.get("abbrev"),
        title=title,
        level=lvl,
        path_display=build_path_display(state),
        path_norm=build_path_norm(state),
        label_display=label_display,
        label_norm=label_norm,
        text_raw=text_raw,
        text_clean=text_clean,
        parent_ids=parent_ids,
        article_no=state.article_no,
        chapter_no=state.chapter_no,
        section_no=state.section_no,
        unit_index={"hang": state.hang, "ho": state.ho, "mok": state.mok, "semok": state.semok},
        effective_from=job["enforcement_date"],
        effective_to=None,
        is_current=True,
        promulgation_date=job.get("promulgation_date"),
        enforcement_date=job.get("enforcement_date"),
        source=SourceMeta(
            file_name=os.path.basename(job["pdf"]),
            page_range=[(state.start_page or 0) + 1, (state.end_page or 0) + 1]
        )
    ))
    state.buffer_lines.clear()
    state.start_page = None
    state.end_page = None

# ===========================================================
# 5) PDF 파서
# ===========================================================
def parse_pdf(job: dict) -> Tuple[List[ProvisionNode], List[EdgeRecord]]:
    reader = PdfReader(job["pdf"])
    nodes: List[ProvisionNode] = []
    edges: List[EdgeRecord] = []
    state = ParseState()
    edge_counter = 0

    def update_title(line: str):
        m = PATTERN_TITLE.search(line)
        if m:
            state.article_title = m.group(2).strip()

    for pidx, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        for line in text.splitlines():
            s = line.strip()
            if not s:
                continue

            # 계층 탐지
            m = PATTERN_JANG.match(s)
            if m:
                flush_node(nodes, job, state)
                state.reset_lower("장")
                state.chapter_no = int(m.group(1))
                continue

            m = PATTERN_JEOL.match(s)
            if m:
                flush_node(nodes, job, state)
                state.reset_lower("절")
                state.section_no = int(m.group(1))
                continue

            m = PATTERN_JO.match(s)
            if m:
                flush_node(nodes, job, state)
                state.reset_lower("조")
                state.article_no = int(m.group(1))
                update_title(s)
                # 조 제목 라인 이후 실제 본문은 항/호/목 등에서 수집
                continue

            m = PATTERN_HANG.match(s)
            if m:
                flush_node(nodes, job, state)
                state.reset_lower("항")
                state.hang = CIRCLED_MAP.get(m.group(1))
                s = s[len(m.group(1)):].strip()
            else:
                m = PATTERN_HO.match(s)
                if m and state.article_no is not None:
                    flush_node(nodes, job, state)
                    state.reset_lower("호")
                    state.ho = int(m.group(1))
                    s = s[m.end():].strip()
                else:
                    m = PATTERN_MOK.match(s)
                    if m and state.article_no is not None:
                        flush_node(nodes, job, state)
                        state.reset_lower("목")
                        state.mok = normalize_mok(m.group(1))
                        s = s[m.end():].strip()
                    else:
                        m = PATTERN_SEMOK.match(s)
                        if m and state.article_no is not None:
                            flush_node(nodes, job, state)
                            state.reset_lower("세목")
                            state.semok = int(m.group(1))
                            s = s[m.end():].strip()

            # 본문 축적
            if s:
                if state.start_page is None:
                    state.start_page = pidx
                state.end_page = pidx
                state.buffer_lines.append(s)
                if state.article_no:
                    update_title(s)

                # 관계(에지) 후보 추출
                if job.get("emit_edges"):
                    for etype, rx in EDGE_RULES_COMPILED:
                        if rx.search(s):
                            edge_counter += 1
                            edges.append(EdgeRecord(
                                edge_id=f"e-{edge_counter:06d}",
                                edge_type=etype,
                                from_id=make_id(job["doc_id"], state, job["enforcement_date"]),
                                to_id=None,  # 후처리 매핑 단계에서 연결 권장
                                anchors=[s[:120]],
                                match_confidence=0.70 if etype != "단순참조" else 0.55
                            ))

    # 마지막 버퍼 flush
    flush_node(nodes, job, state)
    return nodes, edges

# ===========================================================
# 6) JSONL 쓰기
# ===========================================================
def write_jsonl(path: str, rows: List[str]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(r)
            f.write("\n")

def build_document_meta(job: dict) -> DocumentMeta:
    return DocumentMeta(
        doc_id=job["doc_id"],
        doc_type=job["doc_type"],
        official_name_ko=job["official_name_ko"],
        abbrev=job.get("abbrev"),
        issuer=None,
        promulgation_no=None,
        promulgation_date=job.get("promulgation_date"),
        enforcement_date=job.get("enforcement_date"),
        source_files=[{"file_name": os.path.basename(job["pdf"])}]
    )

# ===========================================================
# 7) 실행부 (배치 처리)
# ===========================================================
def run_all(jobs: List[dict], outdir: str):
    os.makedirs(outdir, exist_ok=True)
    for job in jobs:
        # 필수 확인
        required = ["pdf","doc_id","doc_type","official_name_ko","enforcement_date"]
        missing = [k for k in required if not job.get(k)]
        if missing:
            print(f"❌ 누락된 설정: {missing} (doc_id={job.get('doc_id')})")
            continue

        pdf_path = job["pdf"]
        if not os.path.exists(pdf_path):
            print(f"❌ PDF 파일이 없습니다: {pdf_path} (doc_id={job['doc_id']})")
            continue

        print(f"\n📘 처리 시작: {job['official_name_ko']} (doc_id={job['doc_id']})")
        print(f"    PDF: {pdf_path}")
        nodes, edges = parse_pdf(job)

        # 파일명에 doc_id 접두사 부여 → 한 폴더에 모두 저장 가능
        prov_path = os.path.join(outdir, f"{job['doc_id']}_provisions.jsonl")
        doc_path  = os.path.join(outdir, f"{job['doc_id']}_documents.jsonl")
        edge_path = os.path.join(outdir, f"{job['doc_id']}_edges.jsonl")

        write_jsonl(prov_path, [n.to_json() for n in nodes])
        write_jsonl(doc_path,  [build_document_meta(job).to_json()])
        if job.get("emit_edges") and edges:
            write_jsonl(edge_path, [e.to_json() for e in edges])

        print(f"   [OK] {prov_path}")
        print(f"   [OK] {doc_path}")
        if job.get("emit_edges") and edges:
            print(f"   [OK] {edge_path}")
        else:
            print(f"   [i ] edges 미생성(emit_edges={job.get('emit_edges')}, 후보={len(edges)})")

if __name__ == "__main__":
    print(f"🗂 출력 폴더: {os.path.abspath(OUTDIR)}")
    run_all(JOBS, OUTDIR)
    print("\n✅ 모든 작업 완료")
