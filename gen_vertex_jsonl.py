#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF(국내 법령) → Vertex AI Search JSONL (Structured / JSONL with document IDs)

- 출력: 라인당 한 개 JSON 객체 (top-level은 id + structData 만 사용)
  {
    "id": "문서ID(영문/숫자/_/- 만)",
    "structData": {
      "title": "...",          # 표시 제목
      "text": "...",           # 검색 본문
      "uri": "...",            # 원문 경로(선택)
      "law_name_ko": "...",    # 이하 메타 (필터/파싯에 활용)
      "doc_type": "...",
      "level": "...",          # 장/절/조/항/호/목/세목
      "path_display": "...",
      "version_date": "YYYY-MM-DD",
      "doc_id": "...",
      "abbrev": "...",
      "path_norm": ["jang:1","jo:2","hang:1", ...],
      "article_no": 1,
      "chapter_no": 1,
      "section_no": 1,
      "effective_from": "YYYY-MM-DD",
      "promulgation_date": "YYYY-MM-DD",
      "enforcement_date": "YYYY-MM-DD",
      "source_file": "파일명.pdf",
      "page_range": [시작, 끝]  # 1-based
    }
  }

- 의존: pypdf  (pip install pypdf)
- 서문(“문서” 레벨)은 저장하지 않음
"""

import os
import re
import json
import hashlib
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from pypdf import PdfReader

# ===========================================================
# 0) 변환할 문서 작업 목록
# ===========================================================
JOBS = [
    {
        "pdf": r"산업안전보건법_20251001.pdf",
        "doc_id": "kr-osh-act",
        "doc_type": "법률",
        "official_name_ko": "산업안전보건법",
        "abbrev": "산안법",
        "promulgation_date": "2024-12-31",
        "enforcement_date": "2025-10-01",
        # "uri": "gs://your-bucket/laws/산업안전보건법_20251001.pdf",
    },
    {
        "pdf": r"산업안전보건법 시행령_20250621.pdf",
        "doc_id": "kr-osh-dec",
        "doc_type": "시행령",
        "official_name_ko": "산업안전보건법 시행령",
        "abbrev": "산안법 시행령",
        "promulgation_date": "2024-12-31",
        "enforcement_date": "2025-06-21",
    },
    {
        "pdf": r"산업안전보건법 시행규칙_20251001.pdf",
        "doc_id": "kr-osh-rul",
        "doc_type": "시행규칙",
        "official_name_ko": "산업안전보건법 시행규칙",
        "abbrev": "산안법 시행규칙",
        "promulgation_date": "2024-12-31",
        "enforcement_date": "2025-07-01",
    },
    {
        "pdf": r"산업안전보건기준에관한규칙_20251001.pdf",
        "doc_id": "kr-osh-std",
        "doc_type": "규칙",
        "official_name_ko": "산업안전보건기준에 관한 규칙",
        "abbrev": "산안기준규칙",
        "promulgation_date": "2025-07-01",
        "enforcement_date": "2025-07-15",
    },
]

OUTDIR = "./out_vertex"

# ===========================================================
# 1) 패턴 (장-절-조-항-호-목-세목)
# ===========================================================
PATTERN_JANG = re.compile(r"^제\s*(\d+)\s*장")
PATTERN_JEOL = re.compile(r"^제\s*(\d+)\s*절")
PATTERN_JO   = re.compile(r"^제\s*(\d+)\s*조")
PATTERN_HANG = re.compile(r"^([①-⑳])")
PATTERN_HO   = re.compile(r"^(\d+)\.")
PATTERN_MOK  = re.compile(r"^([가-힣])\.")
PATTERN_SEMOK= re.compile(r"^(\d+)\)")
PATTERN_TITLE= re.compile(r"^(제\s*\d+\s*조)\s*\(([^)]+)\)")

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
# 2) 파싱 상태
# ===========================================================
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
# 3) 도우미
# ===========================================================
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

def build_vertex_title(job: Dict, state: ParseState) -> str:
    if state.article_no:
        if state.article_title:
            return f"{job['official_name_ko']} 제{state.article_no}조({state.article_title})" + \
                   (" " + display_hang(state.hang) if state.hang else "")
        base = f"{job['official_name_ko']} 제{state.article_no}조"
        return base + (f" {display_hang(state.hang)}" if state.hang else "")
    pd = " > ".join(build_path_display(state))
    return f"{job['official_name_ko']} {pd}" if pd else job['official_name_ko']

# ---- 안전한 문서 ID 생성 (영문/숫자/_/- 만, 길이 제한) -------------------------
def make_id(doc_id: str, state: ParseState, enforcement_date: str) -> str:
    parts = [doc_id]
    if state.article_no is not None: parts.append(str(state.article_no))
    if state.hang  is not None: parts.append(str(state.hang))
    if state.ho    is not None: parts.append(str(state.ho))
    if state.mok   is not None: parts.append(state.mok)
    if state.semok is not None: parts.append(f"{state.semok}p")
    parts.append(enforcement_date.replace("-", ""))  # YYYYMMDD

    raw = "_".join(parts)
    safe = re.sub(r"[^A-Za-z0-9_-]", "_", raw)      # 허용문자만
    safe = re.sub(r"_+", "_", safe).strip("_")

    MAX_LEN = 120
    if len(safe) > MAX_LEN:
        h = hashlib.sha1(safe.encode("utf-8")).hexdigest()[:8]
        safe = (safe[:MAX_LEN-9] + "_" + h).strip("_")

    return safe or "doc_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]

# ===========================================================
# 4) PDF → JSONL 라인
# ===========================================================
def parse_pdf_to_vertex_lines(job: Dict) -> List[str]:
    reader = PdfReader(job["pdf"])
    out_lines: List[str] = []
    state = ParseState()

    def flush_vertex_record():
        if not state.buffer_lines:
            return
        lvl = current_level(state)
        if lvl == "문서":
            state.buffer_lines.clear(); state.start_page = None; state.end_page = None
            return

        content = re.sub(r"\s+", " ", "\n".join(state.buffer_lines).strip())
        if not content:
            state.buffer_lines.clear(); state.start_page = None; state.end_page = None
            return

        vid = make_id(job["doc_id"], state, job["enforcement_date"])
        title = build_vertex_title(job, state)
        path_disp = " > ".join(build_path_display(state))

        record = {
            "id": vid,
            "structData": {
                "title": title,
                "text": content,
                "uri": job.get("uri") or os.path.basename(job["pdf"]),
                "law_name_ko": job["official_name_ko"],
                "doc_type": job["doc_type"],
                "level": lvl,
                "path_display": path_disp,
                "version_date": job.get("enforcement_date"),
                "doc_id": job["doc_id"],
                "abbrev": job.get("abbrev"),
                "path_norm": build_path_norm(state),
                "article_no": state.article_no,
                "chapter_no": state.chapter_no,
                "section_no": state.section_no,
                "effective_from": job.get("enforcement_date"),
                "promulgation_date": job.get("promulgation_date"),
                "enforcement_date": job.get("enforcement_date"),
                "source_file": os.path.basename(job["pdf"]),
                "page_range": [ (state.start_page or 0) + 1, (state.end_page or 0) + 1 ],
            }
        }
        out_lines.append(json.dumps(record, ensure_ascii=False))
        state.buffer_lines.clear(); state.start_page = None; state.end_page = None

    def update_title_from_line(line: str):
        m = PATTERN_TITLE.search(line)
        if m:
            state.article_title = m.group(2).strip()

    # 페이지 순회
    for pidx, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        for line in text.splitlines():
            s = line.strip()
            if not s:
                continue

            m = PATTERN_JANG.match(s)
            if m:
                flush_vertex_record(); state.reset_lower("장")
                state.chapter_no = int(m.group(1)); continue

            m = PATTERN_JEOL.match(s)
            if m:
                flush_vertex_record(); state.reset_lower("절")
                state.section_no = int(m.group(1)); continue

            m = PATTERN_JO.match(s)
            if m:
                flush_vertex_record(); state.reset_lower("조")
                state.article_no = int(m.group(1)); update_title_from_line(s); continue

            m = PATTERN_HANG.match(s)
            if m:
                flush_vertex_record(); state.reset_lower("항")
                state.hang = CIRCLED_MAP.get(m.group(1)); s = s[len(m.group(1)):].strip()
            else:
                m = PATTERN_HO.match(s)
                if m and state.article_no is not None:
                    flush_vertex_record(); state.reset_lower("호")
                    state.ho = int(m.group(1)); s = s[m.end():].strip()
                else:
                    m = PATTERN_MOK.match(s)
                    if m and state.article_no is not None:
                        flush_vertex_record(); state.reset_lower("목")
                        state.mok = normalize_mok(m.group(1)); s = s[m.end():].strip()
                    else:
                        m = PATTERN_SEMOK.match(s)
                        if m and state.article_no is not None:
                            flush_vertex_record(); state.reset_lower("세목")
                            state.semok = int(m.group(1)); s = s[m.end():].strip()

            # 본문 축적
            if s:
                if state.start_page is None: state.start_page = pidx
                state.end_page = pidx
                state.buffer_lines.append(s)
                if state.article_no: update_title_from_line(s)

    flush_vertex_record()
    return out_lines

# ===========================================================
# 5) 쓰기/실행
# ===========================================================
def write_lines(path: str, lines: List[str]):
    d = os.path.dirname(path)
    if d: os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for ln in lines:
            f.write(ln + "\n")

def run_all(jobs: List[Dict], outdir: str):
    os.makedirs(outdir, exist_ok=True)
    for job in jobs:
        required = ["pdf","doc_id","doc_type","official_name_ko","enforcement_date"]
        missing = [k for k in required if not job.get(k)]
        if missing:
            print(f"❌ 누락된 설정: {missing} (doc_id={job.get('doc_id')})"); continue
        if not os.path.exists(job["pdf"]):
            print(f"❌ PDF 파일이 없습니다: {job['pdf']} (doc_id={job['doc_id']})"); continue

        print(f"\n📘 처리: {job['official_name_ko']} (doc_id={job['doc_id']})")
        lines = parse_pdf_to_vertex_lines(job)
        out_path = os.path.join(outdir, f"vertex_{job['doc_id']}.jsonl")
        write_lines(out_path, lines)
        print(f"   [OK] {out_path}  (records={len(lines)})")

if __name__ == "__main__":
    print(f"🗂 출력 폴더: {os.path.abspath(OUTDIR)}")
    run_all(JOBS, OUTDIR)
    print("\n✅ 완료")



