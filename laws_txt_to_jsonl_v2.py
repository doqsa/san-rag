#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
laws/*.txt → laws_index.jsonl
- 산업안전보건법 계열 파일을 읽어 조문별로 구조화하고 메타데이터 포함 JSONL로 저장
- 아직 LlamaIndex 인덱싱은 하지 않음
"""

from __future__ import annotations
import re, json
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

SRC_DIR = Path("./laws")
OUT_PATH = Path("./laws_index.jsonl")

ZWSP = "\u200b\u200c\u200d\uFEFF"
NBSP = "\u00A0"

RE_JANG = re.compile(r"^\s*제\s*(\d+)\s*장\s*(.*)$")
RE_JO   = re.compile(r"^\s*제\s*(\d+)\s*조\s*(?:\(([^)]+)\))?\s*(.*)$")
RE_HANG = re.compile(r"^\s*([①②③④⑤⑥⑦⑧⑨⑩])\s*(.*)$")
RE_HO   = re.compile(r"^\s*(\d+)\.\s*(.*)$")
RE_MOK  = re.compile(r"^\s*([가-하])\.\s*(.*)$")
HANG_NUMS = {"①":1,"②":2,"③":3,"④":4,"⑤":5,"⑥":6,"⑦":7,"⑧":8,"⑨":9,"⑩":10}

def clean_lines(raw: str) -> list[str]:
    t = raw.replace("\r\n","\n").replace("\r","\n")
    t = t.translate({ord(c): None for c in ZWSP+NBSP})
    return [ln.rstrip() for ln in t.split("\n")]

def guess_meta_from_filename(fn: str) -> tuple[str,str]:
    base = Path(fn).stem
    m = re.search(r"(\d{8})$", base)
    if m:
        ver = f"{m.group(1)[:4]}-{m.group(1)[4:6]}-{m.group(1)[6:8]}"
        name = base[: -9] if base.endswith("_"+m.group(1)) else base
    else:
        ver = datetime.now().strftime("%Y-%m-%d"); name = base
    return name, ver

def parse_basic_law(text: str, law_name: str, version_date: str, file_name: str) -> list[dict[str,Any]]:
    lines = clean_lines(text)
    jang_no=None; jang_title=None
    jo_no=None; jo_title=None
    hang_no=None; ho_no=None; mok_no=None
    level="none"; buf=[]; records=[]
    sib={"jo":0,"hang":0,"ho":0,"mok":0}

    def reset(lv):
        if lv=="jang": sib.update({"jo":0,"hang":0,"ho":0,"mok":0})
        elif lv=="jo": sib.update({"hang":0,"ho":0,"mok":0})
        elif lv=="hang": sib.update({"ho":0,"mok":0})
        elif lv=="ho": sib.update({"mok":0})

    def flush(unit):
        nonlocal buf
        t="\n".join(buf).strip()
        if not t: buf.clear(); return
        rec = {
            "law_name_ko": law_name,
            "version_date": version_date,
            "file_name": file_name,
            "unit_type": unit,
            "jang_no": jang_no, "jang_title": jang_title,
            "jo_no": jo_no, "jo_title": jo_title,
            "hang_no": hang_no, "ho_no": ho_no, "mok_no": mok_no,
            "text": t
        }
        records.append(rec)
        buf.clear()

    for ln in lines:
        if not ln.strip(): continue
        if ln.strip().startswith("부칙"): break

        m=RE_JANG.match(ln)
        if m:
            if level!="none": flush(level)
            jang_no=int(m.group(1)); jang_title=(m.group(2) or "").strip() or None
            reset("jang"); level="jang"; continue

        m=RE_JO.match(ln)
        if m:
            if level in ("jo","hang","ho","mok"): flush(level)
            jo_no=int(m.group(1)); jo_title=(m.group(2) or "").strip() or None
            reset("jo"); level="jo"; sib["jo"]+=1; continue

        m=RE_HANG.match(ln)
        if m:
            if level in ("hang","ho","mok"): flush(level)
            hang_no=HANG_NUMS.get(m.group(1)); level="hang"; sib["hang"]+=1
            buf.append(m.group(2)); continue

        m=RE_HO.match(ln)
        if m:
            if level in ("ho","mok"): flush(level)
            ho_no=int(m.group(1)); level="ho"; sib["ho"]+=1
            buf.append(m.group(2)); continue

        m=RE_MOK.match(ln)
        if m:
            if level=="mok": flush(level)
            mok_no=m.group(1); level="mok"; sib["mok"]+=1
            buf.append(m.group(2)); continue

        buf.append(ln)
    if level!="none": flush(level)
    return records

def main():
    all_records=[]
    for path in sorted(SRC_DIR.glob("*.txt")):
        law_name, version_date = guess_meta_from_filename(path.name)
        text = path.read_text(encoding="utf-8", errors="ignore")
        recs = parse_basic_law(text, law_name, version_date, path.name)
        all_records.extend(recs)
        print(f"📘 {path.name}: {len(recs)} recs")

    with OUT_PATH.open("w", encoding="utf-8") as f:
        for r in all_records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"✅ Saved {len(all_records)} records to {OUT_PATH}")

if __name__ == "__main__":
    main()
