#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
법령 PDF(폴더 내 *.pdf 전부) → 클린 텍스트(.txt) 일괄 변환
- pdfminer.six 기반
- 페이지별 반복 라인(머리글/꼬리글) 자동 탐지·제거 (빈도+위치)
- URL/페이지 카운터/사이트명(법제처·국가법령정보센터 등) 제거
- 줄끝 하이픈 복원, 공백 정리
- '제1장 총칙' 이전 및 '부칙' 이후 자동 제외
- 문단 내 불필요한 줄바꿈 제거(soft wrap 해제), 문단(\n\n) 유지
- 탐지된 머리글·꼬리글을 파일별 로그(.log)로 저장 옵션
- 결과물은 laws/ 폴더 아래에 저장
"""

import re
import argparse
from pathlib import Path
from collections import Counter
from typing import List, Tuple, Dict

from pdfminer.high_level import extract_text

# --------- 패턴/유틸 ---------
URL_LINE = re.compile(r"https?://\S+", re.IGNORECASE)
PAGE_LINE = re.compile(r"^\s*(?:Page\s*)?\d+\s*(?:/\s*\d+)?\s*$", re.IGNORECASE)
SITE_HINTS = ("법제처", "국가법령정보센터", "National Law", "www.law.go.kr", "law.go.kr")
DASH_RULE = re.compile(r"^\s*[-–—]{3,}\s*$")
LINE_HYPHEN_BREAK = re.compile(r"-\n(?=\S)")  # 줄 끝 하이픈으로 강제 줄바꿈된 경우 붙이기
MULTI_EMPTY = re.compile(r"\n{3,}")          # 빈 줄 3개 이상 → 2개
SPACES = re.compile(r"[ \u00A0]{2,}")        # 공백 2개 이상 → 1개

HEAD_LIKE = re.compile(r"^(제\s*\d+\s*장|제\s*\d+\s*절|제\s*\d+\s*관|제\s*\d+\s*편)\b")

def normalize_for_detection(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    s = s.lower()
    s = re.sub(r"\d+", "", s)
    s = re.sub(r"[^\w가-힣 ]+", "", s)
    return s.strip()

def looks_like_noise_line(s: str) -> bool:
    if URL_LINE.search(s): return True
    if PAGE_LINE.match(s): return True
    if any(h in s for h in SITE_HINTS): return True
    if DASH_RULE.match(s): return True
    if len(s.strip()) <= 1: return True
    return False

def split_pages(raw_text: str) -> List[str]:
    raw_text = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    pages = raw_text.split("\x0c")
    if pages and not pages[-1].strip():
        pages = pages[:-1]
    return pages

def page_to_lines(page: str) -> List[str]:
    return page.split("\n")

def detect_headers_footers(
    pages_lines: List[List[str]],
    top_k: int = 5,
    bottom_k: int = 5,
    min_pages: int = 3,
    min_ratio: float = 0.3,
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """상·하단 후보(top_k/bottom_k 라인)에서 반복되는 정규화 문자열을 빈도 기반으로 탐지"""
    n = len(pages_lines)
    head_freq, foot_freq = Counter(), Counter()

    for lines in pages_lines:
        if not lines: 
            continue
        top = [l for l in lines[:top_k] if l.strip()]
        bot = [l for l in lines[-bottom_k:] if l.strip()]
        top_norms = {normalize_for_detection(l) for l in top if l.strip()}
        bot_norms = {normalize_for_detection(l) for l in bot if l.strip()}
        for t in top_norms:
            if t: head_freq[t] += 1
        for b in bot_norms:
            if b: foot_freq[b] += 1

    head = {k:v for k,v in head_freq.items() if v >= min_pages and v >= int(n*min_ratio) and len(k) >= 2}
    foot = {k:v for k,v in foot_freq.items() if v >= min_pages and v >= int(n*min_ratio) and len(k) >= 2}
    return head, foot

def should_drop_line(line: str, headers:set, footers:set) -> bool:
    s = line.strip()
    if not s: return True
    if looks_like_noise_line(s): return True
    norm = normalize_for_detection(s)
    if norm in headers: return True
    if norm in footers: return True
    return False

def slice_main_body(text: str) -> str:
    start = re.search(r"제\s*1\s*장\s*총\s*칙", text)
    end = re.search(r"\n\s*부\s*칙\s*\n", text)
    sidx = start.start() if start else 0
    eidx = end.start() if end else len(text)
    return text[sidx:eidx].strip()

def soft_unwrap_paragraphs(text: str) -> str:
    text = MULTI_EMPTY.sub("\n\n", text)
    paragraphs = re.split(r"\n{2,}", text)

    fixed_parts: List[str] = []
    for para in paragraphs:
        lines = para.split("\n")
        buf: List[str] = []
        for i, line in enumerate(lines):
            line = line.rstrip()
            if not line.strip():
                continue

            if HEAD_LIKE.match(line.strip()):
                if buf and not buf[-1].endswith("\n"):
                    buf.append("\n")
                buf.append(line.strip())
                buf.append("\n")
                continue

            next_line = lines[i+1] if i+1 < len(lines) else ""
            last_char = line.strip()[-1] if line.strip() else ""
            next_is_head = bool(HEAD_LIKE.match(next_line.strip()))
            is_sentence_end = bool(re.search(r"[\.!?\u2026:;·]$", last_char))

            if next_is_head:
                buf.append(line.strip())
                buf.append("\n")
            elif is_sentence_end:
                buf.append(line.strip())
                buf.append("\n")
            else:
                if buf and (buf[-1].endswith("\n") or buf[-1].endswith(" ")):
                    buf.append(line.strip())
                else:
                    if buf:
                        buf.append(" " + line.strip())
                    else:
                        buf.append(line.strip())

        joined = "".join(buf).strip()
        fixed_parts.append(joined)

    return "\n\n".join(part for part in fixed_parts if part)

def pdf_to_clean_text(pdf_path: Path, top_k=5, bottom_k=5, log_detected: bool=False) -> Tuple[str, Dict[str,int], Dict[str,int]]:
    raw = extract_text(str(pdf_path)) or ""
    pages = split_pages(raw)
    pages_lines = [page_to_lines(p) for p in pages]

    headers, footers = detect_headers_footers(
        pages_lines, top_k=top_k, bottom_k=bottom_k, min_pages=3, min_ratio=0.3
    )
    header_set, footer_set = set(headers.keys()), set(footers.keys())

    kept: List[str] = []
    for lines in pages_lines:
        for line in lines:
            if should_drop_line(line, header_set, footer_set):
                continue
            kept.append(line)

    text = "\n".join(kept)
    text = LINE_HYPHEN_BREAK.sub("", text)
    text = text.replace("\t", " ")
    text = SPACES.sub(" ", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = MULTI_EMPTY.sub("\n\n", text).strip()
    text = slice_main_body(text)

    text = re.sub(r"제\s*(\d+)\s*장", r"제\1장", text)
    text = re.sub(r"제\s*(\d+)\s*절", r"제\1절", text)
    text = re.sub(r"제\s*(\d+)\s*관", r"제\1관", text)
    text = re.sub(r"제\s*(\d+)\s*조", r"제\1조", text)
    text = re.sub(r"제\s*(\d+)\s*항", r"제\1항", text)
    text = re.sub(r"제\s*(\d+)\s*호", r"제\1호", text)

    text = soft_unwrap_paragraphs(text)

    return text, headers, footers

def main():
    ap = argparse.ArgumentParser(description="폴더 내 PDF 일괄 정제 → laws/ 폴더에 저장")
    ap.add_argument("--src", type=str, default=".", help="PDF가 있는 폴더(기본=현재 폴더)")
    ap.add_argument("--glob", type=str, default="*.pdf", help="파일 패턴(기본=*.pdf)")
    ap.add_argument("--top-k", type=int, default=5, help="상단에서 탐지할 최대 라인 수/페이지")
    ap.add_argument("--bottom-k", type=int, default=5, help="하단에서 탐지할 최대 라인 수/페이지")
    ap.add_argument("--log-detected", action="store_true", help="탐지된 헤더/푸터를 파일별 .log로 저장")
    args = ap.parse_args()

    src = Path(args.src)
    out_dir = src / "laws"
    out_dir.mkdir(exist_ok=True)

    pdfs = sorted(src.glob(args.glob))
    if not pdfs:
        raise SystemExit(f"[오류] {src} 에서 '{args.glob}' 패턴 PDF를 찾지 못했습니다.")

    print(f"[정보] 대상 파일 {len(pdfs)}개: {[p.name for p in pdfs]}")
    print(f"[출력 폴더] {out_dir.resolve()}")

    for pdf_path in pdfs:
        try:
            txt, headers, footers = pdf_to_clean_text(
                pdf_path,
                top_k=args.top_k,
                bottom_k=args.bottom_k,
                log_detected=args.log_detected
            )

            out_txt = out_dir / (pdf_path.stem + ".txt")
            out_txt.write_text(txt, encoding="utf-8")
            print(f"✅ {pdf_path.name} → laws/{out_txt.name}")

            if args.log_detected:
                log_path = out_dir / (pdf_path.stem + ".log")
                lines = ["=== Detected Headers (normalized) ==="]
                for k,v in sorted(headers.items(), key=lambda x: -x[1]):
                    lines.append(f"{k}\t({v} pages)")
                lines.append("\n=== Detected Footers (normalized) ===")
                for k,v in sorted(footers.items(), key=lambda x: -x[1]):
                    lines.append(f"{k}\t({v} pages)")
                log_path.write_text("\n".join(lines), encoding="utf-8")
                print(f"   ↳ 탐지 로그 저장: laws/{log_path.name}")

        except Exception as e:
            print(f"❌ {pdf_path.name} 처리 실패: {e}")

    print("\n[완료] 모든 PDF 변환 종료. 결과는 laws/ 폴더에 저장되었습니다.")

if __name__ == "__main__":
    main()
