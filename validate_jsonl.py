#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JSONL 스키마 검증기 (Pydantic v2)
- provisions.jsonl / documents.jsonl / edges.jsonl 구조를 점검합니다.
- CONFIG만 맞춰 놓고:  python validate_jsonl.py  로 실행하세요.
"""
from __future__ import annotations
import os, sys, json
from typing import List, Optional, Dict, Literal
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

# =========================
# 설정
# =========================
CONFIG = {
    "dir": "./out",                      # JSONL들이 있는 폴더
    "provisions": "provisions.jsonl",
    "documents": "documents.jsonl",
    "edges": "edges.jsonl",              # 없으면 자동 스킵

    # ⚙️ 옵션: 특정 level 레코드는 아예 검증에서 제외(무시)
    #   예: PDF 서문 등으로 생성된 "문서" 레벨
    "ignore_levels": {"문서"},

    "strict_edges": False,               # True면 edges의 to_id 누락도 오류 처리
    "max_errors_per_file": 50,           # 파일당 에러 리포트 최대치
}

# =========================
# Pydantic 모델 정의
# =========================
Level = Literal["문서","장","절","조","항","호","목","세목"]

class SourceMeta(BaseModel):
    file_name: str
    page_range: List[int] = Field(min_length=2, max_length=2)

    @field_validator("page_range")
    @classmethod
    def pages_positive(cls, v: List[int]):
        if len(v) == 2 and (v[0] < 0 or v[1] < 0 or v[0] > v[1]):
            raise ValueError("page_range must be [start,end] with non-negative and start<=end")
        return v

class ProvisionNode(BaseModel):
    id: str
    doc_id: str
    doc_type: str
    law_name_ko: str
    abbrev: Optional[str] = None
    title: Optional[str] = None

    level: Level
    path_display: List[str]
    path_norm: List[str]

    label_display: Optional[str] = None
    label_norm: Optional[str] = None

    text_raw: Optional[str] = None
    text_clean: Optional[str] = None
    parent_ids: List[str]

    article_no: Optional[int] = None
    chapter_no: Optional[int] = None
    section_no: Optional[int] = None

    # hang/ho/mok/semok은 값이 없을 수 있으므로 Optional 허용
    unit_index: Dict[str, Optional[object]]

    effective_from: str
    effective_to: Optional[str] = None
    is_current: bool
    promulgation_date: Optional[str] = None
    enforcement_date: Optional[str] = None

    source: SourceMeta

    @field_validator("path_norm")
    @classmethod
    def check_path_norm(cls, v: List[str]):
        allow = {"jang","jeol","jo","hang","ho","mok","semok"}
        for seg in v:
            if ":" not in seg:
                raise ValueError(f"path_norm segment missing colon: {seg}")
            k,_ = seg.split(":",1)
            if k not in allow:
                raise ValueError(f"invalid path_norm key: {k}")
        return v

    @model_validator(mode="after")
    def _require_labels_for_sublevels(self):
        """
        cross-field 검증:
        - level이 항/호/목/세목이면 label_display/label_norm이 반드시 필요
        """
        if self.level in {"항","호","목","세목"}:
            if not self.label_display or not self.label_norm:
                raise ValueError(f"{self.level}에는 label_display/label_norm이 필요합니다")
        return self

class DocumentMeta(BaseModel):
    doc_id: str
    doc_type: str
    official_name_ko: str
    abbrev: Optional[str] = None
    issuer: Optional[str] = None
    promulgation_no: Optional[str] = None
    promulgation_date: Optional[str] = None
    enforcement_date: Optional[str] = None
    source_files: List[Dict[str,str]]

class EdgeRecord(BaseModel):
    edge_id: str
    edge_type: Literal["위임","세부화","준용","단순참조"]
    from_id: str
    to_id: Optional[str] = None
    anchors: List[str]
    match_confidence: float

# =========================
# 유틸
# =========================
def validate_jsonl(path: str, model, max_errors: int, *, ignore_levels: set[str] | None = None, extra_check=None):
    """
    - ignore_levels에 포함된 레코드는 '유효 처리(skip)'로 집계
    - extra_check(obj, ln): 추가 도메인 검증 훅
    """
    total = 0            # 파일 내 유효 행 수(빈 줄 제외)
    ok = 0               # 검증 통과 + 무시(skip) 포함
    skipped = 0          # ignore_levels로 스킵된 건수
    errors = []

    if not os.path.exists(path):
        return {"exists": False, "total": 0, "ok": 0, "skipped": 0, "errors": ["파일이 없습니다."]}

    with open(path, "r", encoding="utf-8") as f:
        for ln, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            total += 1
            try:
                raw = json.loads(s)

                # 레벨 무시 로직
                if ignore_levels and isinstance(raw, dict):
                    lv = raw.get("level")
                    if lv in ignore_levels:
                        skipped += 1
                        ok += 1  # 스킵을 통과로 간주
                        continue

                obj = model.model_validate(raw)  # 사전 파싱 후 검증
                if extra_check:
                    extra_check(obj, ln)
                ok += 1

            except ValidationError as e:
                errors.append(f"[line {ln}] {e}")
            except Exception as e:
                errors.append(f"[line {ln}] {type(e).__name__}: {e}")

            if len(errors) >= max_errors:
                errors.append(f"... 에러가 {max_errors}개를 넘어 더 이상 표시하지 않습니다.")
                break

    return {"exists": True, "total": total, "ok": ok, "skipped": skipped, "errors": errors}

def extra_edges_check(obj: EdgeRecord, ln: int):
    if CONFIG["strict_edges"] and obj.to_id is None:
        raise ValueError("to_id 없음(strict_edges=True)")

def report(title: str, res: dict):
    print(f"{title}")
    if not res["exists"]:
        print("  ❌ 파일을 찾을 수 없습니다.")
        return
    print(f"  - 총 라인(유효): {res['total']}, 통과(스킵 포함): {res['ok']}, 스킵: {res.get('skipped',0)}, 오류: {len(res['errors'])}")
    for e in res["errors"]:
        print("    •", e)

# =========================
# 메인
# =========================
def main():
    base = CONFIG["dir"]
    paths = {
        "provisions": os.path.join(base, CONFIG["provisions"]),
        "documents":  os.path.join(base, CONFIG["documents"]),
        "edges":      os.path.join(base, CONFIG["edges"]),
    }

    print(f"🔎 검증 디렉터리: {os.path.abspath(base)}")

    # provisions
    r1 = validate_jsonl(
        paths["provisions"],
        ProvisionNode,
        CONFIG["max_errors_per_file"],
        ignore_levels=CONFIG.get("ignore_levels", set())
    )
    report("\n[1/3] provisions.jsonl 검사:", r1)

    # documents
    r2 = validate_jsonl(
        paths["documents"],
        DocumentMeta,
        CONFIG["max_errors_per_file"]
    )
    report("\n[2/3] documents.jsonl 검사:", r2)

    # edges (없으면 스킵)
    if os.path.exists(paths["edges"]):
        r3 = validate_jsonl(
            paths["edges"],
            EdgeRecord,
            CONFIG["max_errors_per_file"],
            extra_check=extra_edges_check
        )
        report("\n[3/3] edges.jsonl 검사:", r3)
    else:
        print("\n[3/3] edges.jsonl 검사:")
        print("  - 파일 없음: 스킵")

    # 요약
    print("\n✅ 요약")
    for name, res in (("provisions", r1), ("documents", r2)):
        ok, total = res["ok"], res["total"]
        status = "OK" if (total > 0 and ok == total and not res["errors"]) else "CHECK"
        print(f"  {name}: {ok}/{total} valid (skip 포함 {res.get('skipped',0)}) → {status}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n중단됨")
        sys.exit(1)
