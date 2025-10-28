import json
import os

# 파일 목록 정의
files_to_check = [
    'vertex_kr-osh-dec.jsonl',
    'vertex_kr-osh-rul.jsonl',
    'vertex_kr-osh-std.jsonl',
    'vertex_kr-osh-act.jsonl'
]

def validate_jsonl_files(files):
    """
    JSONL 파일들을 순회하며 필수 필드('id', 'title', 'content') 존재 여부 및 
    JSON 유효성을 검증하는 함수.
    """
    all_files_valid = True
    results = {}

    for filename in files:
        if not os.path.exists(filename):
            results[filename] = {"status": "File Not Found", "errors": []}
            all_files_valid = False
            continue

        errors = []
        is_valid = True
        
        with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)
                    
                    # 필수 필드 ('id', 'title', 'content') 검증
                    required_fields = ['id', 'title', 'content']
                    missing_fields = [field for field in required_fields if field not in data]
                    
                    if missing_fields:
                        errors.append(f"Line {line_num}: Missing required field(s): {', '.join(missing_fields)}. Sample: {line[:50]}...")
                        is_valid = False
                        
                    # 'title' 필드가 다른 대소문자로 존재하는지 추가 확인
                    if 'title' not in data and any('title' in k.lower() for k in data.keys()):
                        errors.append(f"Line {line_num}: 'title' field might be in wrong case (e.g., 'Title' or 'TITLE'). Please use all lowercase 'title'. Sample: {line[:50]}...")
                        is_valid = False
                        
                except json.JSONDecodeError as e:
                    errors.append(f"Line {line_num}: JSON Decoding Error: {e}. Sample: {line[:50]}...")
                    is_valid = False

        if errors:
            results[filename] = {"status": "Error Detected", "errors": errors}
            all_files_valid = False
        else:
            results[filename] = {"status": "Valid", "errors": []}
            
    return results, all_files_valid