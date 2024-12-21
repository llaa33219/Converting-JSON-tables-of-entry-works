import csv
import json
import uuid
import os
from datetime import datetime

def modify_cell(value):
    """
    각 셀의 데이터를 수정하는 함수.
    예시: 문자열인 경우 'modified_' 접두사 추가
    필요에 따라 수정 로직을 변경하세요.
    
    :param value: 셀의 원본 값
    :return: 수정된 값
    """
    if isinstance(value, str):
        return f"modified_{value}"
    return value

def csv_to_custom_json_streaming(input_csv, output_json, table_id="jmol"):
    """
    CSV 파일을 읽고, 데이터를 수정한 후 사용자가 제공한 형식의 JSON 파일로 저장합니다.
    대용량 데이터를 스트리밍 방식으로 처리합니다.
    
    :param input_csv: 입력 CSV 파일 경로
    :param output_json: 출력 JSON 파일 경로
    :param table_id: JSON 내 테이블의 ID (기본값: "jmol")
    """
    if not os.path.exists(input_csv):
        print(f"입력 파일 '{input_csv}'이(가) 존재하지 않습니다.")
        return

    if os.path.getsize(input_csv) == 0:
        print(f"입력 파일 '{input_csv}'이(가) 비어 있습니다.")
        return

    with open(output_json, 'w', encoding='utf-8') as jsonfile, open(input_csv, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        fields = reader.fieldnames
        if not fields:
            print("CSV 파일에 헤더가 없습니다.")
            return

        # JSON의 시작 부분 작성
        jsonfile.write('{\n  "tables": [\n    {\n')
        jsonfile.write(f'      "id": "{table_id}",\n')
        jsonfile.write(f'      "fields": {json.dumps(fields, ensure_ascii=False)},\n')
        jsonfile.write(f'      "name": "{os.path.basename(input_csv)}",\n')
        jsonfile.write('      "object": null,\n')
        
        # 'data' 배열 시작
        jsonfile.write('      "data": [\n')
        first_data_entry = True
        
        # 'origin' 배열 시작
        jsonfile.write('      ],\n')  # 'data' 배열 닫기
        jsonfile.write('      "origin": [\n')
        first_origin_entry = True
        
        # Reset CSV reader to read from the beginning
        csvfile.seek(0)
        reader = csv.DictReader(csvfile)
        
        for row_num, row in enumerate(reader, start=1):
            # 데이터 수정
            modified_values = [modify_cell(value) for value in row.values()]
            original_values = list(row.values())
            
            # 고유 키 생성 (UUID 사용)
            unique_key = str(uuid.uuid4())
            
            # 'data' 항목 생성
            data_entry = {
                "key": unique_key,
                "value": modified_values
            }
            data_entry_json = json.dumps(data_entry, ensure_ascii=False)
            
            # 'data' 배열에 항목 추가
            if not first_data_entry:
                jsonfile.write(',\n')
            jsonfile.write(f'        {data_entry_json}')
            first_data_entry = False
            
            # 'origin' 항목 추가
            origin_entry_json = json.dumps(original_values, ensure_ascii=False)
            if not first_origin_entry:
                jsonfile.write(',\n')
            jsonfile.write(f'        {origin_entry_json}')
            first_origin_entry = False
            
            if row_num % 100000 == 0:
                print(f"{row_num}행 처리 완료.")
        
        # 'origin' 배열 닫기
        jsonfile.write('\n      ],\n')
        
        # 나머지 필드 작성
        jsonfile.write('      "chart": [],\n')
        jsonfile.write('      "isCloud": false,\n')
        jsonfile.write('      "cloudDate": null,\n')
        updated_time = datetime.utcnow().isoformat() + "Z"
        jsonfile.write(f'      "updated": "{updated_time}"\n')
        
        # 테이블 및 JSON 닫기
        jsonfile.write('    }\n  ]\n}\n')

    print(f"JSON 파일 '{output_json}'이(가) 성공적으로 생성되었습니다.")

if __name__ == "__main__":
    input_csv = 'questions_answers_merged.csv'            # 입력 CSV 파일 경로
    output_json = '32232323.json'         # 출력 JSON 파일 경로
    csv_to_custom_json_streaming(input_csv, output_json)
