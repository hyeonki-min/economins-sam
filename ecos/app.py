import json
import boto3
import os
import requests
from datetime import datetime

# === 설정 ===
s3 = boto3.client('s3')
BASE_URL = 'https://ecos.bok.or.kr/api/StatisticSearch'

# === 환경변수 불러오기 ===
def get_config():
    return {
        "BUCKET_NAME": os.environ["S3_BUCKET_NAME"],
        "OUTPUT_KEY": os.environ["S3_OUTPUT_KEY"],
        "ECOS_API_KEY": os.environ["ECOS_API_KEY"],
        "STAT_CODE": os.environ["STAT_CODE"],
        "ITEM_CODE": os.environ.get("ITEM_CODE", ""),
        "ITEM_CODE2": os.environ.get("ITEM_CODE2", ""),
        "CYCLE": os.environ["CYCLE"]
    }

# === 날짜 기본값 유틸 ===
def get_default_date(cycle: str, type_: str) -> str:
    now = datetime.now()
    if type_ == 'start':
        return '199601' if cycle == 'M' else '1996Q1'
    elif type_ == 'end':
        if cycle == 'M':
            return now.strftime('%Y%m')
        elif cycle == 'Q':
            quarter = (now.month - 1) // 3 + 1
            return f"{now.year}Q{quarter}"
    raise ValueError("type_ must be either 'start' or 'end'")

# === API URL 생성 ===
def build_api_url(config: dict, start_date: str, end_date: str) -> str:
    base = (
        f"{BASE_URL}/{config['ECOS_API_KEY']}/json/kr/1/1000/"
        f"{config['STAT_CODE']}/{config['CYCLE']}/{start_date}/{end_date}/{config['ITEM_CODE']}"
    )
    
    if config.get('ITEM_CODE2'):
        base += f"/{config['ITEM_CODE2']}"
    
    return base

# === 데이터 가공 ===
def transform_data(data: dict) -> list:
    result = []
    rows = data.get("StatisticSearch", {}).get("row", [])
    
    for item in rows:
        time_str = item.get("TIME", "").replace(" ", "").replace("년", "").replace("월", "")
         # 분기 데이터 처리 (예: 2003Q1)
        if 'Q' in time_str:
            try:
                year, quarter = time_str[:4], int(time_str[-1])
                month_end = quarter * 3
                date_key = f"{year}-{month_end:02d}"

                # 분기 시작의 2개월도 null로 추가
                for m in range(month_end - 2, month_end):
                    null_key = f"{year}-{m:02d}"
                    result.append({"x": null_key, "y": None})
                
                value = round(float(item.get("DATA_VALUE", 0)), 1)
                result.append({"x": date_key, "y": value})
            except Exception:
                continue

        # 월별 데이터 처리 (예: 202405)
        else:
            try:
                date_fmt = datetime.strptime(time_str, "%Y%m")
                date_key = date_fmt.strftime("%Y-%m")
                value = round(float(item.get("DATA_VALUE", 0)), 1)
                result.append({"x": date_key, "y": value})
            except Exception:
                continue
    
    return result

# === S3 업로드 ===
def upload_to_s3(bucket: str, key: str, content: str):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=content.encode('utf-8'),
        ContentType='application/json'
    )

# === Lambda 핸들러 ===
def lambda_handler(event, context):
    config = get_config()

    start_date = get_default_date(config['CYCLE'], 'start')
    end_date = get_default_date(config['CYCLE'], 'end')
    url = build_api_url(config, start_date, end_date)

    try:
        resp = requests.get(url)
        resp.raise_for_status()
        raw_data = resp.json()
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"API 요청 실패: {str(e)}"
        }

    try:
        transformed = transform_data(raw_data)
        json_string = json.dumps(transformed, ensure_ascii=False)
        upload_to_s3(config["BUCKET_NAME"], config["OUTPUT_KEY"], json_string)
        return {
            'statusCode': 200,
            'body': f'파일 업로드 완료: s3://{config["BUCKET_NAME"]}/{config["OUTPUT_KEY"]}'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"S3 업로드 실패 또는 데이터 변환 오류: {str(e)}"
        }
