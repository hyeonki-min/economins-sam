import requests
from datetime import datetime, timedelta
import boto3
import calendar
import json
import os
import time

# --- 설정 --- #
s3 = boto3.client('s3')

BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
OUTPUT_KEY = os.environ.get("S3_OUTPUT_KEY")
KRX_API_KEY = os.environ.get("KRX_API_KEY")
INDEX_TYPE = os.environ.get("INDEX_TYPE")

# --- API 호출 및 코스피 종가 추출 --- #
def get_kospi_close_price(date_str: str) -> float | None:
    headers = {
        "AUTH_KEY": KRX_API_KEY,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    url = f"http://data-dbg.krx.co.kr/svc/apis/idx/{INDEX_TYPE}"
    params = {"basDd": date_str}

    max_retries = 3
    retry_delay = 3

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                for item in data.get("OutBlock_1", []):
                    if item.get("IDX_NM") in ["코스피", "코스닥"]:
                        return float(item["CLSPRC_IDX"])
                return None
            elif response.status_code == 403:
                print(f"[{date_str}] 403 Forbidden - {retry_delay}초 후 재시도 ({attempt}/{max_retries})")
            else:
                print(f"[{date_str}] HTTP 오류 {response.status_code} - 재시도 안 함")
                return None
        except requests.exceptions.RequestException as e:
            print(f"[{date_str}] 요청 예외 발생: {e} - {retry_delay}초 후 재시도 ({attempt}/{max_retries})")

        time.sleep(retry_delay)
    return None

# --- 월별 마지막 거래일 찾기 --- #
def get_last_trading_day_of_month(year: int, month: int) -> tuple[str, int] | None:
    last_day = calendar.monthrange(year, month)[1]
    date = datetime(year, month, last_day)

    while date.month == month:
        if date.weekday() < 5:
            date_str = date.strftime("%Y%m%d")
            close_price = get_kospi_close_price(date_str)
            if close_price is not None:
                ym = date.strftime("%Y-%m")
                print(f"[{ym}] {date_str} → {close_price}")
                return ym, close_price
        date -= timedelta(days=1)
    print(f"[{year}-{month:02}] 종가 데이터 없음")
    return None

# --- 기존 JSON 불러오기 --- #
def load_existing_data() -> list:
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=OUTPUT_KEY)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception:
        return []

# --- 전체 데이터 수집 --- #
def collect_historical_kospi(start_year: int, end_year: int):
    result = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            data = get_last_trading_day_of_month(year, month)
            if data:
                ym, close_price = data
                result.append({"x": ym, "y": close_price})
    result.sort(key=lambda x: x['x'])
    upload_json(result)
    print(f"📘 과거 데이터 저장 완료")

# --- S3 업로드 --- #
def upload_json(result):
    json_string = json.dumps(result, ensure_ascii=False)
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=OUTPUT_KEY,
            Body=json_string.encode('utf-8'),
            ContentType='application/json'
        )
        return {
            'statusCode': 200,
            'body': f'File uploaded to s3://{BUCKET_NAME}/{OUTPUT_KEY}'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error uploading to S3: {str(e)}'
        }

# --- 이전 달로 이동 --- #
def move_to_prev_month(date_obj: datetime) -> datetime:
    year = date_obj.year
    month = date_obj.month
    day = date_obj.day

    if month == 1:
        prev_month = 12
        year -= 1
    else:
        prev_month = month - 1

    # 이전 달의 최대 일 수 확인
    last_day = calendar.monthrange(year, prev_month)[1]
    corrected_day = min(day, last_day)

    return datetime(year, prev_month, corrected_day)

# --- 최신 월 데이터만 추가 --- #
def append_latest_kospi():
    result = load_existing_data()
    prev_month = move_to_prev_month(datetime.today())
    data = get_last_trading_day_of_month(prev_month.year, prev_month.month)
    if data:
        ym, close_price = data
        existing_data[ym] = {"x": ym, "y": close_price}

        updated_result = list(existing_data.values())
        updated_result.sort(key=lambda x: x['x'])

        upload_json(updated_result)

        print(f"✅ {ym} 데이터 {'업데이트' if ym in existing_data else '추가'} 완료")
    else:
        print(f"⚠️ {ym} 종가 데이터 없음")

def lambda_handler(event, context):
    append_latest_kospi()