import requests
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
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
    RETRYABLE_STATUS = {401, 403, 429}

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                for item in data.get("OutBlock_1", []):
                    if item.get("IDX_NM") in ("코스피", "코스닥"):
                        return float(item["CLSPRC_IDX"].replace(",", ""))
                return None

            if response.status_code in RETRYABLE_STATUS:
                print(f"[{date_str}] HTTP {response.status_code} - retry ({attempt}/{max_retries})")
            else:
                print(f"[{date_str}] HTTP {response.status_code} - no retry")
                return None

        except requests.exceptions.RequestException as e:
            print(f"[{date_str}] request error: {e} - retry ({attempt}/{max_retries})")

        if attempt < max_retries:
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
        r = s3.get_object(Bucket=BUCKET_NAME, Key=OUTPUT_KEY)
        return json.loads(r["Body"].read())
    except Exception:
        return []

# --- S3 업로드 --- #
def upload_json(data: list):
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=OUTPUT_KEY,
            Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json"
        )
    except ClientError as e:
        print("❌ S3 upload failed")
        print(e)
        raise

# --- 이전 달로 이동 --- #
def move_to_prev_month(now: datetime) -> datetime:
    year, month = now.year, now.month
    prev_month = 12 if month == 1 else month - 1
    year = year - 1 if month == 1 else year
    last_day = calendar.monthrange(year, prev_month)[1]
    return datetime(year, prev_month, min(now.day, last_day))

# --- 최신 월 데이터만 추가 --- #
def run():
    existing_data = load_existing_data()

    prev_month = move_to_prev_month(datetime.utcnow())
    result = get_last_trading_day_of_month(prev_month.year, prev_month.month)

    if not result:
        msg = f"No data for {prev_month.strftime('%Y-%m')}"
        print(f"⚠️ {msg}")
        return {
            "status": "NO_DATA",
            "message": msg
        }

    ym, close_price = result

    updated = False
    for item in existing_data:
        if item["x"] == ym:
            item["y"] = close_price
            updated = True
            break

    if not updated:
        existing_data.append({"x": ym, "y": close_price})

    existing_data.sort(key=lambda x: x["x"])
    upload_json(existing_data)

    msg = f"{ym} {'updated' if updated else 'inserted'}"
    print(f"✅ {msg}")

    return {
        "status": "OK",
        "ym": ym,
        "price": close_price,
        "updated": updated
    }

# ------------------------
# Slack 메시지
# ------------------------
def send_slack_message(text: str):
    webhook = os.environ["SLACK_WEBHOOK_URL"]
    payload = {"text": text}

    resp = requests.post(
        webhook,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
    )

def lambda_handler(event, context):
    result = run()
    msg = f"📌 Kospi/Kosdaq Batch S3 저장 완료!\n- {result['ym']}: {result['price']}"
    )
    send_slack_message(msg)

    return {
        "statusCode": 200,
        "body": json.dumps(result, ensure_ascii=False)
    }