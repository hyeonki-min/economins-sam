from common.slack import send_slack_message
import requests
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import calendar
import hashlib
import json
import os
import time

s3 = boto3.client("s3")

BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
OUTPUT_KEY = os.environ["S3_OUTPUT_KEY"]
KRX_API_KEY = os.environ["KRX_API_KEY"]
INDEX_TYPE = os.environ["INDEX_TYPE"]

RETRYABLE_STATUS = {401, 403, 429}


def get_kospi_close_price(date_str: str) -> float | None:
    headers = {"AUTH_KEY": KRX_API_KEY}
    url = f"http://data-dbg.krx.co.kr/svc/apis/idx/{INDEX_TYPE}"
    params = {"basDd": date_str}

    for attempt in range(1, 4):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            if r.status_code == 200:
                for item in r.json().get("OutBlock_1", []):
                    if item.get("IDX_NM") in ("코스피", "코스닥"):
                        return float(item["CLSPRC_IDX"].replace(",", ""))

            elif r.status_code not in RETRYABLE_STATUS:
                return None

        except requests.exceptions.RequestException:
            pass

        if attempt < 3:
            time.sleep(3)

    return None


def get_last_trading_day_of_month(year: int, month: int):
    date = datetime(year, month, calendar.monthrange(year, month)[1])

    while date.month == month:
        if date.weekday() < 5:
            price = get_kospi_close_price(date.strftime("%Y%m%d"))
            if price is not None:
                return date.strftime("%Y-%m"), price
        date -= timedelta(days=1)

    return None


def load_existing_data() -> list:
    try:
        r = s3.get_object(Bucket=BUCKET_NAME, Key=OUTPUT_KEY)
        return json.loads(r["Body"].read())
    except ClientError as e:
        raise


def upload_json(data: list):
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=OUTPUT_KEY,
        Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=3600",
    )


def move_to_prev_month(now: datetime) -> datetime:
    month = 12 if now.month == 1 else now.month - 1
    year = now.year - 1 if now.month == 1 else now.year
    return datetime(year, month, 1)


def hash_list(data: list) -> str:
    """
    리스트 전체를 안정적으로 해시하기 위해
    key 정렬 + UTF-8 인코딩 후 SHA256 적용
    """
    raw = json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def run():
    existing = load_existing_data()
    if not isinstance(existing, list):
        raise RuntimeError("Existing data is not a list")

    prev_month = move_to_prev_month(datetime.utcnow())
    result = get_last_trading_day_of_month(prev_month.year, prev_month.month)

    if not result:
        return {
            "status": "NO_DATA",
            "ym": None,
            "price": None,
        }

    ym, price = result

    # -----------------------------
    # 1️⃣ 새 리스트 생성
    # -----------------------------
    new_list = list(existing)

    found = False
    for item in new_list:
        if item["x"] == ym:
            item["y"] = price
            found = True
            break

    if not found:
        new_list.append({"x": ym, "y": price})

    new_list.sort(key=lambda x: x["x"])

    # -----------------------------
    # 2️⃣ 개수 감소 방지
    # -----------------------------
    old_count = len(existing)
    new_count = len(new_list)

    if new_count < old_count:
        return {
            "status": "SKIPPED_SHRINK",
            "old_count": old_count,
            "new_count": new_count,
        }

    # -----------------------------
    # 3️⃣ 해시 비교
    # -----------------------------
    old_hash = hash_list(existing)
    new_hash = hash_list(new_list)

    if old_hash == new_hash:
        return {
            "status": "NO_CHANGE",
            "count": old_count,
            "hash": old_hash,
        }

    # -----------------------------
    # 4️⃣ 변경 발생 시 업로드
    # -----------------------------
    upload_json(new_list)

    return {
        "status": "SUCCESS",
        "old_count": old_count,
        "new_count": new_count,
        "old_hash": old_hash,
        "new_hash": new_hash,
    }

def lambda_handler(event, context):
    try:
        result = run()

        send_slack_message(
            service=f"KRX | {OUTPUT_KEY}",
            result=result,
        )

        return {
            "statusCode": 200,
            "body": json.dumps(result, ensure_ascii=False),
        }

    except Exception as e:
        send_slack_message(
            service=f"KRX | {OUTPUT_KEY}",
            message=str(e),
            status="ERROR",
        )
        raise
