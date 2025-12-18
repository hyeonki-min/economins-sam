from common.slack import send_slack_message
import requests
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import calendar
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

    updated = False
    for item in existing:
        if item["x"] == ym:
            item["y"] = price
            updated = True
            break

    if not updated:
        existing.append({"x": ym, "y": price})

    existing.sort(key=lambda x: x["x"])
    upload_json(existing)

    return {
        "status": "SUCCESS",
        "ym": ym,
        "price": price,
        "updated": updated,
    }


def lambda_handler(event, context):
    try:
        result = run()

        send_slack_message(
            service=f"KRX | {OUTPUT_KEY}",
            message=f"{result.get('ym')}: {result.get('price')}",
            status=result["status"],
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
