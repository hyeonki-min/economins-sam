from common.slack import send_slack_message
import hashlib
import json
import boto3
from datetime import datetime, timezone, timedelta
import os
import requests
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

# -----------------------------
# 환경 변수
# -----------------------------
BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
OUTPUT_KEY = os.environ["S3_OUTPUT_KEY"]

SYMBOL = os.environ.get("YAHOO_SYMBOL", "CL=F")


# -----------------------------
# 기존 데이터 로드
# -----------------------------
def load_existing_data() -> list:
    try:
        r = s3.get_object(Bucket=BUCKET_NAME, Key=OUTPUT_KEY)
        return json.loads(r["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return []
        raise


# -----------------------------
# 업로드
# -----------------------------
def upload_json(data: list):
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=OUTPUT_KEY,
        Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=3600",
    )


def fetch_recent_daily(symbol: str):
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=60)

    params = {
        "interval": "1d",
        "period1": int(start.timestamp()),
        "period2": int(now.timestamp()),
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    }
    
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
    resp = requests.get(url, params=params, timeout=10, headers=headers)
    resp.raise_for_status()

    data = resp.json()
    chart = data.get("chart", {})
    error = chart.get("error")
    if error:
        raise RuntimeError(f"Yahoo error: {error}")

    results = chart.get("result")
    if not results:
        raise RuntimeError("No result from Yahoo")

    result = results[0]
    
    if result["meta"]["dataGranularity"] != "1d":
        raise RuntimeError("Granularity not 1d")

    return result


def get_previous_month_last_close(result):
    timestamps = result["timestamp"]
    closes = result["indicators"]["quote"][0]["close"]

    now = datetime.now(timezone.utc)
    this_month = now.replace(day=1)
    prev_month_end = this_month - timedelta(days=1)
    prev_month_str = prev_month_end.strftime("%Y-%m")

    last_close = None
    last_ts = None

    for ts, close in zip(timestamps, closes):
        if close is None:
            continue

        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        ym = dt.strftime("%Y-%m")

        if ym == prev_month_str:
            if last_ts is None or ts > last_ts:
                last_ts = ts
                last_close = float(close)

    if last_close is None:
        raise RuntimeError("No data for previous month")

    return {
        "x": prev_month_str,
        "y": round(last_close, 2)
    }
    
def append_if_missing(existing, new_item):
    months = {item["x"] for item in existing}

    if new_item["x"] in months:
        return existing, False

    existing.append(new_item)
    existing.sort(key=lambda x: x["x"])
    return existing, True

# -----------------------------
# 메인 로직
# -----------------------------
def run():
    result = fetch_recent_daily(SYMBOL)
    new_month = get_previous_month_last_close(result)

    existing = load_existing_data()

    updated, appended = append_if_missing(existing, new_month)

    if not appended:
        return {
            "status": "NO_CHANGE",
            "month": new_month["x"]
        }

    upload_json(updated)

    return {
        "status": "SUCCESS",
        "appended_month": new_month["x"]
    }


# -----------------------------
# Lambda Entrypoint
# -----------------------------
def lambda_handler(event, context):
    try:
        result = run()

        send_slack_message(
            service=f"Yahoo Snapshot | {OUTPUT_KEY}",
            result=result
        )

        return {
            "statusCode": 200,
            "body": json.dumps(result, ensure_ascii=False),
        }

    except Exception as e:
        send_slack_message(
            service=f"Yahoo Snapshot | {OUTPUT_KEY}",
            message=str(e),
            status="ERROR",
        )
        raise
