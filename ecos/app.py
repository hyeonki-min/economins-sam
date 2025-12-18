from common.slack import send_slack_message
import json
import boto3
import os
import requests
from datetime import datetime
from botocore.exceptions import ClientError

# === AWS ===
s3 = boto3.client("s3")

# === Constants ===
BASE_URL = "https://ecos.bok.or.kr/api/StatisticSearch"

BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
OUTPUT_KEY = os.environ["S3_OUTPUT_KEY"]
ECOS_API_KEY = os.environ["ECOS_API_KEY"]
STAT_CODE = os.environ["STAT_CODE"]
ITEM_CODE = os.environ.get("ITEM_CODE", "")
ITEM_CODE2 = os.environ.get("ITEM_CODE2", "")
CYCLE = os.environ["CYCLE"]  # M | Q


# === Date Utils ===
def get_default_date(cycle: str, kind: str) -> str:
    now = datetime.utcnow()

    if kind == "start":
        return "199601" if cycle == "M" else "1996Q1"

    if kind == "end":
        if cycle == "M":
            return now.strftime("%Y%m")
        if cycle == "Q":
            quarter = (now.month - 1) // 3 + 1
            return f"{now.year}Q{quarter}"

    raise ValueError("kind must be 'start' or 'end'")


# === API URL Builder ===
def build_api_url(start_date: str, end_date: str) -> str:
    base = (
        f"{BASE_URL}/{ECOS_API_KEY}/json/kr/1/1000/"
        f"{STAT_CODE}/{CYCLE}/{start_date}/{end_date}/{ITEM_CODE}"
    )
    if ITEM_CODE2:
        base += f"/{ITEM_CODE2}"
    return base


# === Transform ===
def transform_data(data: dict) -> list:
    result = []
    rows = data.get("StatisticSearch", {}).get("row", [])

    for item in rows:
        time_str = (
            item.get("TIME", "")
            .replace(" ", "")
            .replace("년", "")
            .replace("월", "")
        )

        # 분기 데이터 (예: 2003Q1)
        if "Q" in time_str:
            try:
                year = time_str[:4]
                quarter = int(time_str[-1])
                month_end = quarter * 3

                # 분기 시작 2개월은 null
                for m in range(month_end - 2, month_end):
                    result.append({"x": f"{year}-{m:02d}", "y": None})

                value = round(float(item.get("DATA_VALUE", 0)), 1)
                result.append({"x": f"{year}-{month_end:02d}", "y": value})

            except Exception:
                continue

        # 월별 데이터 (예: 202405)
        else:
            try:
                date_fmt = datetime.strptime(time_str, "%Y%m")
                value = round(float(item.get("DATA_VALUE", 0)), 1)
                result.append(
                    {"x": date_fmt.strftime("%Y-%m"), "y": value}
                )
            except Exception:
                continue

    result.sort(key=lambda x: x["x"])
    return result


# === S3 Upload ===
def upload_json(data: list):
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=OUTPUT_KEY,
        Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=3600",
    )


# === Core Job ===
def run():
    start_date = get_default_date(CYCLE, "start")
    end_date = get_default_date(CYCLE, "end")
    url = build_api_url(start_date, end_date)

    resp = requests.get(url, timeout=10)
    resp.raise_for_status()

    transformed = transform_data(resp.json())

    if not transformed:
        return {
            "status": "NO_DATA",
            "count": 0,
        }

    upload_json(transformed)

    return {
        "status": "SUCCESS",
        "count": len(transformed),
    }


# === Lambda Handler ===
def lambda_handler(event, context):
    try:
        result = run()

        send_slack_message(
            service=f"ECOS | {OUTPUT_KEY}",
            message=f"rows={result.get('count')}",
            status=result["status"],
        )

        return {
            "statusCode": 200,
            "body": json.dumps(result, ensure_ascii=False),
        }

    except Exception as e:
        send_slack_message(
            service=f"ECOS | {OUTPUT_KEY}",
            message=str(e),
            status="ERROR",
        )
        raise
