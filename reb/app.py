from common.slack import send_slack_message
import hashlib
import json
import boto3
from datetime import datetime
import os
import requests
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
BASE_URL = "https://www.reb.or.kr/r-one/openapi/SttsApiTblData.do"

BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
OUTPUT_KEY = os.environ["S3_OUTPUT_KEY"]
REB_API_KEY = os.environ["REB_API_KEY"]
STATBL_ID = os.environ["STATBL_ID"]
CLS_ID = os.environ["CLS_ID"]
GRP_ID = os.environ.get("GRP_ID")
ITM_ID = os.environ.get("ITM_ID")

PARAMS = {
    "KEY": REB_API_KEY,
    "Type": "json",
    "STATBL_ID": STATBL_ID,
    "DTACYCLE_CD": "MM",
    "CLS_ID": CLS_ID,
    "pSize": 1000,
}

if GRP_ID:
    PARAMS["GRP_ID"] = GRP_ID
if ITM_ID:
    PARAMS["ITM_ID"] = ITM_ID


def transform_data(data: dict) -> list:
    result = []
    rows = data.get("SttsApiTblData", [])[1].get("row", [])

    for item in rows:
        date_str = (
            item.get("WRTTIME_DESC", "")
            .replace(" ", "")
            .replace("년", "-")
            .replace("월", "")
        )
        try:
            date_fmt = datetime.strptime(date_str, "%Y-%m")
            value = round(float(item.get("DTA_VAL", 0)), 1)
            result.append({"x": date_fmt.strftime("%Y-%m"), "y": value})
        except Exception:
            continue

    result.sort(key=lambda x: x["x"])
    return result


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


def hash_list(data: list) -> str:
    """
    리스트 전체를 안정적으로 해시하기 위해
    key 정렬 + UTF-8 인코딩 후 SHA256 적용
    """
    raw = json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def run():
    resp = requests.get(BASE_URL, params=PARAMS, timeout=10)
    resp.raise_for_status()

    transformed = transform_data(resp.json())

    if not transformed:
        return {
            "status": "NO_DATA",
            "count": 0,
        }

    # -----------------------------
    # 1️⃣ 기존 데이터 로드
    # -----------------------------
    existing = load_existing_data()
    if not isinstance(existing, list):
        raise RuntimeError("Existing data is not a list")

    old_count = len(existing)
    new_count = len(transformed)

    # -----------------------------
    # 2️⃣ 개수 감소 방지
    # -----------------------------
    if new_count < old_count:
        return {
            "status": "SKIPPED_SHRINK",
            "old_count": old_count,
            "new_count": new_count,
        }

    # -----------------------------
    # 3️⃣ 정렬 (해시 안정성)
    # -----------------------------
    existing_sorted = sorted(existing, key=lambda x: x["x"])
    transformed_sorted = sorted(transformed, key=lambda x: x["x"])

    old_hash = hash_list(existing_sorted)
    new_hash = hash_list(transformed_sorted)

    # -----------------------------
    # 4️⃣ 동일 데이터면 skip
    # -----------------------------
    if old_hash == new_hash:
        return {
            "status": "NO_CHANGE",
            "count": old_count,
            "hash": old_hash,
        }

    # -----------------------------
    # 5️⃣ 변경 발생 시 업로드
    # -----------------------------
    upload_json(transformed_sorted)

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
            service=f"REB | {OUTPUT_KEY}",
            result=result
        )

        return {
            "statusCode": 200,
            "body": json.dumps(result, ensure_ascii=False),
        }

    except Exception as e:
        send_slack_message(
            service=f"REB | {OUTPUT_KEY}",
            message=str(e),
            status="ERROR",
        )
        raise
