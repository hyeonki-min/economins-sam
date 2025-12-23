from common.slack import send_slack_message
import os
import gzip
import json
import time
import hashlib
import requests
import boto3
from botocore.exceptions import ClientError
from lxml import etree
from datetime import datetime, timedelta
from typing import Optional

# =========================
# Environment / Config
# =========================

S3_BUCKET = os.environ["S3_BUCKET_NAME"]
TRADE_TYPE = os.environ.get("TRADE_TYPE", "SALE")
SERVICE_KEY = os.environ["DATA_GO_KR_API_KEY"]
PUBLIC_API_URL = os.environ["PUBLIC_API_URL"]

HEADERS = {"User-Agent": "real-estate-etl/1.0"}

s3 = boto3.client("s3")

# =========================
# Exceptions
# =========================

class RateLimitDetected(Exception):
    pass

# =========================
# Utilities
# =========================

def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def iterate_months(start: datetime, end: datetime):
    cur = start.replace(day=1)
    while cur <= end:
        yield cur.strftime("%Y%m")
        cur = (cur + timedelta(days=32)).replace(day=1)

def s3_prefix(lawd_cd: str, deal_ymd: str) -> str:
    return (
        f"raw/"
        f"trade_type={TRADE_TYPE}/"
        f"deal_ymd={deal_ymd}/"
        f"lawd_cd={lawd_cd}"
    )

def target_deal_ymd(month_offset: int) -> str:
    """
    실행 시점 기준 완료된 월에서 offset만큼 과거
    offset=0 → 지난달
    offset=1 → 2개월 전
    offset=2 → 3개월 전
    """
    today = datetime.utcnow()

    # 완료된 마지막 월 = 지난달
    year = today.year
    month = today.month - 1
    if month == 0:
        month = 12
        year -= 1

    # offset 적용
    for _ in range(month_offset):
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    return f"{year}{month:02d}"

# =========================
# S3 Helpers
# =========================

def load_districts() -> list:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key="meta/district_code.json")
        r = s3.get_object(Bucket=S3_BUCKET, Key="meta/district_code.json")
        return json.loads(r["Body"].read())
    except ClientError as e:
        raise
    
def load_latest_s3(prefix: str):
    key = f"{prefix}/latest.json"
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("NoSuchKey", "404", "403"):
            return None
        raise

def save_latest_s3(prefix: str, payload: dict):
    key = f"{prefix}/latest.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )

def upload_snapshot_s3(prefix: str, snapshot_date: str, xml_bytes: bytes):
    key = f"{prefix}/snapshots/{snapshot_date}.xml.gz"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=gzip.compress(xml_bytes),
        ContentType="application/gzip",
    )

def log_failure_s3(*, failures: list[dict], run_id: str, deal_ymd: str):
    date = datetime.utcnow().strftime("%Y-%m-%d")
    key = f"logs/failures/{date}/deal_ymd={deal_ymd}/run_{run_id}.json"

    payload = {
        "run_id": run_id,
        "deal_ymd": deal_ymd,
        "failure_count": len(failures),
        "logged_at": datetime.utcnow().isoformat(),
        "failures": failures,
    }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

# =========================
# API Call & Validation
# =========================

def fetch_and_check(lawd_cd: str, deal_ymd: str) -> bytes:
    resp = requests.get(
        PUBLIC_API_URL,
        params={
            "serviceKey": SERVICE_KEY,
            "pageNo": "1",
            "numOfRows": "9999",
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ymd,
        },
        headers=HEADERS,
        timeout=10,
    )

    if resp.status_code == 429:
        raise RateLimitDetected("HTTP 429 Too Many Requests")
    if resp.status_code >= 500:
        raise RuntimeError(f"HTTP {resp.status_code}")

    xml_bytes = resp.content
    check_api_status(xml_bytes)
    return xml_bytes

def check_api_status(xml_bytes: bytes):
    root = etree.fromstring(xml_bytes)
    code = root.xpath("//*[local-name()='resultCode']/text()")
    msg  = root.xpath("//*[local-name()='resultMsg']/text()")

    result_code = code[0] if code else None
    result_msg  = msg[0] if msg else ""

    if result_code == "22":
        raise RateLimitDetected(f"Quota exceeded: {result_msg}")
    if result_code and result_code != "000":
        raise RuntimeError(f"API error {result_code}: {result_msg}")

def count_items_from_xml(xml_bytes: bytes) -> Optional[int]:
    try:
        root = etree.fromstring(xml_bytes)
        return len(root.xpath("//*[local-name()='item']"))
    except Exception:
        return None
    
# =========================
# Core Logic
# =========================

def process_one(lawd_cd: str, deal_ymd: str, region_name: str):
    snapshot_date = datetime.utcnow().strftime("%Y-%m-%d")
    prefix = s3_prefix(lawd_cd, deal_ymd)

    xml_bytes = fetch_and_check(lawd_cd, deal_ymd)
    content_hash = sha256(xml_bytes)

    latest = load_latest_s3(prefix)
    if latest and latest.get("content_hash") == content_hash:
        print(f"[SKIP] {lawd_cd} {deal_ymd} no change")
        return

    upload_snapshot_s3(prefix, snapshot_date, xml_bytes)

    record_count = count_items_from_xml(xml_bytes)

    latest_payload = {
        "trade_type": TRADE_TYPE,
        "region_name": region_name,
        "lawd_cd": lawd_cd,
        "deal_ymd": deal_ymd,
        "latest_snapshot_date": snapshot_date,
        "latest_file": f"snapshots/{snapshot_date}.xml.gz",
        "content_hash": content_hash,
        "record_count": record_count,
        "checked_at": datetime.utcnow().isoformat(),
    }

    save_latest_s3(prefix, latest_payload)
    print(f"[UPDATED] {lawd_cd} {deal_ymd} records={record_count}")

# =========================
# Runner
# =========================

def run(event: dict):
    month_offset = event.get("month_offset", 0)
    deal_ymd = target_deal_ymd(month_offset)

    districts = load_districts()

    run_id = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    failures: list[dict] = []

    for region in districts:
        lawd_cd = region["lawd_cd"]
        region_name = region["region_name"]

        try:
            process_one(lawd_cd, deal_ymd, region_name)
            time.sleep(1)

        except RateLimitDetected:
            raise

        except Exception as e:
            failures.append({
                "trade_type": TRADE_TYPE,
                "region_name": region_name,
                "lawd_cd": lawd_cd,
                "deal_ymd": deal_ymd,
                "error": str(e),
                "occurred_at": datetime.utcnow().isoformat(),
            })

    if failures:
        log_failure_s3(
            failures=failures,
            run_id=run_id,
            deal_ymd=deal_ymd,
        )
    
    total = len(districts)
    failed = len(failures)
    
    if failed == 0:
        status = "SUCCESS"
    elif failed < total:
        status = "PARTIAL_FAILURE"
    else:
        status = "FAILURE"
    return {
        "status": status,
        "deal_ymd": deal_ymd,
        "run_id": run_id,
        "total": total,
        "failed": failed,
        "failures": failures,
    }

# =========================
# Lambda Handler
# =========================

def lambda_handler(event, context):
    try:
        result = run(event)
        status = result["status"]

        if status == "SUCCESS":
            send_slack_message(
                service=f"Molit | {TRADE_TYPE}",
                message=f"성공\n deal_ymd:{result['deal_ymd']}\n 성공 {result['total']} / {result['total']}\n",
                status="SUCCESS",
            )
        elif status == "PARTIAL_FAILURE":
            send_slack_message(
                service=f"Molit | {TRADE_TYPE}",
                message=f"부분 실패\n deal_ymd:{result['deal_ymd']}\n 실패 {result['failed']} / {result['total']}\n",
                status="SUCCESS",
            )
        else:
            send_slack_message(
                service=f"Molit | {TRADE_TYPE}",
                message=f"실패\n deal_ymd:{result['deal_ymd']}\n 실패 {result['failed']} / {result['total']}\n",
                status="SUCCESS",
            )
        return {
            "statusCode": 200,
            "body": json.dumps({"status": "SUCCESS"}, ensure_ascii=False),
        }
    except RateLimitDetected:
        send_slack_message(
            service=f"Molit | {TRADE_TYPE}",
            message=f"429",
            status="ERROR",
        )
        return {
            "statusCode": 429,
            "body": json.dumps({"status": "RATE_LIMIT"}, ensure_ascii=False),
        }
    except Exception as e:
        send_slack_message(
            service=f"Molit | {TRADE_TYPE}",
            message=str(e),
            status="ERROR",
        )
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "ERROR", "message": str(e)}, ensure_ascii=False),
        }
