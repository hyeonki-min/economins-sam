from common.slack import send_slack_message
from datetime import datetime, timedelta
import json
import os

import boto3
from boto3.dynamodb.conditions import Key
from openai import OpenAI

# ---------------------------
# Clients
# ---------------------------
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

table = dynamodb.Table(os.environ["DDB_TABLE"])
BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

# ---------------------------
# OpenAI Batch 결과 로딩
# ---------------------------
def load_batch_output(batch_id: str) -> list[dict]:
    batch = client.batches.retrieve(batch_id)

    if batch.status != "completed":
        raise RuntimeError(f"Batch not completed: {batch.status}")

    output_file = client.files.content(batch.output_file_id)
    return [json.loads(line) for line in output_file.text.splitlines()]


def parse_batch_jsonl(jsonl_rows: list[dict]) -> list[dict]:
    results = []

    for row in jsonl_rows:
        custom_id = row.get("custom_id")
        error = row.get("error")
        response = row.get("response")

        if error:
            results.append({
                "id": custom_id,
                "error": error,
            })
            continue

        try:
            content = response["body"]["choices"][0]["message"]["content"]
            results.append(json.loads(content))
        except Exception as e:
            results.append({
                "id": custom_id,
                "error": f"json_parse_error: {e}",
            })

    return results


# ---------------------------
# DynamoDB helpers
# ---------------------------
def fetch_pending_jobs(code: str) -> list[dict]:
    resp = table.query(
        IndexName="code-status-index",
        KeyConditionExpression=Key("code").eq(code) & Key("status").eq("pending"),
    )
    return resp.get("Items", [])


def update_status(code_type: str, new_status: str):
    table.update_item(
        Key={"code_type": code_type},
        UpdateExpression="SET #s = :s",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": new_status},
    )


# ---------------------------
# S3 저장
# ---------------------------
def save_to_s3(code: str, type_: str, results: list[dict]) -> str:
    key = f"monetary-policy/{code}/{type_}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(results, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=3600",
    )
    return key


# ---------------------------
# 날짜 유틸
# ---------------------------
PUBLISH_DATES = [
    (1, 16), (2, 25), (4, 17), (5, 29),
    (7, 10), (8, 28), (10, 23), (11, 27),
]

def get_target_report_month(today=None):
    today = today or datetime.today()
    year = today.year

    release_days = [(m, datetime(year, m, d)) for m, d in PUBLISH_DATES]
    release_days.sort(key=lambda x: x[1])

    target = None
    for m, available in release_days:
        if today >= available:
            target = m
        else:
            break
    return target


def get_code(today=None) -> str | None:
    today = today or datetime.today()
    month = get_target_report_month(today)
    if not month:
        return None
    return f"{today.year}-{month:02d}"


# ---------------------------
# Core Job
# ---------------------------
def run():
    code = get_code()

    if not code:
        return {
            "status": "NO_DATA",
            "reason": "no report month",
        }

    pending_jobs = fetch_pending_jobs(code)

    if not pending_jobs:
        return {
            "status": "NO_DATA",
            "code": code,
            "reason": "no pending batch jobs",
        }

    results = []

    for job in pending_jobs:
        batch_id = job["batch_id"]
        type_ = job["type"]
        code_type = f"{job['code']}#{type_}"

        try:
            jsonl_rows = load_batch_output(batch_id)
            parsed = parse_batch_jsonl(jsonl_rows)
            s3_key = save_to_s3(job["code"], type_, parsed)

            update_status(code_type, "completed")

            results.append({
                "code_type": code_type,
                "batch_id": batch_id,
                "s3_key": s3_key,
                "status": "SUCCESS",
            })

        except Exception as e:
            update_status(code_type, "error")
            results.append({
                "code_type": code_type,
                "batch_id": batch_id,
                "status": "ERROR",
                "error": str(e),
            })

    return {
        "status": "SUCCESS",
        "code": code,
        "processed": len(results),
        "results": results,
    }


# ---------------------------
# Lambda Handler
# ---------------------------
def lambda_handler(event, context):
    try:
        result = run()

        send_slack_message(
            service="BOK | Batch Result Processor",
            message=json.dumps(result, ensure_ascii=False),
            status=result["status"],
        )

        return {
            "statusCode": 200,
            "body": json.dumps(result, ensure_ascii=False),
        }

    except Exception as e:
        send_slack_message(
            service="BOK | Batch Result Processor",
            message=str(e),
            status="ERROR",
        )
        raise
