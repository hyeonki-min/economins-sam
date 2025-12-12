from datetime import datetime, timedelta
import json
import os
from openai import OpenAI
import requests
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

TABLE_NAME = os.environ["DDB_TABLE"]
BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

table = dynamodb.Table(TABLE_NAME)

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])


# ---------------------------
# OpenAI Batch 결과 JSONL 로딩
# ---------------------------
def load_batch_output(batch_id):
    batch = client.batches.retrieve(batch_id)

    if batch.status != "completed":
        raise Exception(f"Batch not completed: {batch.status}")

    output_file = client.files.content(batch.output_file_id)
    raw_lines = output_file.text.splitlines()
    return [json.loads(line) for line in raw_lines]


def parse_batch_jsonl(jsonl_rows):
    results = []

    for data in jsonl_rows:
        custom_id = data.get("custom_id")
        response = data.get("response")
        error = data.get("error")

        if error:
            results.append({"id": custom_id, "error": error})
            continue

        try:
            content = response["body"]["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            results.append(parsed)
        except Exception as e:
            results.append({"id": custom_id, "error": f"json_parse_error: {e}"})

    return results


# ---------------------------
# GSI → pending job 조회
# ---------------------------
def fetch_pending_jobs(code: str):
    resp = table.query(
        IndexName="code-status-index",
        KeyConditionExpression=Key("code").eq(code) & Key("status").eq("pending")
    )
    return resp.get("Items", [])


# ---------------------------
# 상태 업데이트 (pending → completed)
# ---------------------------
def update_status(code_type, new_status):
    table.update_item(
        Key={"code_type": code_type},
        UpdateExpression="SET #s = :s",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": new_status}
    )


# ---------------------------
# S3에 저장
# ---------------------------
def save_to_s3(code, type_, results):
    key = f"monetary-policy/{code}/{type_}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(results, ensure_ascii=False),
        ContentType="application/json"
    )

    return key


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

PUBLISH_DATES = [
    (1, 16),
    (2, 25),
    (4, 17),
    (5, 29),
    (7, 10),
    (8, 28),
    (10, 23),
    (11, 27),
]


def get_target_report_month(today=None):
    if today is None:
        today = datetime.today()

    year = today.year
    release_days = []

    for (m, d) in PUBLISH_DATES:
        release_day = datetime(year, m, d)
        available = release_day + timedelta(days=0)
        release_days.append((m, available))

    release_days.sort(key=lambda x: x[1])

    target = None
    for (m, available) in release_days:
        if today >= available:
            target = m
        else:
            break
    return target

def get_code(today=None):
    if today is None:
        today = datetime.today()

    month = get_target_report_month()
    if month is None:
        return None

    year = today.year
    return f"{year}-{month:02d}"

# ---------------------------
# Lambda Handler
# ---------------------------
def lambda_handler(event, context):
    code = get_code()
    pending_jobs = fetch_pending_jobs(code)

    if not pending_jobs:
        return {"message": "No pending batch jobs"}

    outputs = []

    for job in pending_jobs:
        batch_id = job["batch_id"]
        type_ = job["type"]
        code = job["code"]
        code_type = f"{code}#{type_}"
        print(f"[INFO] Processing batch {batch_id} ({type_}, {code})")

        try:
            # 1) OpenAI Batch JSONL 로드
            jsonl_rows = load_batch_output(batch_id)

            # 2) 결과 파싱
            parsed = parse_batch_jsonl(jsonl_rows)

            # 3) S3 저장
            s3_key = save_to_s3(code, type_, parsed)

            # 4) DynamoDB 상태 update (completed)
            update_status(code_type, "completed")

            outputs.append({
                "batch_id": batch_id,
                "status": "completed",
                "code_type": code_type,
                "s3_key": s3_key
            })

        except Exception as e:
            print(f"[ERROR] Batch {code_type} failed: {e}")
            update_status(code_type, "error")
            outputs.append({
                "code_type": code_type,
                "status": "error",
                "error": str(e)
            })
    msg = "📌 OpenAI Batch S3 저장 완료!\n" + "\n".join(
        f"- {o['code_type']}: {o['status']}"
        for o in outputs
    )
    send_slack_message(msg)

    return {
        "message": "Batch processing finished",
        "details": outputs
    }
