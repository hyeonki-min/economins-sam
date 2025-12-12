import os
import re
import json
import boto3
from boto3.dynamodb.conditions import Key
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime, timedelta

from pdfminer.high_level import extract_text
from openai import OpenAI

# --------------------------
# 0. AWS 클라이언트 초기화
# --------------------------
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["DDB_TABLE"])

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])


# ------------------------
# 1. 정규화 함수
# ------------------------
def normalize_text(text: str) -> str:
    text = text.replace("ž", "·")
    lines = text.splitlines()
    cleaned_lines = []

    line_remove_patterns = [
        r"^\s*\[?\s*(그림|표)\s*\d+",
        r"^\s*주\s*[: ]?\s*\d+",
        r"^\s*(자료|출처)\s*[: ]?",
        r"^\s*-\s*\d+\s*-",
    ]

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if any(re.search(p, stripped) for p in line_remove_patterns):
            continue
        cleaned_lines.append(stripped)

    text = " ".join(cleaned_lines)
    text = re.sub(r"\d+\)", " ", text)
    text = re.sub(r"[^\w\s.,!?가-힣()/%\-ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]", " ", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def unify_roman_and_symbols(text: str) -> str:
    variants_I = [
        "Ⅰ", "Ｉ", "𝑰", "𝐈", "𝘐", "𝕀", "𝖨", "𝗜", "𝛪",
    ]
    for v in variants_I:
        text = text.replace(v, "I")

    for v in ["–", "—", "−", "﹣", "‐"]:
        text = text.replace(v, "-")

    for v in ["。", "．", "｡"]:
        text = text.replace(v, ".")

    return text


def remove_table_of_contents(text: str) -> str:
    m = re.search(r"[ⅠI]\s*-\s*1", text)
    if m:
        return text[m.start():]
    return text


def cut_statistics_section(text: str) -> str:
    m = re.search(r"주요\s*통계\s*및\s*참고", text)
    if m:
        return text[:m.start()].strip()
    return text


def clean_non_text_blocks(paragraph: str) -> str:
    sents = re.split(r"(?<=[.!?])\s+", paragraph)
    cleaned = []

    for s in sents:
        s = s.strip()
        if not s:
            continue

        if re.search(r"^(그림|표)\s*\d+", s):
            continue
        if re.search(r"^주\s*\d+", s):
            continue
        if re.search(r"^(자료|출처)[: ]", s):
            continue

        tokens = s.split()
        num_tokens = sum(1 for t in tokens if re.match(r"^[\d\.\-,/]+$", t))
        if len(tokens) > 0 and num_tokens / len(tokens) > 0.6:
            continue

        cleaned.append(s)

    return " ".join(cleaned).strip()


def extract_paragraphs(raw_text: str) -> list[str]:
    # (1) normalize
    text = unify_roman_and_symbols(raw_text)
    text = normalize_text(text)
    # (2) 목차 제거
    text = remove_table_of_contents(text)
    # (3) 말미 통계 제거
    text = cut_statistics_section(text)
    # (4) 줄 기반 문단 후보
    cleaned = clean_non_text_blocks(text)
    return cleaned

# ------------------------
# 2. summary prompt & batch jsonl
# ------------------------
def estimate_tokens(text: str) -> int:
    return int(len(text) / 3)


def decide_summary_lines(tokens: int) -> str:
    if tokens < 1000:
        return "3~4줄"
    elif tokens < 2000:
        return "5~6줄"
    elif tokens < 3000:
        return "6~8줄"
    return "8~10줄"


def build_system_prompt(text: str):
    tokens = estimate_tokens(text)
    lines = decide_summary_lines(tokens)
    return f"""
너는 경제 분석 전문가야. 
다음 문단을 불필요한 문장이나 반복 표현은 제거하고 핵심 경제 흐름만 정리하여 핵심 내용을 {lines}로 요약해줘. 
요약은 '핵심 문장 단위 문자열 리스트' 형태로 반환해야 해.
또한 기준 금리 변동 여부를 제목으로 1줄로 작성해줘.
"""

def create_batch_jsonl(paragraph, output_file="/tmp/batch_input.jsonl"):
    with open(output_file, "w", encoding="utf-8") as f:
        prompt = build_system_prompt(paragraph)
        item = {
            "custom_id": f"para-0000",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-5.1",
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": paragraph},
                ],
                "max_completion_tokens": 800,
                "temperature": 0,
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                      "name": "paragraph_summary",
                      "schema": {
                        "type": "object",
                        "properties": {
                            "title": {
                                "type": "string"
                            },
                            "summary": {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                }
                            }
                        },
                        "required": ["title", "summary"],
                      }
                    }
                  },
                },
            }
        f.write(json.dumps(item, ensure_ascii=False) + "\n")

    return output_file

# ------------------------
# 3. PDF 다운로드 & Batch 제출
# ------------------------
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


def extract_pdf_links(page_url):
    soup = BeautifulSoup(requests.get(page_url).text, "html.parser")
    rows = soup.select("table#tableId tbody tr")

    pdf_links = []
    for row in rows:
        tds = row.find_all("td")
        if not tds:
            continue
        last_td = tds[1]
        link = last_td.select_one("div.fileGoupBox ul li:nth-of-type(2) a.i-download[href]")
        if not link:
            continue

        filename = link.get_text(strip=True)
        pdf_url = urljoin(page_url, link["href"])
        pdf_links.append({"filename": filename, "url": pdf_url})

    return pdf_links


def should_download_today(page_url, today=None):
    month = get_target_report_month(today)
    if month is None:
        return None

    pdfs = extract_pdf_links(page_url)
    year = today.year

    short_code = f"{year % 100:02d}{month:02d}"
    expected_code = f"{year}-{month:02d}"

    for info in pdfs:
        if short_code in info["filename"]:
            return {
                **info,
                "code": expected_code
            }

    return None

def download_pdf(pdf_url, filename):
    path = f"/tmp/{filename}"
    r = requests.get(pdf_url)
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)
    return path


def submit_batch(file_path):
    batch_input = client.files.create(
        file=open(file_path, "rb"),
        purpose="batch",
    )

    batch_job = client.batches.create(
        input_file_id=batch_input.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
    )
    return batch_job


def save_batch_id(batch_id: str, code: str, type_: str):
    item = {
        "code_type": f"{code}#{type_}",
        "code": code,
        "type": type_,
        "batch_id": batch_id,
        "status": "pending",
        "created_at": datetime.now().isoformat(),
    }

    table.put_item(Item=item)
    return item

# ------------------------
# 4. Slack 메시지
# ------------------------
def send_slack_message(text: str):
    webhook = os.environ["SLACK_WEBHOOK_URL"]
    payload = {"text": text}

    resp = requests.post(
        webhook,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
    )
    print("[Slack] status:", resp.status_code)
    print("[Slack] response:", resp.text)

def exists_batch(code, type_):
    code_type = f"{code}#{type_}"

    resp = table.query(
        KeyConditionExpression=Key("code_type").eq(code_type)
    )

    return resp["Count"] > 0

# ------------------------
# 5. Lambda Handler
# ------------------------
def lambda_handler(event, context):
    BOK_PAGE_URL = os.environ["BOK_PAGE_URL"]

    pdf_info = should_download_today(BOK_PAGE_URL, today=datetime.today())
    if not pdf_info:
        return {"message": "No PDF available today."}
    if exists_batch(pdf_info["code"], "bok-decision"):
        return {"message": "Already process."}
    pdf_path = download_pdf(pdf_info["url"], pdf_info["filename"])

    raw = extract_text(pdf_path)
    paragraphs = extract_paragraphs(raw)
    jsonl_path = create_batch_jsonl(paragraphs)

    batch = submit_batch(jsonl_path)
    save_batch_id(batch.id, pdf_info["code"], "bok-decision")

    msg = f"📌 *OpenAI Batch Decision 요청 완료!*\n• Batch ID: `{batch.id}`"
    send_slack_message(msg)

    return {
        "status": "submitted",
        "batch_id": batch.id,
        "paragraphs": len(paragraphs),
    }
