import os
import re
import json
import boto3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime, timedelta

from pdfminer.high_level import extract_text
from openai import OpenAI
from pydantic import BaseModel
import tiktoken

# --------------------------
# 0. AWS 클라이언트 초기화
# --------------------------
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["DDB_TABLE"])

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

class SummaryOutput(BaseModel):
    title: str
    summary: str

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


def build_author_pattern():
    DEPT_KEYWORDS = [
        "조사국", "금융시장국", "국제국", "금융결제국",
        "경제통계1국", "경제통계2국", "금융안정국", "통화정책국",
        "경제연구원", "외자운용원", "국제협력국", "발권국",
        "금융업무국"
    ]
    dept_regex = "|".join([re.escape(d) for d in DEPT_KEYWORDS])
    # 괄호 내부에 부서명이 word boundary로 등장하는 경우만 허용
    return rf"(\([^)]*(?:{dept_regex})[^)]*\))"


def split_paragraphs_by_roman(text: str) -> list[str]:
    author_pattern = build_author_pattern()
    tokens = re.split(author_pattern, text)

    paragraphs = []
    i = 1
    while i < len(tokens):
        header = tokens[i].strip()
        content = tokens[i+1].strip() if i + 1 < len(tokens) else ""
        paragraphs.append(f"{header} {content}".strip())
        i += 2

    pre = tokens[0].strip()
    if pre and paragraphs:
        paragraphs[0] = (pre + " " + paragraphs[0]).strip()

    return [p for p in paragraphs if p.strip()]


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
    text = unify_roman_and_symbols(raw_text)
    text = normalize_text(text)
    text = remove_table_of_contents(text)
    text = cut_statistics_section(text)

    paras = split_paragraphs_by_roman(text)
    cleaned = [clean_non_text_blocks(p) for p in paras]
    return cleaned


# ------------------------
# 2. summary prompt & batch jsonl
# ------------------------
def estimate_tokens(text: str) -> int:
    enc = tiktoken.get_encoding("cl100k_base")
    return len(enc.encode(text))


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
다음 문단을 불필요한 문장이나 반복 표현은 제거하고 핵심 경제 흐름만 정리하고 핵심만 {lines}로 요약해줘. 
이를 대표하는 제목을 1줄로 작성해줘.
"""

def create_batch_jsonl(paragraphs, output_file="/tmp/batch_input.jsonl"):
    with open(output_file, "w", encoding="utf-8") as f:
        for i, para in enumerate(paragraphs, start=1):
            prompt = build_system_prompt(para)

            item = {
                "custom_id": f"para-{i:04d}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-5.1",
                    "messages": [
                        {"role": "system", "content": prompt},
                        {"role": "user", "content": para},
                    ],
                    "max_completion_tokens": 500,
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
                                "type": "string"
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
        available = release_day + timedelta(days=7)
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
        last_td = tds[-1]
        link = last_td.select_one("div.fileGoupBox li.ajasOpen5Btn a.i-download[href]")
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

    for info in pdfs:
        if f"({year}.{month}월" in info["filename"]:
            return info

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


def save_batch_id(batch_id):
    table.put_item(Item={"batch_id": batch_id, "status": "pending"})


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


# ------------------------
# 5. Lambda Handler
# ------------------------
def lambda_handler(event, context):
    BOK_PAGE_URL = os.environ["BOK_PAGE_URL"]

    pdf_info = should_download_today(BOK_PAGE_URL, today=datetime.today())
    if not pdf_info:
        return {"message": "No PDF available today."}

    pdf_path = download_pdf(pdf_info["url"], pdf_info["filename"])

    raw = extract_text(pdf_path)
    paragraphs = extract_paragraphs(raw)
    jsonl_path = create_batch_jsonl(paragraphs)

    batch = submit_batch(jsonl_path)
    save_batch_id(batch.id)

    msg = f"📌 *OpenAI Batch Issue 요청 완료!*\n• Batch ID: `{batch.id}`"
    send_slack_message(msg)

    return {
        "status": "submitted",
        "batch_id": batch.id,
        "paragraphs": len(paragraphs),
    }
