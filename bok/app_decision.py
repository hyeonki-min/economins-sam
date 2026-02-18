from common.slack import send_slack_message
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
BOK_PAGE_URL = os.environ["BOK_PAGE_URL"]


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
    if tokens < 2500:
        return "3 ~ 6"
    elif tokens < 5000:
        return "6 ~ 8"
    else:
        return "8 ~ 10"


def build_system_prompt(text: str):
    tokens = estimate_tokens(text)
    lines = decide_summary_lines(tokens)
    return f"""
당신은 경제 전문가이자 문서 편집자입니다.

주어진 텍스트를 읽고 아래 구조의 JSON만 출력하세요.
{{"title":"text","summary":["sentence"],"tooltip":{{"keyword":"description"}}}}

규칙:
- JSON 외 다른 텍스트 출력 금지
- 정보 추가·해석 금지
- 내부 추론 없이 바로 JSON 결과를 작성할 것
- 조건을 완벽히 만족하지 못해도 반드시 JSON을 출력할 것

title:
- 기준 금리 변동 여부에 대한 헤드라인형 제목
- 명사 중심, 간결한 한국어

summary:
- {lines}개의 완결된 문장
- 중립적·분석적 한국어
- 정책 판단, 국내·외 경제 여건, 위험 요인을 포괄

tooltip:
- 0~10개까지 유연하게 선택
- summary에 실제로 등장한 경제 개념만 선택
- 용어는 summary에 나온 표현 그대로 사용하되
  괄호(), 기호, 수식어는 제거한 핵심 용어만 사용
- 분석 방식이나 서술 구조를 나타내는 표현은 제외
- 실질적인 경제 개념·지표·정책·경제 구조에 해당하는 용어만 선택
- 설명은 경제적 의미를 담은 명사형 정의
- 군더더기 설명 금지

token handling:
- 출력 길이가 토큰 제한을 초과할 가능성이 있으면, 툴팁 항목 수를 먼저 줄인다
- 제목(title)과 요약(summary)은 항상 전체를 유지한다
"""

def create_batch_jsonl(paragraph, output_file="/tmp/batch_input.jsonl"):
    with open(output_file, "w", encoding="utf-8") as f:
        prompt = build_system_prompt(paragraph)
        item = {
            "custom_id": f"para-0000",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-5.2",
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": paragraph},
                ],
                "max_completion_tokens": 1000,
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
                                "items": { "type": "string" }
                            },
                            "tooltip": {
                                "type": "object",
                                "additionalProperties": { "type": "string" }
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


PUBLISH_DATES = [
    (1, 15),
    (2, 26),
    (4, 10),
    (5, 28),
    (7, 16),
    (8, 27),
    (10, 22),
    (11, 26),
]


def get_target_report_month(today=None):
    today = today or datetime.today()
    year = today.year

    release_days = [
        (m, datetime(year, m, d)) for m, d in PUBLISH_DATES
    ]
    release_days.sort(key=lambda x: x[1])

    target = None
    for m, available in release_days:
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

        link = tds[1].select_one(
            "div.fileGoupBox ul li:nth-of-type(2) a.i-download[href]"
        )
        if not link:
            continue

        pdf_links.append({
            "filename": link.get_text(strip=True),
            "url": urljoin(page_url, link["href"]),
        })
    return pdf_links


def should_download_today(today=None):
    today = today or datetime.today()
    month = get_target_report_month(today)
    if not month:
        return None

    pdfs = extract_pdf_links(BOK_PAGE_URL)
    year = today.year
    short_code = f"{year % 100:02d}{month:02d}"
    expected_code = f"{year}-{month:02d}"

    for info in pdfs:
        if short_code in info["filename"]:
            return {**info, "code": expected_code}

    return None


def download_pdf(pdf_url, filename):
    path = f"/tmp/{filename}"
    r = requests.get(pdf_url, timeout=10)
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)
    return path


def submit_batch(file_path):
    batch_input = client.files.create(
        file=open(file_path, "rb"),
        purpose="batch",
    )
    return client.batches.create(
        input_file_id=batch_input.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
    )


def exists_batch(code, type_):
    resp = table.query(
        KeyConditionExpression=Key("code_type").eq(f"{code}#{type_}")
    )
    return resp["Count"] > 0


def save_batch(batch_id, code, type_):
    table.put_item(
        Item={
            "code_type": f"{code}#{type_}",
            "code": code,
            "type": type_,
            "batch_id": batch_id,
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
        }
    )


def run():
    pdf_info = should_download_today()

    if not pdf_info:
        return {
            "status": "NO_DATA",
            "reason": "no pdf today",
        }

    if exists_batch(pdf_info["code"], "bok-decision"):
        return {
            "status": "NO_DATA",
            "reason": "already processed",
            "code": pdf_info["code"],
        }

    pdf_path = download_pdf(pdf_info["url"], pdf_info["filename"])

    raw_text = extract_text(pdf_path)
    paragraph = extract_paragraphs(raw_text)

    jsonl_path = create_batch_jsonl(paragraph)
    batch = submit_batch(jsonl_path)

    save_batch(batch.id, pdf_info["code"], "bok-decision")

    return {
        "status": "SUCCESS",
        "batch_id": batch.id,
        "code": pdf_info["code"],
        "paragraphs": len(paragraph),
    }


def lambda_handler(event, context):
    try:
        result = run()

        send_slack_message(
            service="BOK | Decision Batch",
            result=result
        )

        return {
            "statusCode": 200,
            "body": json.dumps(result, ensure_ascii=False),
        }

    except Exception as e:
        send_slack_message(
            service="BOK | Decision Batch",
            message=str(e),
            status="ERROR",
        )
        raise
