import os
import json
import requests
from datetime import datetime

_SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")


def send_slack_message(
    service: str,
    result: dict | None = None,
    message: str | None = None,
    status: str | None = None,
):
    """
    1️⃣ 정상 케이스 → result 넘기면 자동 포맷
    2️⃣ 에러 케이스 → message + status="ERROR"
    """

    if not _SLACK_WEBHOOK_URL:
        return

    # -----------------------------
    # status 결정
    # -----------------------------
    if status:
        final_status = status
    elif result:
        final_status = result.get("status", "NO_DATA")
    else:
        final_status = "NO_DATA"

    status_meta = {
        "SUCCESS": {"emoji": "✅", "color": "#2eb886"},
        "NO_CHANGE": {"emoji": "⏸", "color": "#9e9e9e"},
        "SKIPPED_SHRINK": {"emoji": "⚠️", "color": "#e01e5a"},
        "NO_DATA": {"emoji": "ℹ️", "color": "#439FE0"},
        "ERROR": {"emoji": "❌", "color": "#e01e5a"},
    }

    meta = status_meta.get(final_status, status_meta["NO_DATA"])

    # -----------------------------
    # 메시지 생성
    # -----------------------------
    if message:
        final_message = message

    elif result:
        parts = []

        # Molit 구조 대응
        if "deal_ymd" in result:
            parts.append(f"deal_ymd: {result['deal_ymd']}")

        if "total" in result:
            failed = result.get("failed", 0)
            parts.append(f"success: {result['total'] - failed} / {result['total']}")

        # 기존 count 기반 구조
        if "old_count" in result and "new_count" in result:
            parts.append(f"{result['old_count']} → {result['new_count']}")

        elif "count" in result:
            parts.append(f"rows={result['count']}")

        final_message = "\n".join(parts) if parts else str(result)

    else:
        final_message = "No message"

    payload = {
        "attachments": [
            {
                "color": meta["color"],
                "text": (
                    f"{meta['emoji']} *{service}*\n"
                    f"{final_message}\n"
                    f"_time: {datetime.utcnow().isoformat()}_"
                ),
            }
        ]
    }

    try:
        requests.post(
            _SLACK_WEBHOOK_URL,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=5,
        )
    except Exception as e:
        print(f"[Slack Error] {e}")
