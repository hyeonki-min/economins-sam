# common/slack.py
import os
import json
import requests
from datetime import datetime


_SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")


def send_slack_message(
    service: str,
    message: str,
    status: str = "NO_DATA",
):
    """
    status: SUCCESS | ERROR | NO_DATA
    """
    if not _SLACK_WEBHOOK_URL:
        return

    emoji = {
        "SUCCESS": "✅",
        "ERROR": "❌",
        "NO_DATA": "ℹ️",
    }.get(status, "ℹ️")

    payload = {
        "text": (
            f"{emoji} *{service}*\n"
            f"{message}\n"
            f"_time: {datetime.utcnow().isoformat()}_"
        )
    }

    try:
        requests.post(
            _SLACK_WEBHOOK_URL,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=5,
        )
    except Exception:
        # Slack 장애는 실패 전파 ❌
        pass
