from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import boto3

from rss_collector import collect_rss_news
from news_selector import load_rules, select_collected_news, should_collect_feed


s3 = boto3.client("s3")


def upload_directory_to_s3(local_dir: Path, bucket: str, prefix: str) -> int:
    uploaded = 0

    if not local_dir.exists():
        print(f"[WARN] local dir does not exist: {local_dir}")
        return uploaded

    for path in local_dir.rglob("*"):
        if not path.is_file():
            continue

        key = f"{prefix.rstrip('/')}/{path.relative_to(local_dir).as_posix()}"
        print(f"[INFO] upload {path} -> s3://{bucket}/{key}")
        s3.upload_file(str(path), bucket, key)
        uploaded += 1

    return uploaded


def lambda_handler(event, context):
    bucket = os.environ["S3_BUCKET_NAME"]
    prefix = os.environ.get("ALPHA_LAB_S3_PREFIX", "alpha-lab")
    output_dir = Path(os.environ.get("ALPHA_LAB_OUTPUT_DIR", "/tmp/alpha_lab_output"))
    source_config = os.environ.get("ALPHA_LAB_RSS_SOURCE_CONFIG", "data/config/rss_sources.json")
    rules_path = os.environ.get("ALPHA_LAB_NEWS_RULES_PATH", "data/rules/news_selection_rules.json")

    output_dir.mkdir(parents=True, exist_ok=True)

    rules = load_rules(rules_path)

    raw_path = collect_rss_news(
        source_config_path=source_config,
        output_dir=output_dir,
        # Gate before HTTP fetch, based on each configured RSS source category.
        # Unknown/tech feeds remain eligible for article-level economic checks.
        source_filter=lambda source: should_collect_feed(source.category, rules),
    )

    # raw/news/{date} stays where collect_rss_news wrote it.
    # Only add processed/news_selection/{date} under the same output_dir.
    selection = select_collected_news(
        raw_path=raw_path,
        output_dir=output_dir,
        rules_path=rules_path,
        asof_time=datetime.now(timezone.utc).isoformat(),
    )

    uploaded = upload_directory_to_s3(output_dir, bucket, prefix)

    return {
        "ok": True,
        "raw_path": str(raw_path),
        "uploaded_count": uploaded,
        "bucket": bucket,
        "prefix": prefix,
        "selection": selection,
    }
