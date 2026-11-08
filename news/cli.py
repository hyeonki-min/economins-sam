from __future__ import annotations

import argparse
import json
from pathlib import Path

from news_selector import select_articles
from rss_collector import collect_rss_news


def read_jsonl(path: Path) -> list[dict]:
    records = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def write_jsonl(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(prog="alpha_lab")
    subparsers = parser.add_subparsers(dest="command")

    news_parser = subparsers.add_parser("news")
    news_subparsers = news_parser.add_subparsers(dest="news_command")

    collect_parser = news_subparsers.add_parser("collect")
    collect_parser.add_argument("--source-config", default="data/config/rss_sources.json")
    collect_parser.add_argument("--rules", default="data/rules/news_selection_rules.json")
    collect_parser.add_argument("--output-dir", default=None)

    args = parser.parse_args()

    if args.command == "news" and args.news_command == "collect":
        raw_path = collect_rss_news(
            source_config_path=args.source_config,
            output_dir=args.output_dir,
        )

        records = read_jsonl(raw_path)
        selected = select_articles(records, rules_path=args.rules)

        # raw/news/YYYY-MM-DD/rss_news_*.jsonl -> processed/news_selection/YYYY-MM-DD/news_selection_*.jsonl
        parts = raw_path.parts
        date_str = raw_path.parent.name
        filename = raw_path.name.replace("rss_news_", "news_selection_")
        base_dir = Path(args.output_dir) if args.output_dir else Path("data")
        selection_path = base_dir / "processed" / "news_selection" / date_str / filename

        write_jsonl(selected, selection_path)
        print(f"[INFO] selection saved: {selection_path}")
        return

    parser.print_help()


if __name__ == "__main__":
    main()
