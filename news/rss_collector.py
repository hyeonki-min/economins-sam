    }.get((tier or "D").upper(), 4)


def deduplicate(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    이번 실행 안에서 같은 URL/title로 수집된 기사는
    reliability tier가 더 높은 소스 기사를 유지한다.
    """
    selected: dict[str, dict[str, Any]] = {}

    for record in records:
        key = record["dedup_key"]

        if key not in selected:
            selected[key] = record
            continue

        current = selected[key]

        if (
            tier_rank(record.get("source_reliability_tier"))
            < tier_rank(current.get("source_reliability_tier"))
        ):
            selected[key] = record

    return list(selected.values())


def save_jsonl(
    records: list[dict[str, Any]],
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(
                json.dumps(record, ensure_ascii=False)
                + "\n"
            )


def collect_rss_news(
    source_config_path: str | Path = "data/config/rss_sources.json",
    output_dir: str | Path | None = None,
    lookback_minutes: int = DEFAULT_LOOKBACK_MINUTES,
    source_filter: Callable[[RssSource], bool] | None = None,
) -> Path:
    """
    RSS를 전체 조회하되 저장 대상은 아래 조건을 모두 만족하는 기사만이다.

    1. published_at이 존재
    2. 현재 KST 기준 최근 lookback_minutes 이내
    3. 같은 날짜 raw 폴더에 아직 저장되지 않은 dedup_key
    """
    base = Path(output_dir) if output_dir else output_base_dir()
    run_dt = now_kst()
    date_str = run_dt.strftime("%Y-%m-%d")
    timestamp = run_dt.strftime("%Y%m%d_%H%M%S")

    raw_date_dir = base / "raw" / "news" / date_str
    existing_dedup_keys = load_existing_dedup_keys(raw_date_dir)

    print(
        f"[INFO] rss collect start "
        f"now={run_dt.isoformat()} "
        f"lookback_minutes={lookback_minutes} "
        f"existing_today={len(existing_dedup_keys)}"
    )

    sources = load_sources(source_config_path)
    all_records: list[dict[str, Any]] = []

    total_feed_entries = 0
    total_recent_candidates = 0
    total_skipped_old = 0
    total_skipped_unknown_time = 0
    total_skipped_existing_today = 0

    for source in sources:
        # Optional pre-fetch economic-source gate. The default remains unfiltered.
        # Filtering here avoids network requests for clearly non-economic feeds.
        if source_filter is not None and not source_filter(source):
            print(
                f"[INFO] skip non-economic RSS source={source.source_id} "
                f"category={source.category}"
            )
            continue
        try:
            source_records, stats = collect_from_source(
                source=source,
                collected_dt=run_dt,
                lookback_minutes=lookback_minutes,
                existing_dedup_keys=existing_dedup_keys,
            )

            all_records.extend(source_records)

            total_feed_entries += stats.feed_entries
            total_recent_candidates += stats.saved_candidates
            total_skipped_old += stats.skipped_old
            total_skipped_unknown_time += (
                stats.skipped_unknown_published_at
            )
            total_skipped_existing_today += (
                stats.skipped_existing_today
            )
        except Exception as exc:
            print(
                f"[ERROR] collect failed "
                f"source={source.source_id}: {exc}"
            )

    deduped = deduplicate(all_records)

    # 이번 실행 중 여러 source에서 중복된 기사를 한 건만 남긴 뒤 저장.
    out_path = raw_date_dir / f"rss_news_{timestamp}.jsonl"
    save_jsonl(deduped, out_path)

    print(
        f"[INFO] rss collect done "
        f"feed_entries={total_feed_entries} "
        f"recent_candidates={total_recent_candidates} "
        f"deduped_in_run={len(deduped)} "
        f"skipped_old={total_skipped_old} "
        f"skipped_unknown_published_at={total_skipped_unknown_time} "
        f"skipped_existing_today={total_skipped_existing_today} "
        f"path={out_path}"
    )

    return out_path