"""Economic RSS news selector, compatible with news_selection_rules_final_2026-11-08.json.

Article fields: title, summary, source_category, optional first_seen_at/published_at.
Returns original article plus the legacy-compatible ``selection`` object.

Feed gating happens upstream through ``should_collect_feed``.  The selector retains
raw articles and annotates non-economic records with a noise decision.
"""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_RULES_PATH = "data/rules/news_selection_rules.json"

# This is only a heuristic: no ontology or company-symbol resolution occurs here.
_MONEY_RE = re.compile(
    r"(?:\d[\d,]*(?:\.\d+)?\s*(?:백|천)?\s*(?:억|조)\s*(?:원|달러)|"
    r"\d[\d,]*(?:\.\d+)?\s*%\s*(?:증가|감소|상승|하락|급등|급락))",
    re.IGNORECASE,
)
_CONCRETE_RE = re.compile(
    r"체결|확정|시행|승인|완료|돌입|중단|해지|취소|상향|하향|인상|인하|동결|"
    r"급등|급락|수주|매각|소각|적자전환|흑자전환|파산|반려|발행|재개|철회|"
    r"(?!예정)착공|준공",
    re.IGNORECASE,
)
_TENTATIVE_RE = re.compile(
    r"검토|논의|협의|가능성|관측|거론|예정|추진|루머|소문|(?<!\w)MOU(?!\w)|"
    r"업무협약|협력 논의|전망(?:치|에 따르면|이다|으로)?",
    re.IGNORECASE,
)
# Qualifying action must be explicit in the title to defeat tentative-only wording.

def load_rules(path: str | Path = DEFAULT_RULES_PATH) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as f:
        rules = json.load(f)
    if not isinstance(rules, dict):
        raise ValueError("Rules JSON must be an object")
    return rules


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(text or ""))).strip()


def contains_any(text: str, keywords: list[str]) -> list[str]:
    """Case-insensitive matching. Bound short Latin keywords (AI/EU/IPO etc)."""
    text = _normalize(text)
    result: list[str] = []
    seen: set[str] = set()
    for word in keywords:
        word = _normalize(word)
        key = word.casefold()
        if not word or key in seen:
            continue
        seen.add(key)
        # Avoid matching AI inside RAISE and IT inside capitalized English words.
        has_short_latin = bool(re.fullmatch(r"[A-Za-z0-9+#&.\-]{1,4}", word))
        if has_short_latin:
            match = re.search(r"(?<![A-Za-z0-9])" + re.escape(word) + r"(?![A-Za-z0-9])", text, re.I)
        else:
            match = re.search(re.escape(word), text, re.I)
        if match:
            result.append(word)
    return result


def match_patterns(text: str, patterns: list[str]) -> list[str]:
    result = []
    text = _normalize(text)
    for pattern in patterns:
        try:
            if re.search(pattern, text, re.I):
                result.append(pattern)
        except re.error:
            # Fail loudly: a malformed rule must not silently change exclusions.
            raise ValueError(f"Invalid regex in rule: {pattern!r}") from None
    return result


def clamp_score(value: float) -> float:
    return max(0.0, min(1.0, round(value, 4)))


def classify_score(score: float, rules: dict[str, Any]) -> str:
    levels = rules["classification_levels"]
    if score >= levels["high"]["min_score"]:
        return "high"
    if score >= levels["review"]["min_score"]:
        return "review"
    return "noise"


def should_collect_feed(source_category: str, rules: dict[str, Any]) -> bool:
    """Feed prefilter: unknown/tech must pass article-level evidence later.

    RSS fetcher should call this before requesting each *categorized* feed.
    Unknown sources are tentatively fetched, never blindly accepted.
    """
    category = (source_category or "unknown").lower()
    scope = rules.get("collection_scope", {})
    if not scope or scope.get("mode") != "ECONOMIC_RSS_FEEDS_ONLY":
        return True
    if category in scope.get("excluded_source_categories", []):
        return False
    return (
        category in scope.get("allowed_source_categories", [])
        or category in scope.get("conditional_source_categories", [])
        or category == "unknown" and scope.get("unknown_source_policy") == "REQUIRE_ECONOMIC_SCOPE_EVIDENCE"
    )


def is_hard_excluded(title: str, rules: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons = [
        *[f"hard_exclusion_keyword:{x}" for x in contains_any(title, rules.get("hard_exclusion_keywords", []))],
        *[f"hard_exclusion_pattern:{x}" for x in match_patterns(title, rules.get("hard_exclusion_title_patterns", []))],
    ]
    return bool(reasons), reasons


def _time_parse(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        return dt if dt.tzinfo else None
    except ValueError:
        return None


def _timing_flags(article: dict[str, Any], asof_time: str | None) -> tuple[bool, list[str]]:
    """Never pretend publish time is first-seen time; reject known future records."""
    if asof_time is None:
        return True, []
    asof = _time_parse(asof_time)
    if asof is None:
        raise ValueError("asof_time must be ISO-8601 with timezone")
    flags = []
    # rss_collector stores the first observation by this collector as collected_at.
    seen = _time_parse(article.get("first_seen_at") or article.get("collected_at"))
    published = _time_parse(article.get("published_at"))
    if not seen:
        flags.append("first_seen_at_unverified")
    if not published:
        flags.append("published_at_unverified")
    if seen and seen > asof or published and published > asof:
        flags.append("article_after_asof")
        return False, flags
    return True, flags


def _noise_result(
    article: dict[str, Any], rules: dict[str, Any], reason: str,
    flags: list[str], matched_negative: list[str] | None = None,
) -> dict[str, Any]:
    return {
        **article,
        "selection": {
            "ruleset_version": rules.get("version"),
            "investment_relevance_score": 0.0,
            "report_candidate_level": "noise",
            "matched_positive_rules": [],
            "matched_negative_rules": matched_negative or [],
            "matched_weak_rules": [],
            "conflict_flags": flags,
            "investment_relevance_reason": reason,
            "exclusion_reason": reason,
            "economic_scope_passed": False,
            "material_event_detected": False,
        },
    }


def select_article(
    article: dict[str, Any],
    rules: dict[str, Any],
    *,
    asof_time: str | None = None,
) -> dict[str, Any]:
    """Score investment-event relevance without sentiment/ontology/price hindsight.

    The original article is left intact; decisions live in ``selection``.
    """
    title = _normalize(article.get("title", ""))
    summary = _normalize(article.get("summary", ""))
    text = f"{title} {summary}".strip()
    category = _normalize(article.get("source_category") or "unknown").lower()
    weights = rules.get("scoring_weights", {})
    event_policy = rules.get("event_quality_gate", {})
    economic_policy = rules.get("economic_scope_gate", {})

    if not should_collect_feed(category, rules):
        return _noise_result(article, rules, "경제/증권 RSS 출처 범위에서 제외", ["excluded_source_category"])
    if not text:
        return _noise_result(article, rules, "제목 및 요약 부재", ["empty_article"])
    timing_ok, timing_flags = _timing_flags(article, asof_time)
    if not timing_ok:
        return _noise_result(article, rules, "판단 시각 이후 수집 또는 발행된 뉴스", timing_flags)

    keys = (
        "positive_industry_keywords", "positive_action_keywords", "weak_action_keywords",
        "major_company_keywords", "global_partner_keywords", "policy_support_keywords",
        "market_policy_keywords", "market_policy_action_keywords", "corporate_capital_market_keywords",
        "material_negative_event_keywords", "macro_material_event_keywords", "policy_decision_event_keywords",
        "hard_noise_keywords", "content_noise_keywords", "startup_noise_keywords",
        "startup_exception_keywords", "game_content_noise_keywords", "game_investment_exception_keywords",
        "consumer_finance_noise_keywords", "consumer_finance_exception_keywords",
        "weak_editorial_keywords", "weak_editorial_exception_keywords",
    )
    m = {key: contains_any(text, rules.get(key, [])) for key in keys}
    title_patterns = match_patterns(title, rules.get("noise_title_patterns", []))
    hard_excluded, hard_reasons = is_hard_excluded(title, rules)

    industry = bool(m["positive_industry_keywords"])
    company = bool(m["major_company_keywords"])
    action = bool(m["positive_action_keywords"])
    weak = bool(m["weak_action_keywords"])
    policy = bool(m["market_policy_keywords"])
    policy_action = bool(m["market_policy_action_keywords"])
    capital = bool(m["corporate_capital_market_keywords"])
    negative = bool(m["material_negative_event_keywords"])
    macro_event = bool(m["macro_material_event_keywords"])
    policy_decision = bool(m["policy_decision_event_keywords"])
    support = bool(m["policy_support_keywords"])
    strong = any(bool(m.get(group)) for group in event_policy.get(
        "strong_event_group_any", [
            "positive_action_keywords", "material_negative_event_keywords",
            "corporate_capital_market_keywords", "macro_material_event_keywords",
            "policy_decision_event_keywords",
        ]
    ))
    headline_material = bool(
        any(contains_any(title, m[key]) for key in (
            "positive_action_keywords", "material_negative_event_keywords",
            "corporate_capital_market_keywords", "macro_material_event_keywords",
            "policy_decision_event_keywords",
        ))
    )
    concrete = bool(_CONCRETE_RE.search(title))
    # Any pending/discussion wording limits HIGH, even if the headline also
    # contains "승인" or "계약 체결" (e.g., "승인 검토", "체결 예정").
    tentative = bool(_TENTATIVE_RE.search(title))

    # An economic source_category alone cannot pass. Material events in an economic
    # feed count even without a known company in the fixed company dictionary.
    company_material = company and (strong or weak or support)
    industry_material = industry and (strong or weak or support)
    capital_event = capital
    market_event = bool(policy and (policy_action or policy_decision or macro_event))
    macro_material = macro_event
    economic_source = category in rules.get("collection_scope", {}).get("allowed_source_categories", [])
    unidentified_company_event = bool(
        economic_source and (action or negative) and headline_material
        and (not policy_decision or (industry or company or policy or capital or macro_event
              or re.search(r"관세|수출|기업|산업|증시|주식|반도체|금융", title)))
    )
    # For unknown or tech sources, require explicit economic indicators, not merely 'AI 출시'.
    unknown_source_event = bool((industry or company or policy) and strong)
    policy_decision_economic = bool(
        policy_decision and (
            industry or company or policy or capital or macro_event
            or re.search(r"관세|수출|기업|산업|증시|주식|반도체|금융", title)
        )
    )
    economic_scope = any((
        company_material,
        industry_material,
        capital_event,
        market_event,
        macro_material,
        policy_decision_economic,
        unidentified_company_event,
        unknown_source_event,
    ))
    if economic_policy.get("enabled", True) and not economic_scope:
        return _noise_result(article, rules, "경제적 사건 근거 부족", [*timing_flags, "economic_scope_failed"])

    # Hard exclusions apply only to title, except for explicit material economic
    # events mentioned IN THE TITLE (not merely a coincidental keyword in summary).
    if hard_excluded and not (
        event_policy.get("hard_exclusion_exception") == "KEEP_IF_DOCUMENTED_MATERIAL_CORPORATE_OR_MARKET_EVENT"
        and headline_material and (industry or company or policy or capital)
        and not tentative
    ):
        return _noise_result(article, rules, "제목 기준 비투자성 콘텐츠", [*timing_flags, "hard_exclusion"], hard_reasons)

    matched_pos: list[str] = []
    matched_neg: list[str] = []
    matched_weak: list[str] = []
    flags: list[str] = list(timing_flags)
    score = rules.get("category_weights", {}).get(category, 0.0)

    def award(group: str, key: str, prefix: str, *, when: bool = True) -> None:
        nonlocal score
        if when and m[group]:
            score += weights.get(key, 0.0)
            matched_pos.extend(f"{prefix}:{x}" for x in m[group])

    award("positive_industry_keywords", "positive_industry_keyword_match", "positive_industry")
    award("positive_action_keywords", "positive_action_keyword_match", "positive_action")
    award("major_company_keywords", "major_company_keyword_match", "major_company")
    award("global_partner_keywords", "global_partner_keyword_match", "global_partner")
    award("policy_support_keywords", "policy_support_keyword_match", "policy_support")
    award("market_policy_keywords", "market_policy_keyword_match", "market_policy")
    award("market_policy_action_keywords", "market_policy_action_keyword_match", "market_policy_action", when=policy)
    award("corporate_capital_market_keywords", "corporate_capital_market_keyword_match", "capital_market")
    # New groups in the final JSON previously had no matching weights; their
    # presence now contributes to importance with configurable optional weights.
    award("material_negative_event_keywords", "material_negative_event_keyword_match", "negative_event")
    award("macro_material_event_keywords", "macro_material_event_keyword_match", "macro_event")
    award("policy_decision_event_keywords", "policy_decision_event_keyword_match", "policy_decision")

    for group, key, label in (
        ("hard_noise_keywords", "hard_noise_keyword_match", "hard_noise"),
        ("content_noise_keywords", "content_noise_keyword_match", "content_noise"),
    ):
        if m[group]:
            score += weights.get(key, 0.0)
            matched_neg.extend(f"{label}:{x}" for x in m[group])
    if title_patterns:
        score += weights.get("noise_title_pattern_match", 0.0)
        matched_neg.extend(f"title_pattern:{x}" for x in title_patterns)
    if weak:
        matched_weak.extend(f"weak_action:{x}" for x in m["weak_action_keywords"])
        if not strong:
            score += weights.get("weak_action_only", -0.18)
    if m["weak_editorial_keywords"] and not m["weak_editorial_exception_keywords"]:
        score += weights.get("weak_editorial_penalty", -0.12)
        matched_weak.extend(f"weak_editorial:{x}" for x in m["weak_editorial_keywords"])
    if not (industry or company or policy or capital or macro_event or policy_decision or negative):
        score += weights.get("no_industry_or_company_connection", -0.10)
        matched_neg.append("no_named_investment_connection")

    # Explicit material events are stronger than merely mentioning an industry.
    # These defaults are adjustable through scoring_weights if desired.
    if strong and headline_material and concrete and not tentative:
        score += weights.get("confirmed_material_event_bonus", 0.12)
        flags.append("confirmed_material_event")
    if strong and headline_material and _MONEY_RE.search(title) and not tentative:
        score += weights.get("quantified_material_event_bonus", 0.23)
        flags.append("quantified_material_event")
    if negative and "material_negative_event_keyword_match" not in weights:
        score += 0.22
    if macro_event and not policy_action:
        score += weights.get("macro_material_event_keyword_match", 0.22)
    if policy_decision and not policy_action:
        score += weights.get("policy_decision_event_keyword_match", 0.22)

    forced_noise_reason: str | None = None
    for group, exception_group, flag, reason in (
        ("startup_noise_keywords", "startup_exception_keywords", "startup_noise_without_exception", "일반 창업·지원·행사 콘텐츠"),
        ("game_content_noise_keywords", "game_investment_exception_keywords", "game_noise_without_exception", "게임 경기·e스포츠 콘텐츠"),
        ("consumer_finance_noise_keywords", "consumer_finance_exception_keywords", "consumer_finance_without_exception", "개인 금융상품·소비자 안내 콘텐츠"),
    ):
        if m[group] and not m[exception_group] and not (headline_material and (capital or negative or macro_event or policy_decision)):
            forced_noise_reason = reason
            flags.append(flag)
            matched_neg.extend(f"{group}:{x}" for x in m[group])

    if (m["hard_noise_keywords"] or m["content_noise_keywords"] or title_patterns) and economic_scope:
        flags.append("noise_keyword_with_economic_event")

    score = clamp_score(score)
    level = classify_score(score, rules)

    # Importance guard: do not promote ordinary industry/company mentions or
    # tentative agreements to HIGH regardless of how many keyword groups match.
    if event_policy.get("enabled", True):
        if event_policy.get("high_requires_material_event", True) and not strong and level == "high":
            level = "review"
            flags.append("high_requires_material_event")
        if tentative and level == "high":
            level = "review"
            flags.append("rumor_or_discussion_high_limited")
        if not headline_material and level == "high":
            level = "review"
            flags.append("title_without_material_event_high_limited")
        if m["weak_editorial_keywords"] and not m["weak_editorial_exception_keywords"] and level == "high":
            level = "review"
            flags.append("editorial_high_limited")

    # Speculative event with both a recognized company AND an industry can be
    # reviewed even if weak-action penalty pushed the numerical score below 0.32.
    if (level == "noise" and company and industry and weak and not forced_noise_reason):
        level = "review"
        flags.append("company_industry_weak_event_review_floor")

    if forced_noise_reason:
        level = "noise"
    exclusion_reason = (forced_noise_reason or "투자 사건 관련성 점수가 낮음") if level == "noise" else None
    reason_parts = []
    if matched_pos:
        reason_parts.append("positive=" + ", ".join(matched_pos[:10]))
    if matched_neg:
        reason_parts.append("negative=" + ", ".join(matched_neg[:10]))
    if matched_weak:
        reason_parts.append("weak=" + ", ".join(matched_weak[:6]))

    return {
        **article,
        "selection": {
            "ruleset_version": rules.get("version"),
            "investment_relevance_score": score,
            "report_candidate_level": level,
            "matched_positive_rules": matched_pos,
            "matched_negative_rules": matched_neg,
            "matched_weak_rules": matched_weak,
            "conflict_flags": flags,
            "investment_relevance_reason": " | ".join(reason_parts) if reason_parts else "matched rules 없음",
            "exclusion_reason": exclusion_reason,
            "economic_scope_passed": True,
            "material_event_detected": strong,
        },
    }


def select_articles(
    records: list[dict[str, Any]],
    rules_path: str | Path = DEFAULT_RULES_PATH,
    *,
    asof_time: str | None = None,
) -> list[dict[str, Any]]:
    rules = load_rules(rules_path)
    return [select_article(record, rules, asof_time=asof_time) for record in records]



# ---------------------------------------------------------------------------
# Integration API for the existing AWS Lambda handler.
# Keep collection/S3 ownership outside this module.
# ---------------------------------------------------------------------------

def _iter_collected_articles(raw_path: Path):
    """Stream JSONL/NDJSON or load JSON list/wrapper; supports .gz variants.

    A different collector output schema must be adapted explicitly rather than
    silently skipping articles. Raw files are never modified.
    """
    import gzip

    name = raw_path.name.lower()
    compressed = name.endswith(".gz")
    suffix_name = name[:-3] if compressed else name
    opener = gzip.open if compressed else open

    if suffix_name.endswith((".jsonl", ".ndjson")):
        with opener(raw_path, "rt", encoding="utf-8") as handle:
            for lineno, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                article = json.loads(line)
                if not isinstance(article, dict):
                    raise ValueError(f"{raw_path}:{lineno}: article must be an object")
                yield article
        return

    if suffix_name.endswith(".json"):
        with opener(raw_path, "rt", encoding="utf-8") as handle:
            data = json.load(handle)
        # Common RSS raw shapes, e.g. {"data": {"items": [...]}}.
        while isinstance(data, dict):
            for key in ("articles", "items", "news", "records", "data"):
                if key in data:
                    data = data[key]
                    break
            else:
                raise ValueError(f"Unsupported RSS JSON wrapper in {raw_path}")
        if not isinstance(data, list):
            raise ValueError(f"RSS JSON must contain a list: {raw_path}")
        for index, article in enumerate(data):
            if not isinstance(article, dict):
                raise ValueError(f"Article at index {index} must be an object")
            yield article
        return

    raise ValueError(f"Unsupported RSS raw format: {raw_path.name}")


def _normalize_collected_article(article: dict[str, Any]) -> dict[str, Any]:
    """Adapt common collector field aliases without overwriting original keys."""
    aliases = {
        "증권": "stock", "주식": "stock", "시장": "stock",
        "산업": "industry", "기업": "industry",
        "경제": "economy", "거시경제": "economy",
        "금융": "finance", "기술": "tech", "IT": "tech", "테크": "tech",
        "생활": "lifestyle", "스포츠": "sports", "연예": "entertainment",
    }
    normalized = dict(article)
    if not normalized.get("title"):
        normalized["title"] = normalized.get("headline", "")
    if not normalized.get("summary"):
        normalized["summary"] = next(
            (normalized[key] for key in ("description", "excerpt", "content_summary")
             if isinstance(normalized.get(key), str)), ""
        )
    category = (
        normalized.get("source_category")
        or normalized.get("feed_category")
        or normalized.get("category")
    )
    if isinstance(category, dict):
        category = category.get("name") or category.get("slug")
    if not isinstance(category, str) and isinstance(normalized.get("source"), dict):
        category = normalized["source"].get("category")
    category = category.strip() if isinstance(category, str) else "unknown"
    normalized["source_category"] = aliases.get(category, category.lower())
    return normalized


def _resolve_raw_news_partition(raw_path: Path, output_dir: Path) -> tuple[str, list[Path]]:
    """Use the collector's existing raw/news/{date} partition verbatim.

    Supports either raw/news/YYYY-MM-DD/<files> or raw/news/YYYY-MM-DD.jsonl.
    Never infer the partition from Lambda invocation time.
    """
    from datetime import date

    if not raw_path.exists():
        raise FileNotFoundError(f"Collected RSS raw path does not exist: {raw_path}")
    if not raw_path.is_relative_to(output_dir):
        raise ValueError(f"Raw path must be within output_dir: {raw_path}")

    rel = raw_path.relative_to(output_dir)
    if len(rel.parts) < 3 or rel.parts[:2] != ("raw", "news"):
        raise ValueError(
            "Expected existing collector layout raw/news/{date}, "
            f"got {rel.as_posix()}"
        )

    partition = rel.parts[2]
    if len(rel.parts) == 3 and raw_path.is_file():
        # Date can also be the file name, e.g. raw/news/2026-11-08.jsonl.
        for extension in (".jsonl.gz", ".ndjson.gz", ".json.gz", ".jsonl", ".ndjson", ".json"):
            if partition.lower().endswith(extension):
                partition = partition[:-len(extension)]
                break

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", partition):
        date.fromisoformat(partition)  # validate while retaining original partition string
    elif re.fullmatch(r"\d{8}", partition):
        date.fromisoformat(f"{partition[:4]}-{partition[4:6]}-{partition[6:]}")
    else:
        raise ValueError(
            f"Unknown raw/news date partition {partition!r}; "
            "do not invent a different date or S3 key."
        )

    if raw_path.is_file():
        files = [raw_path]
    else:
        # The collector may return the date directory rather than one file.
        files = sorted(p for p in raw_path.rglob("*") if p.is_file() and (
            p.name.lower().endswith((".json", ".jsonl", ".ndjson", ".json.gz", ".jsonl.gz", ".ndjson.gz"))
        ))
    if not files:
        raise ValueError(f"No supported raw JSON/JSONL news files under {raw_path}")
    return partition, files


def select_collected_news(
    raw_path: str | Path,
    output_dir: str | Path,
    rules_path: str | Path = DEFAULT_RULES_PATH,
    *,
    asof_time: str | None = None,
) -> dict[str, Any]:
    """Classify existing raw/news/{date} into processed/news_selection/{date}.

    Never rename, relocate, or rewrite collected Raw. No run_id directory.
    For each raw rss_news_{timestamp}.jsonl, create timestamp-matched selected,
    audit and summary files. The raw files and S3 prefix remain unchanged.
    """
    output_dir = Path(output_dir).resolve()
    raw_path = Path(raw_path)
    if not raw_path.is_absolute():
        raw_path = raw_path if raw_path.exists() else output_dir / raw_path
    raw_path = raw_path.resolve()

    date_partition, raw_files = _resolve_raw_news_partition(raw_path, output_dir)
    rules = load_rules(rules_path)
    # The actual collector always returns one file per run:
    # raw/news/YYYY-MM-DD/rss_news_YYYYMMDD_HHMMSS.jsonl
    # Match this basename in processed so hourly runs cannot overwrite one another.
    if not raw_path.is_file() or not re.fullmatch(r"rss_news_\d{8}_\d{6}\.jsonl", raw_path.name):
        raise ValueError(
            "Expected actual collector output raw/news/YYYY-MM-DD/"
            "rss_news_YYYYMMDD_HHMMSS.jsonl"
        )
    if len(raw_files) != 1:
        raise ValueError("Expected exactly one RSS raw file per collector run")
    selection_dir = output_dir / "processed" / "news_selection" / date_partition
    selection_dir.mkdir(parents=True, exist_ok=True)
    stem = raw_path.stem
    selected_path = selection_dir / f"{stem}.selected.jsonl"
    audit_path = selection_dir / f"{stem}.audit.jsonl"
    summary_path = selection_dir / f"{stem}.summary.json"
    counts = {"high": 0, "review": 0, "noise": 0}

    with selected_path.open("w", encoding="utf-8") as selected, \
         audit_path.open("w", encoding="utf-8") as audit:
        index = 0
        for source_path in raw_files:
            for article in _iter_collected_articles(source_path):
                index += 1
                result = select_article(
                    _normalize_collected_article(article), rules, asof_time=asof_time
                )
                decision = result["selection"]
                level = decision["report_candidate_level"]
                if level not in counts:
                    raise ValueError(f"Unexpected selection level: {level!r}")
                counts[level] += 1
                if level in ("high", "review"):
                    selected.write(json.dumps(result, ensure_ascii=False) + "\n")
                audit.write(json.dumps({
                    "index": index,
                    "raw_source": source_path.relative_to(output_dir).as_posix(),
                    "article_id": article.get("article_id"),
                    "dedup_key": article.get("dedup_key"),
                    "published_at": article.get("published_at"),
                    "collected_at": article.get("collected_at"),
                    "title": result.get("title"),
                    "source_category": result.get("source_category"),
                    **decision,
                }, ensure_ascii=False) + "\n")

    summary = {
        "ruleset_version": rules.get("version"),
        "date": date_partition,
        "total_count": sum(counts.values()),
        "selected_count": counts["high"] + counts["review"],
        "counts": counts,
        "raw_path": raw_path.relative_to(output_dir).as_posix(),
        "selected_path": selected_path.relative_to(output_dir).as_posix(),
        "audit_path": audit_path.relative_to(output_dir).as_posix(),
        "summary_path": summary_path.relative_to(output_dir).as_posix(),
    }
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return summary


def _read_records(path: Path) -> list[dict[str, Any]]:
    raw = path.read_text(encoding="utf-8")
    if path.suffix == ".jsonl":
        return [json.loads(line) for line in raw.splitlines() if line.strip()]
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError("Input JSON must be an array or JSONL")
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description="Economic RSS article selector")
    parser.add_argument("--rules", default=DEFAULT_RULES_PATH)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--selected-only", action="store_true", help="Write only high/review; raw input is unchanged")
    parser.add_argument("--asof-time", help="PIT cutoff: ISO-8601 with timezone")
    args = parser.parse_args()
    records = select_articles(_read_records(args.input), args.rules, asof_time=args.asof_time)
    counts = {level: sum(a["selection"]["report_candidate_level"] == level for a in records) for level in ("high", "review", "noise")}
    if args.selected_only:
        records = [a for a in records if a["selection"]["report_candidate_level"] != "noise"]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.suffix == ".jsonl":
        args.output.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in records), encoding="utf-8")
    else:
        args.output.write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"input": sum(counts.values()), "output": len(records), "levels": counts}, ensure_ascii=False))


if __name__ == "__main__":
    main()
