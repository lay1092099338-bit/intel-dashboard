#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
intel-dashboard ingest

读取当日各 cron 产生的 /tmp/*.json，合并到 data.json 并 git push。

数据源（折中方案，Telegram 不入库）:
- /tmp/reddit_daily.json   →  reddit tab
- /tmp/cam101_report_09.json + /tmp/cam101_report_22.json  →  cam101 tab

不再读 cron 简报；以"上游真正新内容"为准。

去重：按 item.id（reddit-{post_id}, cam101-{post_id}）。已有 id 则覆盖（反映状态/回帖更新）。

幂等：当天可多次运行；只在 data 实际变化时才提交。
"""

import os, json, sys, subprocess, hashlib, time
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_FILE = ROOT / "data.json"
HISTORY_DIR = ROOT / "history"
HISTORY_DIR.mkdir(exist_ok=True)

REDDIT_DAILY = Path("/tmp/reddit_daily.json")
REDDIT_VIBEMATE_CAMGIRL = Path("/tmp/reddit_vibemate_camgirl.json")
REDDIT_VIBEMATE_FAM = Path("/tmp/reddit_vibemate_fam.json")
CAM101_REPORTS = [Path("/tmp/cam101_report_09.json"), Path("/tmp/cam101_report_22.json")]

TZ = timezone(timedelta(hours=8))


def now_iso():
    return datetime.now(TZ).strftime("%Y-%m-%dT%H:%M:%S+08:00")


def today_str():
    return datetime.now(TZ).strftime("%Y-%m-%d")


def load_json(p: Path):
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[warn] failed to parse {p}: {e}")
        return None


def classify_reddit(item) -> str:
    """根据 daily_fetch 的 tags 推断 dashboard category"""
    tags = item.get("tags", []) or []
    if "bug" in tags:
        return "bug"
    if "trust" in tags or "confusion" in tags:
        return "negative"
    if "positive" in tags:
        return "positive"
    return "neutral"


def reddit_items_from_daily(daily) -> list:
    """把 reddit_daily.json -> dashboard items"""
    out = []
    for it in daily.get("items", []):
        post_id = it.get("id") or ""
        if not post_id:
            continue
        sub = it.get("subreddit", "")
        permalink = it.get("permalink", "")
        url = f"https://www.reddit.com{permalink}" if permalink else it.get("url", "")
        # 时间戳：created_utc 是 UTC unix
        created = it.get("created_utc")
        if created:
            ts_dt = datetime.fromtimestamp(created, TZ)
            ts = ts_dt.strftime("%Y-%m-%dT%H:%M:%S+08:00")
        else:
            ts = now_iso()
        title = it.get("title", "")
        selftext = it.get("selftext", "") or ""
        # 拼新评论摘要（如果有）
        new_comments = it.get("new_comments", []) or []
        if new_comments and not selftext.strip():
            selftext = "[旧帖新增评论] " + " | ".join(
                (c.get("body", "")[:120] if isinstance(c, dict) else str(c)[:120])
                for c in new_comments[:3]
            )
        category = classify_reddit(it)
        out.append({
            "id": f"reddit-{post_id}",
            "source": "reddit",
            "subreddit": f"r/{sub}" if sub and not sub.startswith("r/") else (sub or ""),
            "title": title,
            "content": selftext[:500],
            "summary": f"自动入库 ({today_str()}): 关键词 {it.get('matched_keyword', '')}, tags={','.join(it.get('tags', []) or [])}",
            "category": category,
            "url": url,
            "score": it.get("score", 0),
            "timestamp": ts,
            "status": "pending",
        })
    return out


def _classify_by_sentiment(sentiment: str) -> str:
    s = (sentiment or "").lower().strip()
    if s == "positive":
        return "positive"
    if s == "negative":
        return "negative"
    return "neutral"


def reddit_items_from_vibemate(payload, source_label: str) -> list:
    """把 vibemate_camgirl / vibemate_fam tmp -> dashboard items"""
    out = []
    sub = payload.get("subreddit", "")
    for it in payload.get("items", []) or []:
        pid = it.get("id") or ""
        if not pid:
            continue
        permalink = it.get("permalink", "")
        url = f"https://www.reddit.com{permalink}" if permalink else ""
        created = it.get("created_utc")
        if created:
            ts = datetime.fromtimestamp(created, TZ).strftime("%Y-%m-%dT%H:%M:%S+08:00")
        else:
            ts = now_iso()
        category = _classify_by_sentiment(it.get("sentiment", ""))
        # vibemate_camgirl 默认负面信号优先；vibemate_fam 默认中性
        if not it.get("sentiment"):
            category = "neutral"
        post_type = it.get("post_type", "")
        summary_bits = [f"自动入库 ({today_str()})", f"源: {source_label}"]
        if post_type:
            summary_bits.append(f"type={post_type}")
        if it.get("matched_keyword"):
            summary_bits.append(f"kw={it['matched_keyword']}")
        if it.get("note"):
            summary_bits.append(f"note: {it['note'][:120]}")
        out.append({
            "id": f"reddit-{pid}",
            "source": "reddit",
            "subreddit": f"r/{sub}" if sub and not sub.startswith("r/") else (sub or ""),
            "title": it.get("title", ""),
            "content": (it.get("selftext", "") or "")[:500],
            "summary": "; ".join(summary_bits),
            "category": category,
            "url": url,
            "score": it.get("score", 0),
            "timestamp": ts,
            "status": "pending",
        })
    return out


def cam101_items_from_report(rep) -> list:
    """把 cam101_report_*.json -> dashboard items（从 replied 列表）"""
    out = []
    run_time = rep.get("run_time", "")  # "YYYY-MM-DD HH:MM"
    if run_time:
        try:
            ts_dt = datetime.strptime(run_time, "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
            ts = ts_dt.strftime("%Y-%m-%dT%H:%M:%S+08:00")
        except Exception:
            ts = now_iso()
    else:
        ts = now_iso()
    for r in rep.get("replied", []) or []:
        pid = r.get("post_id") or ""
        if not pid:
            continue
        out.append({
            "id": f"cam101-{pid}",
            "source": "cam101",
            "title": r.get("post_title", ""),
            "content": (r.get("note") or "")[:500],
            "summary": f"Cam101 论坛回帖, slot={rep.get('slot', '')}",
            "category": "positive",  # 主动回帖默认 positive
            "url": r.get("post_url", ""),
            "timestamp": ts,
            "replied": True,
            "replyText": r.get("reply_text", ""),
            "status": "processed",
        })
    return out


def upsert_items(tab_items: list, new_items: list) -> tuple[int, int]:
    """按 id 合并；返回 (added, updated)"""
    by_id = {it.get("id"): i for i, it in enumerate(tab_items)}
    added = 0
    updated = 0
    for ni in new_items:
        nid = ni.get("id")
        if not nid:
            continue
        if nid in by_id:
            old = tab_items[by_id[nid]]
            # 仅当内容有差异才计入 updated
            merged = {**old, **ni}
            if merged != old:
                tab_items[by_id[nid]] = merged
                updated += 1
        else:
            tab_items.append(ni)
            by_id[nid] = len(tab_items) - 1
            added += 1
    return added, updated


def run_git(*args, cwd=None) -> tuple[int, str]:
    p = subprocess.run(
        ["git", *args],
        cwd=cwd or str(ROOT),
        capture_output=True, text=True, timeout=120,
    )
    return p.returncode, (p.stdout + p.stderr).strip()


def main():
    if not DATA_FILE.exists():
        print(f"[fatal] {DATA_FILE} not found")
        sys.exit(2)
    data = json.loads(DATA_FILE.read_text(encoding="utf-8"))

    summary = {"reddit": {"added": 0, "updated": 0}, "cam101": {"added": 0, "updated": 0}}

    # ---- reddit (multi-source: daily / vibemate_camgirl / vibemate_fam) ----
    reddit_sources = [
        (REDDIT_DAILY, reddit_items_from_daily, None),
        (REDDIT_VIBEMATE_CAMGIRL, lambda p: reddit_items_from_vibemate(p, "vibemate_camgirl"), "vibemate_camgirl"),
        (REDDIT_VIBEMATE_FAM, lambda p: reddit_items_from_vibemate(p, "vibemate_fam"), "vibemate_fam"),
    ]
    for src_path, parser, label in reddit_sources:
        rd = load_json(src_path)
        if not rd:
            print(f"[info] no {src_path}")
            continue
        items = parser(rd)
        if items:
            tab = data["tabs"].setdefault("reddit", {"label": "Reddit", "items": []})
            a, u = upsert_items(tab["items"], items)
            summary["reddit"]["added"] += a
            summary["reddit"]["updated"] += u
        # 备份到 history
        try:
            tag = label or "reddit_daily"
            (HISTORY_DIR / f"{today_str()}-{tag}.json").write_text(
                json.dumps(rd, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception as e:
            print(f"[warn] history write failed: {e}")

    # ---- cam101 ----
    for p in CAM101_REPORTS:
        rep = load_json(p)
        if not rep:
            continue
        items = cam101_items_from_report(rep)
        if items:
            tab = data["tabs"].setdefault("cam101", {"label": "Cam101 论坛", "items": []})
            a, u = upsert_items(tab["items"], items)
            summary["cam101"]["added"] += a
            summary["cam101"]["updated"] += u
        try:
            (HISTORY_DIR / f"{today_str()}-{p.name}").write_text(
                json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception as e:
            print(f"[warn] history write failed: {e}")

    total_added = sum(s["added"] for s in summary.values())
    total_updated = sum(s["updated"] for s in summary.values())

    if total_added == 0 and total_updated == 0:
        print(f"[ok] no changes  reddit={summary['reddit']}  cam101={summary['cam101']}")
        return

    # bump lastUpdated
    data["lastUpdated"] = now_iso()
    DATA_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # git commit + push
    rc, out = run_git("status", "--porcelain", "data.json")
    if rc != 0:
        print(f"[warn] git status failed: {out}")
    if not out.strip():
        print("[ok] data.json bytes unchanged after format roundtrip; skip commit")
        return

    rc, out = run_git("add", "data.json")
    msg = (
        f"data: ingest {today_str()}  "
        f"reddit +{summary['reddit']['added']}/~{summary['reddit']['updated']}  "
        f"cam101 +{summary['cam101']['added']}/~{summary['cam101']['updated']}"
    )
    rc, out = run_git(
        "-c", "user.email=intel-bot@openclaw.local",
        "-c", "user.name=intel-bot",
        "commit", "-m", msg,
    )
    if rc != 0:
        print(f"[warn] git commit failed: {out}")
        return
    rc, out = run_git("push", "origin", "HEAD")
    if rc != 0:
        print(f"[err] git push failed: {out}")
        sys.exit(3)
    print(f"[ok] pushed: {msg}")


if __name__ == "__main__":
    main()
