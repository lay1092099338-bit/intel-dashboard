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

import os, json, sys, subprocess, hashlib, time, html, re, urllib.request, urllib.error
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


# ── LLM 配置（从 openclaw.json 读 modelverse provider）──
LLM_BASE = "https://api.modelverse.cn/v1"
LLM_MODEL = "claude-haiku-4-5"  # 摘要用轻量型，找不到则 fallback
LLM_API_KEY = None
try:
    _cfg = json.loads(Path("/home/ubuntu-m/.openclaw/openclaw.json").read_text())
    _p = _cfg["models"]["providers"]["custom-api-modelverse-cn"]
    LLM_BASE = (_p.get("baseUrl") or LLM_BASE).rstrip("/")
    LLM_API_KEY = _p.get("apiKey")
    avail = {m.get("id") for m in (_p.get("models") or []) if isinstance(m, dict)}
    for prefer in ("claude-haiku-4-5", "claude-haiku-4-6", "claude-sonnet-4-6", "claude-sonnet-4-7"):
        if prefer in avail:
            LLM_MODEL = prefer
            break
except Exception as e:
    print(f"[warn] cannot read modelverse config: {e}")


def llm_summarize(title: str, body: str, source_label: str = "reddit") -> str:
    """调 modelverse anthropic-messages 生成中文一句话摘要。失败时返回空字符串。"""
    if not LLM_API_KEY:
        return ""
    body = (body or "").strip()
    title = (title or "").strip()
    if not body and not title:
        return ""
    body_short = body[:1200]
    prompt = (
        f"下面是一条来自 {source_label} 的帖子，请用中文生成一句话要点摘要（≤2 句、≤50 字）："
        f"主题 + 用户意图/情绪信号。不要 emoji、不要前缀、不要引号、不要重复标题。\n\n"
        f"标题：{title}\n正文：{body_short}"
    )
    payload = {
        "model": LLM_MODEL,
        "max_tokens": 200,
        "messages": [{"role": "user", "content": prompt}],
    }
    req = urllib.request.Request(
        f"{LLM_BASE}/messages",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "x-api-key": LLM_API_KEY,
            "anthropic-version": "2023-06-01",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read()[:200].decode("utf-8", "ignore")
        except Exception:
            err_body = ""
        print(f"[warn] llm http {e.code}: {err_body}")
        return ""
    except Exception as e:
        print(f"[warn] llm call failed: {e}")
        return ""
    parts = data.get("content") or []
    text = "".join(
        p.get("text", "") for p in parts if isinstance(p, dict) and p.get("type") == "text"
    )
    return text.strip().replace("\n", " ")[:200]


def now_iso():
    return datetime.now(TZ).strftime("%Y-%m-%dT%H:%M:%S+08:00")


def clean_html(s: str) -> str:
    """把 reddit 返的 HTML 编码正文转成纯文本。
    处理：
    - HTML entity 解码（&lt; -> <）
    - SC_OFF/SC_ON 注释去掉
    - 去标签（保留文本）
    - 多余空白压缩
    """
    if not s:
        return ""
    # 两轮解码（防双层转义：&amp;lt; -> &lt; -> <）
    s = html.unescape(html.unescape(s))
    # 去掉 reddit 的 SC_OFF/SC_ON 注释
    s = re.sub(r"<!--\s*SC_(OFF|ON)\s*-->", "", s)
    # 去 HTML 标签
    s = re.sub(r"<[^>]+>", " ", s)
    # 压缩空白
    s = re.sub(r"\s+", " ", s).strip()
    return s


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


# ── 分类规则（2026-05-12 重审后的标准）──
# 原则：
#   positive  = 用户主动表达正向信号（获奖庆祝/感谢/品牌推荐/收入提升/主动推广）
#   bug       = 明确报告功能异常
#   negative  = 抱怨/可疑用户/地区限制/负面体验
#   neutral   = 产品咨询/讨论/我方运营/欢迎贴/社交贴/中性分享

BUG_PHRASES = [
    "not working", "isn't connecting", "isnt connecting", "doesn't connect", "doesnt connect",
    "won't connect", "wont connect", "can't connect", "cant connect",
    "broken", "loading error", "failed to", "stuck on", "crashes", "crashed",
    "doesn't work", "doesnt work", "won't work", "wont work", "stopped working",
    "glitch", "freezes", "frozen", "disconnect", "won't load", "wont load",
]

NEGATIVE_PHRASES = [
    "scam", "fake", "strange user", "suspicious", "sketchy", "fraud", "phishing",
    "restriction", "banned", "can't use in", "blocked in", "not available in",
    "exhausting", "frustrat", "angry", "disappoint", "refund",
]

# “用户主动正向”强信号
STRONG_POSITIVE_PHRASES = [
    # 获奖/庆祝
    "i won", "i've won", "won a prize", "won the", "prize win", "lucky prize",
    "carnival prize", "first token",
    # 感谢
    "thank you for", "thanks for the tutorial", "agradecimiento", "appreciate",
    # 品牌推广 / 收入提升
    "earning so much more", "earn extra revenue", "sells twice more",
    "check out lovense", "recommend lovense", "i recommend",
    # 社区表达爱
    "real love and support", "so glad",
]

# "咨询" / “中性讨论”强信号—— 这些即使含“good/great/love”也不是 positive
NEUTRAL_PHRASES = [
    "has anyone tried", "does anyone use", "anybody tried", "any tips",
    "how do i", "how does", "questions about", "need help", "need advice",
    "recommend", "which one",
]


def classify_text(title: str, body: str) -> str:
    """统一分类函数（供各源使用）。优先级：bug > negative > strong_positive > neutral."""
    full = (title + " " + body).lower()
    for kw in BUG_PHRASES:
        if kw in full:
            return "bug"
    for kw in NEGATIVE_PHRASES:
        if kw in full:
            return "negative"
    # 先看咨询/讨论信号（避免“thanks”之类误伤）
    is_inquiry = any(kw in full for kw in NEUTRAL_PHRASES)
    for kw in STRONG_POSITIVE_PHRASES:
        if kw in full:
            # 咨询贴即使包含“thanks”也还是 neutral
            if is_inquiry and kw in ("thank you for", "appreciate"):
                continue
            return "positive"
    return "neutral"


def classify_reddit(item) -> str:
    """优先看 daily_fetch 的 tags，其次起 fallback 到关键词分类。"""
    tags = item.get("tags", []) or []
    if "bug" in tags:
        return "bug"
    if "trust" in tags:
        return "negative"
    # “positive” tag 太宽宽、在 daily_fetch 里包含 love it/great 等词——不一定是用户正向信号。
    # 不再直接信任 tag=positive，还是走文本分类判断。
    title = item.get("title", "")
    body = item.get("selftext", "") or ""
    return classify_text(title, body)


def reddit_items_from_daily(daily) -> list:
    """把 reddit_daily.json -> dashboard items。

    字段兼容两种上游格式：
    - 新版 daily_fetch 写 `content`（HTML编码的正文）
    - 老版/其他接口写 `selftext`（纯文本）
    """
    out = []
    for it in daily.get("items", []):
        post_id = it.get("id") or ""
        if not post_id:
            continue
        sub = it.get("subreddit", "")
        permalink = it.get("permalink", "")
        url = f"https://www.reddit.com{permalink}" if permalink else it.get("url", "")
        # 时间戳：created_utc 是 UTC unix；daily_fetch 只在 run_time 里写了入库时间
        created = it.get("created_utc")
        if created:
            ts_dt = datetime.fromtimestamp(created, TZ)
            ts = ts_dt.strftime("%Y-%m-%dT%H:%M:%S+08:00")
        elif it.get("run_time"):
            ts = it["run_time"]
        else:
            ts = now_iso()
        title = it.get("title", "") or ""
        # 优先 content（HTML 编码，需清洗），其次 selftext
        raw_body = it.get("content") or it.get("selftext") or ""
        body = clean_html(raw_body)
        # 拼新评论摘要（如果有）
        new_comments = it.get("new_comments", []) or []
        if new_comments and not body.strip():
            body = "[旧帖新增评论] " + " | ".join(
                clean_html(c.get("body", "") if isinstance(c, dict) else str(c))[:120]
                for c in new_comments[:3]
            )
        # 仍然为空（图片帖/视频帖/外链帖）→ 给出可读提示
        if not body.strip():
            body = f"[无正文内容 · {it.get('post_type') or 'link/image post'}] 原帖只有标题/图片/外链，请点击链接查看。"
        # summary: 优先用上游资产的 note（中文提要），没有则 LLM 生成
        summary = (it.get("note") or "").strip()
        if not summary and body and not body.startswith("[无正文"):
            summary = llm_summarize(title, body, source_label=f"reddit r/{sub}" if sub else "reddit")
        category = classify_reddit(it)
        out.append({
            "id": f"reddit-{post_id}",
            "source": "reddit",
            "subreddit": f"r/{sub}" if sub and not sub.startswith("r/") else (sub or ""),
            "title": title,
            "content": body[:1000],
            "summary": summary,
            "category": category,
            "url": url,
            "score": it.get("score", 0),
            "timestamp": ts,
            "status": "pending",
        })
    return out


def _classify_by_sentiment(sentiment: str, title: str = "", body: str = "") -> str:
    """优先走文本分类（准确）；sentiment 只作为 fallback。"""
    if title or body:
        cat = classify_text(title, body)
        # 如果文本分类出 bug/negative/positive，以文本判断为准
        if cat != "neutral":
            return cat
    # 文本为 neutral 时才看 sentiment——但 sentiment=positive 不够强，“仅凭 sentiment 到 positive”在这里不够资格让我们打 positive
    s = (sentiment or "").lower().strip()
    if s == "negative":
        return "negative"
    return "neutral"


def reddit_items_from_vibemate(payload, source_label: str) -> list:
    """把 vibemate_camgirl / vibemate_fam tmp -> dashboard items。

    上游 SKILL prompt 要求写 selftext（纯文本）+ note（AI中文一句话提要）。
    为了鲁棒也兼容 content 字段。
    """
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
        title = it.get("title", "") or ""
        raw_body = it.get("selftext") or it.get("content") or ""
        body = clean_html(raw_body)
        if not body.strip():
            post_type = it.get("post_type") or "image/link"
            body = f"[无正文内容 · {post_type}] 原帖只有标题/图片/外链。"
        summary = (it.get("note") or "").strip()
        if not summary and body and not body.startswith("[无正文"):
            summary = llm_summarize(title, body, source_label=f"reddit r/{sub}" if sub else "reddit")
        category = _classify_by_sentiment(
            it.get("sentiment", ""),
            title=title,
            body=body,
        )
        out.append({
            "id": f"reddit-{pid}",
            "source": "reddit",
            "subreddit": f"r/{sub}" if sub and not sub.startswith("r/") else (sub or ""),
            "title": title,
            "content": body[:1000],
            "summary": summary,
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
    ts = None
    # 防御：run_time 必须是 "YYYY-MM-DD HH:MM" 格式，长度至少 16，且前 10 位形如日期
    if isinstance(run_time, str) and len(run_time) >= 16 and run_time[4] == '-' and run_time[7] == '-':
        try:
            ts_dt = datetime.strptime(run_time[:16], "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
            ts = ts_dt.strftime("%Y-%m-%dT%H:%M:%S+08:00")
        except Exception as e:
            print(f"[warn] cam101 run_time parse failed ({run_time!r}): {e}, fallback now")
    if not ts:
        if run_time:
            print(f"[warn] cam101 run_time malformed ({run_time!r}), fallback now")
        ts = now_iso()
    for r in rep.get("replied", []) or []:
        pid = r.get("post_id") or ""
        if not pid:
            continue
        # cam101 上游字段：post_title=原帖标题；note=AI一句话说明为何回；reply_text=我们回复内容。
        # post_content/post_body=原帖正文（如果 SKILL 产出）。
        # 字段映射：
        #   content   → 原帖正文（优先 post_content/post_body）或 “[无正文]” 提示
        #   summary   → note（中文一句话说明）
        #   replyText → reply_text
        post_title = r.get("post_title", "") or ""
        post_note = (r.get("note", "") or "").strip()
        post_body = r.get("post_content") or r.get("post_body") or ""
        post_body = clean_html(post_body) if post_body else ""
        if not post_body:
            post_body = f"[cam101 原帖无正文或未采集] {post_note}" if post_note else "[cam101 原帖无正文或未采集]"
        category = classify_text(post_title, post_note)
        out.append({
            "id": f"cam101-{pid}",
            "source": "cam101",
            "title": post_title,
            "content": post_body[:1000],
            "summary": post_note,
            "category": category,
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
    ingest_ts = now_iso()  # 本次入库时间戳
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
            # 新条目：加 ingestedAt（首次入库时间）
            ni["ingestedAt"] = ingest_ts
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
