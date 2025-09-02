import os, re, json, time, asyncio, hashlib, logging, random
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import aiohttp
import feedparser
import yaml
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# ---------- Config & State ----------

FEEDS_YAML = Path(os.environ.get("FEEDS_YAML", "./feeds.yml"))
STATE_PATH = Path(os.environ.get("STATE_PATH", "./status_state.json"))
FETCH_TIMEOUT = int(os.environ.get("FETCH_TIMEOUT_SECONDS", "15"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "8"))

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")

if not FEEDS_YAML.exists():
    raise SystemExit(f"Missing FEEDS_YAML file at {FEEDS_YAML}")

def load_config() -> Dict[str, Any]:
    cfg = yaml.safe_load(FEEDS_YAML.read_text())
    cfg.setdefault("defaults", {})
    cfg.setdefault("feeds", [])
    return cfg

def load_state() -> Dict[str, Any]:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    # seen_uids: set of unique entries we processed
    # title_seen_at: "normalized-title" -> ISO timestamp (for cross-feed dedupe)
    # threads: incident_id -> {"ts": "...", "channel": "#..."} (Bot mode only)
    return {"seen_uids": [], "title_seen_at": {}, "threads": {}}

def save_state(state: Dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2))

# ---------- Utils ----------

def _compile_words(words: List[str]) -> List[re.Pattern]:
    return [re.compile(rf"\b{re.escape(w)}\b", re.IGNORECASE) for w in words]

def norm_title(title: str) -> str:
    t = re.sub(r"\s+", " ", title).strip().lower()
    # drop common prefixes the vendors use
    t = re.sub(r"^(update|investigating|identified|monitoring|resolved)\s*[:\-]\s*", "", t)
    return t

def sha(obj: str) -> str:
    return hashlib.sha256(obj.encode("utf-8")).hexdigest()[:16]

def entry_uid(feed_url: str, e: Dict[str, Any]) -> str:
    raw = f"{feed_url}|{e.get('id','')}|{e.get('link','')}|{e.get('title','')}"
    return sha(raw)

def incident_id(e: Dict[str, Any]) -> str:
    # Prefer GUID/id; fallback to title+link hash
    base = e.get("id") or (e.get("title","") + "|" + e.get("link",""))
    return "i:" + sha(base)

def pick_severity(title: str, summary: str) -> str:
    txt = f"{title} {summary}".lower()
    if "major" in txt or "outage" in txt:
        return "Major"
    if "partial" in txt or "degraded" in txt:
        return "Partial/Degraded"
    return "Info"

# ---------- Slack (Webhook or Bot) ----------

async def slack_post_webhook(session: aiohttp.ClientSession, payload: Dict[str, Any]) -> bool:
    if not SLACK_WEBHOOK_URL:
        logging.error("No SLACK_WEBHOOK_URL configured.")
        return False
    async with session.post(SLACK_WEBHOOK_URL, json=payload, timeout=FETCH_TIMEOUT) as r:
        txt = await r.text()
        if r.status == 200:
            return True
        logging.error("Webhook post failed: %s %s", r.status, txt[:400])
        return False

async def slack_post_bot(session: aiohttp.ClientSession, channel: str, blocks: List[Dict[str, Any]], text: str, thread_ts: Optional[str] = None) -> Tuple[bool, Optional[str]]:
    if not SLACK_BOT_TOKEN:
        logging.error("No SLACK_BOT_TOKEN configured.")
        return (False, None)
    url = "https://slack.com/api/chat.postMessage"
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json; charset=utf-8"}
    payload = {"channel": channel, "text": text, "blocks": blocks}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    async with session.post(url, headers=headers, json=payload, timeout=FETCH_TIMEOUT) as r:
        data = await r.json()
        if data.get("ok"):
            return (True, data.get("ts"))
        logging.error("Bot post failed: %s", data)
        return (False, None)

def mk_blocks(service: str, title: str, link: str, summary: str, severity: str) -> List[Dict[str, Any]]:
    return [
        {"type": "header", "text": {"type": "plain_text", "text": f"{service}: {title}"[:150]}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Severity:* {severity}\n*Link:* <{link}|Status page>\n\n{summary[:2500]}"}}
    ]

# ---------- Filtering ----------

def is_alert_worthy(title: str, summary: str, pos_rx: List[re.Pattern], neg_rx: List[re.Pattern]) -> bool:
    text = f"{title}\n{summary}"
    if any(rx.search(text) for rx in neg_rx):
        return False
    return any(rx.search(text) for rx in pos_rx)

# ---------- Fetch & Process ----------

sem = asyncio.Semaphore(CONCURRENCY)

async def fetch_feed(session: aiohttp.ClientSession, url: str) -> feedparser.FeedParserDict:
    # small random jitter to avoid thundering herd
    await asyncio.sleep(random.uniform(0, 0.8))
    async with sem:
        async with session.get(url, timeout=FETCH_TIMEOUT) as r:
            content = await r.read()
            # feedparser works on bytes/string directly
            return feedparser.parse(content)

async def process_one_feed(session: aiohttp.ClientSession, feed_cfg: Dict[str, Any], defaults: Dict[str, Any], state: Dict[str, Any]) -> int:
    url = feed_cfg["url"]
    name = feed_cfg.get("name", url)
    positive = feed_cfg.get("positive_keywords", defaults.get("positive_keywords", []))
    negative = feed_cfg.get("negative_keywords", defaults.get("negative_keywords", []))
    pos_rx = _compile_words(positive)
    neg_rx = _compile_words(negative)
    dedupe_minutes = int(defaults.get("dedupe_minutes", 120))
    route = {**defaults.get("route", {}), **feed_cfg.get("route", {})}
    channel = route.get("channel")
    logging.info("Fetching feed: %s (%s)", feed_cfg.get("name"), feed_cfg["url"])

    d = await fetch_feed(session, url)
    entries = d.entries or []
    alerts = 0

    seen_uids = set(state.get("seen_uids", []))
    title_seen_at: Dict[str, str] = state.get("title_seen_at", {})

    for e in entries:
        title = e.get("title", "").strip()
        summary = (e.get("summary") or e.get("description") or "").strip()
        link = e.get("link", url)

        uid = entry_uid(url, e)
        if uid in seen_uids:
            continue

        # Cross-feed dedupe by normalized title within dedupe window
        nt = norm_title(title)
        now = datetime.now(timezone.utc)
        last_when_str = title_seen_at.get(nt)
        if last_when_str:
            last_when = datetime.fromisoformat(last_when_str)
            if now - last_when < timedelta(minutes=dedupe_minutes):
                seen_uids.add(uid)
                continue

        if not is_alert_worthy(title, summary, pos_rx, neg_rx):
            seen_uids.add(uid)
            continue

        severity = pick_severity(title, summary)
        blocks = mk_blocks(name, title, link, summary, severity)
        text = f"{name}: {title} — {link}"

        ok = False
        # Prefer Bot for per-channel routing + threads
        if SLACK_BOT_TOKEN and channel:
            # Thread by incident id
            iid = incident_id(e)
            existing = state["threads"].get(iid)
            if existing and existing.get("channel") == channel:
                ok, _ = await slack_post_bot(session, channel, blocks, text, thread_ts=existing.get("ts"))
            else:
                ok, ts = await slack_post_bot(session, channel, blocks, text)
                if ok and ts:
                    state["threads"][iid] = {"ts": ts, "channel": channel}
        else:
            # Webhook fallback (no threads, channel fixed by webhook)
            ok = await slack_post_webhook(session, {"text": text, "blocks": blocks})

        if ok:
            alerts += 1
            seen_uids.add(uid)
            title_seen_at[nt] = now.isoformat()

    state["seen_uids"] = list(seen_uids)
    state["title_seen_at"] = title_seen_at
    return alerts

async def main_async() -> None:
    cfg = load_config()
    state = load_state()
    async with aiohttp.ClientSession() as session:
        tasks = [process_one_feed(session, f, cfg["defaults"], state) for f in cfg["feeds"]]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    # Log exceptions but keep state
    total = 0
    for r in results:
        if isinstance(r, Exception):
            logging.exception("Feed error: %s", r)
        else:
            total += r
    save_state(state)
    logging.info("Processed %d feeds, sent %d alerts", len(cfg["feeds"]), total)

def main():
    # Optional: load .env if you use python-dotenv
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass
    asyncio.run(main_async())

if __name__ == "__main__":
    main()