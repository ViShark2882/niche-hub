# -*- coding: utf-8 -*-
"""
collector.py
Собирает материалы из:
  • RSS-лент (feedparser)
  • публичных Telegram-каналов (через RSS-прокси + HTML-фолбэк)
Пишет/дополняет data/catalog.csv с колонками:
uid,title,link,source,published,summary
"""

import csv, hashlib, re, time, datetime, pathlib
from urllib.parse import urlparse
import requests
import feedparser
from bs4 import BeautifulSoup

ROOT = pathlib.Path(__file__).resolve().parents[1]
FEEDS_FILE = ROOT / "data" / "feeds.txt"
CATALOG = ROOT / "data" / "catalog.csv"

# ---------- базовые утилиты ----------
def iso_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def norm_source(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def make_uid(link: str) -> str:
    return hashlib.md5(link.encode("utf-8", "ignore")).hexdigest()

def clean_text(s: str, maxlen: int | None = None) -> str:
    if not s:
        return ""
    txt = re.sub(r"\s+", " ", s).strip()
    if maxlen and len(txt) > maxlen:
        txt = txt[:maxlen].rstrip() + "…"
    return txt

# ---------- подготовка каталога ----------
def ensure_header():
    if not CATALOG.exists() or CATALOG.stat().st_size == 0:
        with open(CATALOG, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["uid", "title", "link", "source", "published", "summary"])

def read_existing_uids() -> set[str]:
    uids = set()
    if CATALOG.exists():
        with open(CATALOG, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                uids.add(row.get("uid", ""))
    return uids

# ---------- читаем feeds.txt ----------
def load_feeds() -> list[tuple[str, str]]:
    feeds: list[tuple[str, str]] = []
    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            line = line.split("#", 1)[0].strip()  # режем комментарии справа
            if not line:
                continue
            token = line.split()[0]               # только первый токен
            if token.startswith("telegram:"):
                feeds.append(("telegram", token.split(":", 1)[1].strip()))
            else:
                feeds.append(("rss", token))
    return feeds

# ---------- RSS ----------
def parse_rss(url: str) -> list[dict]:
    items = []
    d = feedparser.parse(url)
    for e in d.entries:
        link = e.get("link") or ""
        if not link:
            continue
        title = (e.get("title") or "").strip()
        summary = clean_text(e.get("summary") or e.get("description") or "", 500)

        # published
        published = None
        for key in ("published_parsed", "updated_parsed"):
            t = e.get(key)
            if t:
                published = datetime.datetime(*t[:6], tzinfo=datetime.timezone.utc).isoformat()
                break
        if not published:
            published = iso_now()

        items.append({
            "title": title or clean_text(summary, 80) or norm_source(link),
            "link": link,
            "source": norm_source(link) or norm_source(url),
            "published": published,
            "summary": summary
        })
    return items

# ---------- TELEGRAM ----------
TG_RSS_PROXY = "https://tg.i-c-a.su/rss/{channel}"  # часто работает для публичных каналов

def parse_tg_rss(channel: str) -> list[dict]:
    url = TG_RSS_PROXY.format(channel=channel)
    items = []
    d = feedparser.parse(url)
    for e in d.entries:
        link = e.get("link") or ""
        if not link:
            # собираем линк вручную, если его нет
            m = re.search(r"/(\d+)", e.get("id",""))
            if m:
                link = f"https://t.me/{channel}/{m.group(1)}"
        if not link:
            continue
        title = (e.get("title") or "").strip()
        summary = clean_text(e.get("summary") or e.get("description") or "", 500)
        # time
        published = None
        for key in ("published_parsed", "updated_parsed"):
            t = e.get(key)
            if t:
                published = datetime.datetime(*t[:6], tzinfo=datetime.timezone.utc).isoformat()
                break
        if not published:
            published = iso_now()
        items.append({
            "title": title or clean_text(summary, 100) or f"Telegram post",
            "link": link,
            "source": "t.me",
            "published": published,
            "summary": summary or title
        })
    return items

def normalize_tg_url(url: str) -> tuple[str, str]:
    m = re.match(r"^https?://t\.me/(?:s/)?([^/?#]+)", url.strip())
    if not m:
        raise ValueError("Not a t.me URL")
    channel = m.group(1)
    page_url = f"https://t.me/s/{channel}"
    return channel, page_url

def parse_tg_html(channel: str) -> list[dict]:
    """
    HTML-фолбэк: пытаемся взять t.me/s/<channel>;
    если не удалось/JS-страница — используем зеркальный рендер r.jina.ai.
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    items = []
    page_url = f"https://t.me/s/{channel}"

    try:
        r = requests.get(page_url, headers=headers, timeout=25)
        use_mirror = (r.status_code >= 400) or ("tgme_widget_message_wrap" not in r.text)
        if use_mirror:
            mirror = f"https://r.jina.ai/http://t.me/s/{channel}"
            r = requests.get(mirror, headers=headers, timeout=25)
            r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
    except Exception as e:
        print(f"WARN: telegram html failed for {channel}: {e}")
        return items

    blocks = soup.select("div.tgme_widget_message_wrap")

    # Если «зеркало» отдало плоский текст — выковыриваем ссылки/текст
    if not blocks:
        for a in soup.select("a[href*='t.me/']"):
            href = a.get("href", "")
            m = re.match(rf"^https?://t\.me/{re.escape(channel)}/(\d+)", href)
            if not m:
                continue
            msgid = m.group(1)
            link = f"https://t.me/{channel}/{msgid}"
            text = a.get_text(" ", strip=True)
            if not text:
                continue
            items.append({
                "title": clean_text(text, 100) or f"Telegram post {msgid}",
                "link": link,
                "source": "t.me",
                "published": iso_now(),
                "summary": clean_text(text, 500)
            })
        return items

    for block in blocks:
        dp = block.get("data-post")  # channel/1234
        if not dp:
            continue
        try:
            _, msgid = dp.split("/", 1)
        except ValueError:
            continue
        link = f"https://t.me/{channel}/{msgid}"

        text_el = block.select_one(".tgme_widget_message_text") or block.select_one(".js-message_text")
        text = text_el.get_text(" ", strip=True) if text_el else ""
        title = clean_text(text, 100) or f"Telegram post {msgid}"

        published = iso_now()
        ttag = block.select_one("time")
        if ttag:
            dt = ttag.get("datetime") or ttag.get("datetime-original")
            if dt:
                try:
                    if dt.endswith("Z"):
                        dt = dt.replace("Z", "+00:00")
                    dtobj = datetime.datetime.fromisoformat(dt)
                    if dtobj.tzinfo is None:
                        dtobj = dtobj.replace(tzinfo=datetime.timezone.utc)
                    published = dtobj.isoformat()
                except Exception:
                    pass

        items.append({
            "title": title,
            "link": link,
            "source": "t.me",
            "published": published,
            "summary": clean_text(text, 500)
        })

    return items

def fetch_telegram(url: str, throttle: float = 1.0) -> list[dict]:
    """
    1) Пытаемся через RSS-прокси tg.i-c-a.su;
    2) если пусто — парсим HTML (t.me/s + r.jina.ai).
    Печатаем статистику по каналу.
    """
    channel, _ = normalize_tg_url(url)
    via_rss = parse_tg_rss(channel)
    via_html = []
    if not via_rss:
        via_html = parse_tg_html(channel)

    total = len(via_rss) or len(via_html)
    print(f"TG channel {channel}: rss={len(via_rss)} html={len(via_html)} total={total}")
    time.sleep(throttle)
    return via_rss if via_rss else via_html

# ---------- точка входа ----------
def main():
    ensure_header()
    existing = read_existing_uids()
    added_total = 0
    fetched = {"rss": 0, "telegram": 0}
    added = {"rss": 0, "telegram": 0}

    feeds = load_feeds()
    for typ, url in feeds:
        try:
            if typ == "rss":
                entries = parse_rss(url)
            elif typ == "telegram":
                entries = fetch_telegram(url)
            else:
                continue

            fetched[typ] += len(entries)

            for it in entries:
                uid = make_uid(it["link"])
                if uid in existing:
                    continue
                with open(CATALOG, "a", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow([uid, it["title"], it["link"], it["source"], it["published"], it["summary"]])
                existing.add(uid)
                added_total += 1
                added[typ] += 1

        except Exception as e:
            print(f"WARN: failed {typ} {url}: {e}")
            continue

    print(f"Fetched: RSS={fetched['rss']}, TG={fetched['telegram']}")
    print(f"Added:   RSS={added['rss']}, TG={added['telegram']}, Total unique={len(existing)}")
    print(f"Added {added_total} new items. Total: {len(existing)}")

if __name__ == "__main__":
    main()
