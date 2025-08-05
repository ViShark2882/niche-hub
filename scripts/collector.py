# -*- coding: utf-8 -*-
"""
collector.py
Собирает материалы из:
  • RSS-лент (feedparser)
  • публичных Telegram-каналов (https://t.me/s/<channel>)
Пишет/дополняет data/catalog.csv с колонками:
uid,title,link,source,published,summary
"""

import csv, hashlib, re, datetime, pathlib
from urllib.parse import urlparse
import requests
import feedparser
from bs4 import BeautifulSoup

ROOT = pathlib.Path(__file__).resolve().parents[1]
FEEDS_FILE = ROOT / "data" / "feeds.txt"
CATALOG = ROOT / "data" / "catalog.csv"


# ---------- Утилиты ----------
def iso_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def norm_source(url):
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


# ---------- Чтение/заголовок каталога ----------
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


# ---------- Загрузка списка лент ----------
def load_feeds() -> list[tuple[str, str]]:
    feeds = []
    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            line = line.split("#", 1)[0].strip()  # режем комментарии справа
            if not line:
                continue
            token = line.split()[0]               # только первый «токен»
            if token.startswith("telegram:"):
                feeds.append(("telegram", token.split(":", 1)[1].strip()))
            else:
                feeds.append(("rss", token))
    return feeds


# ---------- Парсинг RSS ----------
def parse_rss(url: str) -> list[dict]:
    items = []
    d = feedparser.parse(url)
    for e in d.entries:
        link = e.get("link") or ""
        if not link:
            continue
        title = (e.get("title") or "").strip()
        summary = clean_text(e.get("summary") or e.get("description") or "", 500)

        # опубликовано
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


# ---------- Парсинг Telegram (публичные каналы /s/) ----------
def fetch_telegram(url: str) -> list[dict]:
    """
    Принимает:
      https://t.me/s/<channel>
      https://t.me/<channel>  (автоматически преобразуется в /s/)
    """
    url = url.strip()
    if re.match(r"^https?://t\.me/[^/]+$", url):
        url = url + "/s"
    if "/s/" not in url:
        url = url.replace("t.me/", "t.me/s/")

    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=25)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    items = []
    for block in soup.select("div.tgme_widget_message_wrap"):
        dp = block.get("data-post")  # формат: channel/1234
        if not dp:
            continue
        try:
            channel, msgid = dp.split("/", 1)
        except ValueError:
            continue
        link = f"https://t.me/{channel}/{msgid}"

        text_el = block.select_one(".tgme_widget_message_text") or block.select_one(".js-message_text")
        text = text_el.get_text(" ", strip=True) if text_el else ""
        title = clean_text(text, 100) or f"Telegram post {msgid}"

        # published
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
            "source": norm_source(link),
            "published": published,
            "summary": clean_text(text, 500)
        })
    return items


# ---------- Точка входа ----------
def main():
    ensure_header()
    existing = read_existing_uids()
    new_rows = []

    for typ, url in load_feeds():
        try:
            if typ == "rss":
                entries = parse_rss(url)
            elif typ == "telegram":
                entries = fetch_telegram(url)
            else:
                continue

            for it in entries:
                uid = make_uid(it["link"])
                if uid in existing:
                    continue
                new_rows.append([uid, it["title"], it["link"], it["source"], it["published"], it["summary"]])
                existing.add(uid)

        except Exception as e:
            print(f"WARN: failed {typ} {url}: {e}")
            continue

    added = 0
    if new_rows:
        with open(CATALOG, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for row in new_rows:
                w.writerow(row)
                added += 1

    print(f"Added {added} new items. Total: {len(existing)}")

if __name__ == "__main__":
    main()
