# -*- coding: utf-8 -*-
"""
collector.py
Собирает материалы из:
  • RSS-лент (feedparser)
  • публичных Telegram-каналов (https://t.me/s/<channel> или https://t.me/<channel>)
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
            line = line.split("#", 1)[0].strip()  # срезаем комментарии справа
            if not line:
                continue
            token = line.split()[0]               # берём только первый «токен»
            if token.startswith("telegram:"):
                # формат: telegram:https://t.me/...  или telegram:https://t.me/s/...
                feeds.append(("telegram", token.split(":", 1)[1].strip()))
            else:
                feeds.append(("rss", token))
    return feeds

# ---------- парсим RSS ----------
def parse_rss(url: str) -> list[dict]:
    items = []
    d = feedparser.parse(url)
    for e in d.entries:
        link = e.get("link") or ""
        if not link:
            continue
        title = (e.get("title") or "").strip()
        summary = clean_text(e.get("summary") or e.get("description") or "", 500)

        # время публикации
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

# ---------- парсим Telegram (публичные каналы/чаты) ----------
def normalize_tg_url(url: str) -> tuple[str, str]:
    """
    Возвращает (channel, page_url) для /s/<channel>
    Принимает:
      https://t.me/<channel>
      https://t.me/s/<channel>
      http://t.me/<channel> ...
    """
    url = url.strip()
    m = re.match(r"^https?://t\.me/(?:s/)?([^/?#]+)", url)
    if not m:
        raise ValueError("Not a t.me URL")
    channel = m.group(1)
    page_url = f"https://t.me/s/{channel}"
    return channel, page_url

def fetch_telegram(url: str, throttle: float = 1.0) -> list[dict]:
    """
    Скрейпит публичную страницу /s/<channel>.
    Если Telegram вернул 4xx/5xx, пробует зеркальный просмотр через r.jina.ai.
    """
    channel, page_url = normalize_tg_url(url)
    headers = {"User-Agent": "Mozilla/5.0"}
    items = []

    try:
        r = requests.get(page_url, headers=headers, timeout=25)
        if r.status_code >= 400 or "tgme_widget_message_wrap" not in r.text:
            # фолбэк: зеркальный текстовый рендер
            mirror = f"https://r.jina.ai/http://t.me/s/{channel}"
            r = requests.get(mirror, headers=headers, timeout=25)
            if r.status_code >= 400:
                raise RuntimeError(f"telegram mirror failed: {r.status_code}")
            soup = BeautifulSoup(r.text, "lxml")
        else:
            soup = BeautifulSoup(r.text, "lxml")
    except Exception as e:
        print(f"WARN: telegram fetch failed for {page_url}: {e}")
        return items

    # Структура сообщений
    blocks = soup.select("div.tgme_widget_message_wrap")
    # Если это «зеркальный» текст без div'ов — пытаемся собрать ссылки по тексту
    if not blocks:
        # ищем ссылки вида https://t.me/<channel>/<id>
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
        dp = block.get("data-post")  # формат: channel/1234
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

    # небольшой таймаут между каналами, чтобы не схлопотать 429
    time.sleep(throttle)
    return items

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
