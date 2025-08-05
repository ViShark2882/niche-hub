# -*- coding: utf-8 -*-
"""
Страница /trends/: агрегирует темы из профильных RSS-лент за 7 дней
и извлекает сигналы со страниц трендов (Getty/Adobe/Shutterstock/Pond5).
Результат записывает в docs/trends/index.md для GitHub Pages.
"""

import csv
import re
import html
import datetime
import collections
import pathlib
from typing import List, Tuple

import requests
from bs4 import BeautifulSoup


# ---------- Пути ----------
ROOT = pathlib.Path(__file__).resolve().parents[1]
CATALOG = ROOT / "data" / "catalog.csv"
OUT_DIR = ROOT / "docs" / "trends"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Вендорские страницы с трендами (не RSS) ----------
TREND_PAGES = [
    "https://www.gettyimages.com/visualgps/creative-trends",
    "https://helpx.adobe.com/stock/contributor/help/artist-hub-migration/creat-what-s-in-demand.html",
    "https://www.shutterstock.com/explore/the-shot-list",
    "https://blog.pond5.com/",
]

# ---------- Фильтры и стоп-слова ----------
STOP_EN_RU = set(
    """
    and the for you your with from into what when how this that they them are our more learn click view read see free
    a an of on in to by as is it its be or not can new best top news blog post posts page pages site
    это как что для при или они она он его ее без уже ещё еще если когда куда либо либо-то либо-либо всех
    """.split()
)
STOP_HTML = {
    "amp","nbsp","href","img","src","class","quot","ins","figure",
    "html","jpg","http","https","www","com"
}
STOP_BRANDS = {"engadget","ixbt","habr","verge","cnet","gsmarena","apple","amazon","news"}
KEEP_NUM = {"4k","8k","1080p","720p","60fps","30fps","hdr"}

STOP = STOP_EN_RU | STOP_HTML | STOP_BRANDS

# Только домены про фото/стоки (+ t.me)
ALLOWED_DOMAINS = [
    "blog.pond5.com",
    "petapixel.com",
    "fstoppers.com",
    "ephotozine.com",
    "photographylife.com",
    "digital-photography-school.com",
    "feeds.feedburner.com",   # MicrostockInsider
    "t.me",                   # Telegram
]


def fetch_text(url: str, timeout: int = 25) -> str:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        parts = [t.get_text(" ", strip=True) for t in soup.select("h1, h2, h3, p, li, a")]
        return " ".join(parts)
    except Exception:
        return ""

def tokenize(text: str) -> List[str]:
    text = html.unescape(text or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.lower()
    text = re.sub(r"[^a-zа-яё0-9\s\-]+", " ", text)
    raw = [t.strip("-") for t in text.split()]
    out: List[str] = []
    for t in raw:
        if t in KEEP_NUM:
            out.append(t); continue
        if len(t) < 4 or t.isdigit():
            continue
        if t in STOP:
            continue
        out.append(t)
    return out

def _allowed(src: str) -> bool:
    s = (src or "").lower()
    return any(s.endswith(d) for d in ALLOWED_DOMAINS)

def top_keywords_from_catalog(days: int = 7, topn: int = 30) -> List[Tuple[str,int]]:
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
    bag = collections.Counter()
    if not CATALOG.exists():
        return []
    with open(CATALOG, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if not _allowed(row.get("source")):
                continue
            dt_str = (row.get("published") or "").strip().replace("Z","+00:00")
            if not dt_str:
                continue
            try:
                dt = datetime.datetime.fromisoformat(dt_str)
            except Exception:
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            if dt >= cutoff:
                txt = (row.get("title") or "") + " " + (row.get("summary") or "")
                bag.update(tokenize(txt))
    items = [(k,v) for k,v in bag.items() if k not in STOP]
    return sorted(items, key=lambda kv: kv[1], reverse=True)[:topn]

def top_phrases_from_catalog(n: int = 2, days: int = 7, topn: int =
