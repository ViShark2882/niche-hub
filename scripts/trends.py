# -*- coding: utf-8 -*-
"""
/trends/ — отчёт по темам для съёмок.
Берём сигналы из data/catalog.csv за 7 дней (RSS + Telegram) и со страниц трендов
(Getty/Adobe/Shutterstock/Pond5), чистим «мусор», занижаем вес «железных» новостей,
подсчитываем биграммы/триграммы и сохраняем docs/trends/index.md.
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

# ---------- Страницы с трендами (не RSS) ----------
TREND_PAGES = [
    "https://www.gettyimages.com/visualgps/creative-trends",
    "https://helpx.adobe.com/stock/contributor/help/artist-hub-migration/creat-what-s-in-demand.html",
    "https://www.shutterstock.com/explore/the-shot-list",
    "https://blog.pond5.com/",
]

# ---------- Доменные веса (что важнее для стоков) ----------
DOMAIN_WEIGHTS = {
    # фотостоки / медиа
    "blog.pond5.com": 3.0,
    "www.shutterstock.com": 2.5,
    "shutterstock.com": 2.5,
    "blog.depositphotos.com": 2.2,
    "blog.dreamstime.com": 2.0,
    "www.alamy.com": 2.0,
    "videvo.net": 2.0,
    "motionarray.com": 1.8,
    "feeds.feedburner.com": 1.6,  # MicrostockInsider
    "iso.500px.com": 1.5,
    "t.me": 2.0,  # Telegram

    # общие фотоблоги — понижаем
    "petapixel.com": 0.6,
    "www.petapixel.com": 0.6,
    "fstoppers.com": 0.8,
    "www.fstoppers.com": 0.8,
    "ephotozine.com": 0.5,
    "www.ephotozine.com": 0.5,
    "photographylife.com": 0.9,
    "digital-photography-school.com": 1.2,
}

# ---------- Стоп-слова и шум ----------
STOP_EN_RU = set(
    """
    and the for you your with from into what when how this that they them are our more learn click view read see free
    a an of on in to by as is it its be or not can new best top news blog post posts page pages site here most tips about
    это как что для при или они она он его ее без уже ещё еще если когда куда либо либо-то либо-либо всех тут
    """.split()
)

# HTML/WordPress-«шелуха» и тех.шум
STOP_HTML = {
    "amp","nbsp","href","img","src","class","quot","ins","figure","html","jpg","jpeg","png",
    "http","https","www","com","assets","uploads","background","height","width","size","card",
    "wp","wp-post-image","attachment","attachment-card-large","size-card-large"
}

# бренды и общие «железные» токены, которые не дают сюжетов
STOP_BRANDS = {
    "nikon","canon","sony","fuji","fujifilm","pentax","leica","sigma","tamron","viltrox","voigtlander",
    "nokton","rf","ef","af","dslr","mirrorless","sensor","aperture","bokeh","lens","lenses","frame"
}

# оставляем полезные числовые «слова»
KEEP_NUM = {"4k","8k","1080p","720p","60fps","30fps","hdr","9x16","9:16"}

STOP = STOP_EN_RU | STOP_HTML | STOP_BRANDS

# Регексы для отсечения «железных» грамм (85mm, 50mm, f/1.8, 28-135mm и т. п.)
RE_MM = re.compile(r"\b\d{2,3}mm\b")
RE_F = re.compile(r"\bf/?\d+(\.\d+)?\b", re.IGNORECASE)
RE_WP = re.compile(r"^(wp-|attachment-|size-)")
RE_URLISH = re.compile(r"(https?|www\.|\.com\b|\.jpg\b|\.png\b)")

# Разрешённые домены (источники для каталога) — оставляем фотостоки и Telegram
ALLOWED_DOMAINS = [
    "blog.pond5.com",
    "shutterstock.com", "www.shutterstock.com",
    "blog.depositphotos.com",
    "blog.dreamstime.com",
    "www.alamy.com",
    "videvo.net",
    "motionarray.com",
    "feeds.feedburner.com",   # MicrostockInsider
    "iso.500px.com",
    "t.me",
    # при желании можно вернуть общие блоги, но веса у них и так низкие
]

# ---------- Утилиты ----------
def fetch_text(url: str, timeout: int = 25) -> str:
    """Текст со страниц трендов: заголовки и абзацы."""
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        parts = [t.get_text(" ", strip=True) for t in soup.select("h1, h2, h3, p, li, a")]
        return " ".join(parts)
    except Exception:
        return ""

def is_noise_token(t: str) -> bool:
    if not t:
        return True
    if t in STOP:
        return True
    if RE_WP.match(t):
        return True
    if RE_URLISH.search(t):
        return True
    if len(t) <= 3 and t not in KEEP_NUM:
        return True
    if t.isdigit():
        return True
    return False

def tokenize(text: str) -> List[str]:
    """Чистим HTML, выкидываем мусор, оставляем осмысленные токены."""
    text = html.unescape(text or "")
    text = re.sub(r"<[^>]+>", " ", text)      # очистка HTML
    text = text.lower()
    text = re.sub(r"[^a-zа-яё0-9\s\-:x]+", " ", text)  # допустим 9x16 и 9:16
    raw = [t.strip("-").strip() for t in text.split()]
    out: List[str] = []
    for t in raw:
        if t in KEEP_NUM:
            out.append(t); continue
        if is_noise_token(t):
            continue
        out.append(t)
    return out

def token_is_gear(t: str) -> bool:
    if t in STOP_BRANDS:
        return True
    if RE_MM.search(t):
        return True
    if RE_F.search(t):
        return True
    return False

def gram_is_gear(gram: str) -> bool:
    parts = gram.split()
    if any(token_is_gear(p) for p in parts):
        return True
    # явные «железные» сочетания
    if "lens" in parts or "mm" in parts or "camera" in parts:
        return True
    return False

def allowed_source(src: str) -> bool:
    s = (src or "").lower()
    return any(s.endswith(d) for d in ALLOWED_DOMAINS)

def weight_for_source(src: str) -> float:
    s = (src or "").lower()
    return DOMAIN_WEIGHTS.get(s, 1.0)

# ---------- Агрегация по каталогу ----------
def top_words_and_phrases(days: int = 7, topn_words: int = 30, topn_bi: int = 30, topn_tri: int = 20):
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
    words = collections.Counter()
    bi = collections.Counter()
    tri = collections.Counter()

    if not CATALOG.exists():
        return [], [], []

    with open(CATALOG, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            src = row.get("source") or ""
            if not allowed_source(src):
                continue

            dt_str = (row.get("published") or "").strip().replace("Z", "+00:00")
            if not dt_str:
                continue
            try:
                dt = datetime.datetime.fromisoformat(dt_str)
            except Exception:
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            if dt < cutoff:
                continue

            w = weight_for_source(src)
            txt = (row.get("title") or "") + " " + (row.get("summary") or "")
            toks = tokenize(txt)
            # слова
            for t in toks:
                if token_is_gear(t):
                    continue
                words[t] += w
            # биграммы
            for i in range(len(toks) - 1):
                g = f"{toks[i]} {toks[i+1]}"
                if gram_is_gear(g):
                    continue
                bi[g] += w
            # триграммы
            for i in range(len(toks) - 2):
                g = f"{toks[i]} {toks[i+1]} {toks[i+2]}"
                if gram_is_gear(g):
                    continue
                tri[g] += w

    top_words = words.most_common(topn_words)
    top_bi = bi.most_common(topn_bi)
    top_tri = tri.most_common(topn_tri)
    return top_words, top_bi, top_tri

# ---------- Сигналы с официальных тренд-страниц ----------
def signals_from_vendor_pages() -> List[Tuple[str, float]]:
    bag = collections.Counter()
    for url in TREND_PAGES:
        txt = fetch_text(url)
        if not txt:
            continue
        toks = tokenize(txt)
        for t in toks:
            if token_is_gear(t):
                continue
            bag[t] += 2.0  # немного повышаем вес
    return bag.most_common(40)

# ---------- Сборка страницы ----------
def write_report() -> None:
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    top_words, top_bi, top_tri = top_words_and_phrases()
    kw_pages = signals_from_vendor_pages()

    def fmt(lst, limit=None):
        if limit is not None:
            lst = lst[:limit]
        return [f"- {k} — {int(c) if isinstance(c,(int,float)) else c}" for k,c in lst]

    md = [
        "---",
        "layout: page",
        "title: Тренды недели",
        "permalink: /trends/",
        "---",
        "",
        f"_Автообновление: {today} (UTC)_",
        "",
        "## ТОП фразы из лент (биграммы)",
        *fmt(top_bi, 20),
        "",
        "## ТОП фразы из лент (триграммы)",
        *fmt(top_tri, 15),
        "",
        "## Частые слова (контроль шума)",
        *fmt(top_words, 20),
        "",
        "## Сигналы из страниц трендов (Getty/Adobe/Shutterstock/Pond5)",
        *fmt(kw_pages, 25),
        "",
        "> Используйте фразы как темы съёмок и ключевые слова при загрузке на стоки.",
        "",
    ]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "index.md").write_text("\n".join(md), encoding="utf-8")


if __name__ == "__main__":
    write_report()
