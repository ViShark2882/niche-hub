import csv, re, os, datetime, collections, pathlib
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup

ROOT = pathlib.Path(__file__).resolve().parents[1]
CATALOG = ROOT / "data" / "catalog.csv"
OUT_DIR = ROOT / "docs" / "trends"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Официальные страницы с трендами (не RSS; берём заголовки/абзацы)
TREND_PAGES = [
    "https://www.gettyimages.com/visualgps/creative-trends",
    "https://helpx.adobe.com/stock/contributor/help/artist-hub-migration/creat-what-s-in-demand.html",
    "https://www.shutterstock.com/explore/the-shot-list",
    "https://blog.pond5.com/"
]

def fetch_text(url, timeout=25):
    try:
        html = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0"}).text
        soup = BeautifulSoup(html, "lxml")
        parts = [t.get_text(" ", strip=True) for t in soup.select("h1, h2, h3, p, li, a")]
        return " ".join(parts)
    except Exception:
        return ""

def tokenize(text):
    text = re.sub(r"[\t\r\n]+", " ", text.lower())
    text = re.sub(r"[^\w\s\-]+", " ", text)
    return [t for t in text.split() if 3 <= len(t) <= 24 and not t.isdigit()]

def top_keywords_from_catalog(days=7, topn=30):
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    bag = collections.Counter()
    if not CATALOG.exists():
        return []
    with open(CATALOG, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # collector пишет столбец 'published' в ISO; иногда пусто — пропускаем
            dt_str = (row.get("published") or "").replace("Z", "")
            if not dt_str:
                continue
            try:
                dt = datetime.datetime.fromisoformat(dt_str)
            except Exception:
                continue
            if dt >= cutoff:
                title = (row.get("title") or "") + " " + (row.get("summary") or "")
                bag.update(tokenize(title))
    stop = set("""and the for you your with from into what when how this that they them http https www com blog more read about adobe stock shutterstock pond5 getty image images video videos feed rss xml post posts page pages site""".split())
    items = [(k, v) for k, v in bag.items() if k not in stop]
    return sorted(items, key=lambda kv: kv[1], reverse=True)[:topn]

def signals_from_vendor_pages():
    bag = collections.Counter()
    for url in TREND_PAGES:
        txt = fetch_text(url)
        if txt:
            bag.update(tokenize(txt))
    stop = set("""the and for with your you are our from into what when how more learn click view read see free""".split())
    trends = [(k, v) for k, v in bag.items() if k not in stop]
    return sorted(trends, key=lambda kv: kv[1], reverse=True)[:40]

def write_report():
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    kw_catalog = top_keywords_from_catalog()
    kw_pages = signals_from_vendor_pages()

    md = [f"---\nlayout: page\ntitle: Тренды недели\npermalink: /trends/\n---\n"]
    md.append(f"_Автообновление: {today} (UTC)_\n")
    md.append("## Что чаще всего всплывает в источниках за 7 дней\n")

    if kw_catalog:
        md.append("**Сигналы по материалам из лент:**")
        md.extend([f"- {w} — {c}" for w, c in kw_catalog[:25]])

    if kw_pages:
        md.append("\n**Сигналы по страницам трендов (Getty/Adobe/Shutterstock/Pond5):**")
        md.extend([f"- {w} — {c}" for w, c in kw_pages[:25]])

    md.append("\n> Подсказка: используйте эти слова как **темы съёмок** и **теги** при загрузке на стоки.\n")
    (OUT_DIR / "index.md").write_text("\n".join(md), encoding="utf-8")

if __name__ == "__main__":
    write_report()
