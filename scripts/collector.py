import csv, hashlib, os, time
from datetime import datetime, timezone
from urllib.parse import urlparse
import feedparser

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
FEEDS_FILE = os.path.join(DATA_DIR, "feeds.txt")
CATALOG = os.path.join(DATA_DIR, "catalog.csv")

def load_feeds():
    urls = []
    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            # убираем пробелы по краям
            line = line.strip()
            if not line:
                continue
            # режем инлайн-комментарий и все «хвосты» после первого URL
            line = line.split("#", 1)[0].strip()
            parts = line.split()
            if not parts:
                continue
            url = parts[0].strip()
            if url.lower().startswith("http"):
                urls.append(url)
    return urls

def read_catalog():
    if not os.path.exists(CATALOG):
        return {}
    rows = {}
    with open(CATALOG, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows[r["uid"]] = r
    return rows

def save_catalog(rows_dict):
    fieldnames = ["uid", "title", "link", "source", "published", "summary"]
    rows = list(rows_dict.values())
    rows.sort(key=lambda r: r.get("published",""), reverse=True)
    with open(CATALOG, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k,"") for k in fieldnames})

def make_uid(title, link):
    return hashlib.sha1((title + "|" + link).encode("utf-8")).hexdigest()

def main():
    feeds = load_feeds()
    catalog = read_catalog()
    added = 0
    for url in feeds:
        d = feedparser.parse(url)
        for e in d.entries[:10]:
            title = e.get("title", "").strip()
            link = e.get("link", "").strip()
            if not title or not link:
                continue
            uid = make_uid(title, link)
            if uid in catalog:
                continue
            published = ""
            if "published_parsed" in e and e.published_parsed:
                ts = int(time.mktime(e.published_parsed))
                published = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            summary = e.get("summary", "").strip()
            source = urlparse(url).netloc
            catalog[uid] = {
                "uid": uid,
                "title": title.replace("\n"," ").strip(),
                "link": link,
                "source": source,
                "published": published,
                "summary": summary.replace("\n"," ").strip()
            }
            added += 1
    save_catalog(catalog)
    print(f"Added {added} new items. Total: {len(catalog)}")

if __name__ == "__main__":
    main()
