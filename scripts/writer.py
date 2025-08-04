import csv, os, re, requests, sys
from datetime import datetime, timezone
from bs4 import BeautifulSoup

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
DOCS_DIR = os.path.join(os.path.dirname(__file__), "..", "docs")
POSTS_DIR = os.path.join(DOCS_DIR, "_posts")
os.makedirs(POSTS_DIR, exist_ok=True)

CATALOG = os.path.join(DATA_DIR, "catalog.csv")
PUBLISHED = os.path.join(DATA_DIR, "published.csv")

USE_OLLAMA = os.environ.get("USE_OLLAMA") == "1"

def slugify(txt):
    txt = txt.lower()
    txt = re.sub(r"[^a-z0-9а-яё\-\s]", "", txt)
    txt = re.sub(r"\s+", "-", txt).strip("-")
    return txt[:80] or "post"

def load_catalog():
    with open(CATALOG, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)

def load_published():
    published = set()
    if os.path.exists(PUBLISHED):
        with open(PUBLISHED, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                published.add(r["uid"])
    return published

def append_published(uid, path):
    header = not os.path.exists(PUBLISHED)
    with open(PUBLISHED, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["uid","post_path"])
        if header:
            writer.writeheader()
        writer.writerow({"uid": uid, "post_path": path})

def fetch_description(url, fallback):
    try:
        html = requests.get(url, timeout=12, headers={"User-Agent":"Mozilla/5.0"}).text
        soup = BeautifulSoup(html, "html.parser")
        og = soup.find("meta", attrs={"property":"og:description"})
        if og and og.get("content"):
            return og["content"]
        desc = soup.find("meta", attrs={"name":"description"})
        if desc and desc.get("content"):
            return desc["content"]
    except Exception:
        pass
    return fallback or ""

def ai_summary(title, facts):
    if not USE_OLLAMA:
        return "Кратко: " + (facts[:200] if facts else "")
    try:
        import json
        prompt = f"Сделай краткую выжимку (3–5 предложений) и 5 буллетов пользы. Тема: {title}. Факты: {facts}"
        data = {"model":"llama3", "prompt": prompt}
        r = requests.post("http://localhost:11434/api/generate", json=data, timeout=60)
        if r.ok:
            j = r.json()
            return j.get("response","").strip() or ("Кратко: " + (facts[:200] if facts else ""))
    except Exception:
        pass
    return "Кратко: " + (facts[:200] if facts else "")

def write_post(row):
    title = row["title"]
    link = row["link"]
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    slug = slugify(title)
    filename = os.path.join(POSTS_DIR, f"{date}-{slug}.md")
    desc = fetch_description(link, row.get("summary",""))
    body = ai_summary(title, desc)
    # Экранируем кавычки в заголовке
    safe_title = title.replace('"','\"')
    fm = f"---\nlayout: post\ntitle: \"{safe_title}\"\ndate: {date}\ntags: [дайджест]\n---\n"
    content = fm + "\n" + f"**Источник:** [ссылка]({link})\n\n" + body + "\n\n### Где посмотреть\n- Оригинал: [перейти]({link})\n"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)
    return filename

def main(limit=5):
    rows = load_catalog()
    published = load_published()
    created = []
    for r in rows:
        if r["uid"] in published:
            continue
        path = write_post(r)
        rel = os.path.relpath(path, start=DOCS_DIR)
        append_published(r["uid"], rel)
        created.append(path)
        if len(created) >= limit:
            break
    print("Created posts:\n" + "\n".join(created))

if __name__ == "__main__":
    main()
