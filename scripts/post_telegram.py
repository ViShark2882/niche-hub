import os, sys, re, requests

TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
SITE_URL = os.environ.get("SITE_URL", "").rstrip("/")

def parse_title(path):
    title = None
    with open(path, "r", encoding="utf-8") as f:
        txt = f.read()
    m = re.search(r'title:\s*"([^"]+)"', txt)
    if m:
        title = m.group(1)
    return title or "Новая публикация"

def path_to_url(path):
    import os
    base = os.path.basename(path)
    name = base[:-3]
    parts = name.split("-", 3)
    if len(parts) < 4:
        return SITE_URL
    yyyy, mm, dd, slug = parts[0], parts[1], parts[2], parts[3]
    return f"{SITE_URL}/{yyyy}/{mm}/{dd}/{slug}.html"

def send(msg):
    if not TOKEN or not CHAT_ID:
        print("No TELEGRAM_TOKEN/CHAT_ID provided; skip")
        return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": CHAT_ID, "text": msg, "disable_web_page_preview": True})
    print("Telegram status:", r.status_code, r.text[:200])

def main():
    if len(sys.argv) < 2:
        print("No files provided")
        return
    new_posts = [p for p in sys.argv[1:] if p.endswith(".md")]
    if not new_posts:
        print("No new posts")
        return
    lines = ["🆕 Новые материалы:"]
    for p in new_posts[:10]:
        title = parse_title(p)
        url = path_to_url(p.replace("docs/",""))
        lines.append(f"• {title}\n{url}")
    send("\n\n".join(lines))

if __name__ == "__main__":
    main()
