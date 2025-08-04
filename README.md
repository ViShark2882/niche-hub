# AI Income Starter (без бюджета)

Готовый репозиторий для автогенерации сайта на GitHub Pages.
- `docs/` — сайт на Jekyll.
- `data/` — источники и таблицы.
- `scripts/` — сбор, генерация постов, анонсы в Telegram.
- `.github/workflows/` — автоматизации.

## Быстрый старт
1. Загрузите всё содержимое в пустой публичный репозиторий.
2. Settings → Pages → Deploy from a branch → `main` + `/docs` → Save.
3. Settings → Actions → General → Allow all actions.
4. Actions → `collect` → Run workflow. Затем `write` → Run workflow.
5. Сайт откроется по адресу из Pages.

Источники для сбора находятся в `data/feeds.txt` — замените примеры на свои RSS.
