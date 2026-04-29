# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Запуск

```bash
python main.py
```

Перед запуском заполнить в `config.py`: `API_ID`, `API_HASH`, `BOT_TOKEN`, `INITIAL_ADMIN_IDS`.

Зависимости (установить через pip): `aiogram`, `telethon`, `aiosqlite`, `python-socks`, `opentele`.

## Архитектура

Telegram-бот (aiogram 3.x) управляет фермой юзерботов (Telethon). Единый процесс, все операции асинхронные.

**Слои:**

```
aiogram handlers (main.py)
    ↓
Store (in-memory состояние) + db.py (SQLite, manager.db)
    ↓
Telethon userbot clients (sessions/*.session)
    ↓
global_proxy.py (SOCKS5 пул)
```

**Ключевые модули:**

| Файл | Назначение |
|------|-----------|
| `main.py` | Все aiogram-хендлеры (~3600 строк), разбит секциями `── СЕКЦИЯ ──` |
| `config.py` | Все константы и токены |
| `db.py` | Весь SQLite CRUD (aiosqlite), схема создаётся в `init_db()` |
| `store.py` | `Store` — in-memory глобальное состояние, сбрасывается при рестарте |
| `task_queue.py` | `TaskQueue` — Semaphore(2) для «тяжёлых» задач (регистрации, массовый залив) |
| `global_proxy.py` | Пул SOCKS5-прокси: парсинг, health-check, выдача аккаунту |
| `ldv_functions.py` | Регистрация и лайкинг в @leomatchbot |
| `xo_functions.py` | Регистрация и лайкинг в @xo_xo |
| `reg_resume.py` | Возобновляемая регистрация (продолжает с сохранённого шага) |
| `autoreply.py` | `AutoreplyManager` — автоответы юзерботов на входящие |
| `autoreply_rules.py` | Паттерны и тексты автоответов |
| `account_setup.py` | Первичная настройка приватности и username нового аккаунта |
| `tdata_import.py` | Импорт tdata и .session файлов (ZIP/папка) |
| `progress.py` | Прогресс-бар в сообщениях бота |

## Схема БД (manager.db)

Таблицы: `accounts`, `ldv_tasks`, `xo_tasks`, `whitelist`, `admins`, `user_proxies`, `global_proxies`, `autoreply_settings`, `user_settings`, `reg_state`.

`accounts.phone` — первичный ключ везде. Каскадное удаление через `db_delete_account()`.

## Паттерны состояния

**`store.active_action[uid]`** — мьютекс пользовательского диалога. Всегда сбрасывать в `finally`:
```python
store.set_action(uid, "action_name")
try:
    ...
finally:
    store.set_action(uid, None)
```

**`ask_with_cancel()`** — диалоговый ввод с кнопкой «❌ Отмена». Возвращает `None` при отмене/таймауте. Требует `attach_pending_router()` в диспетчере.

**`TaskQueue.submit(coro_factory, owner_id, notify, title)`** — для любых задач дольше 5 сек. FIFO, уведомляет пользователя о позиции.

## Доступ

`is_allowed(uid)` — True если uid в `admins` ИЛИ `whitelist`. Все хендлеры начинаются с этой проверки.

Прокси-строка: `host:port:user:pass` (user/pass опциональны), или `нет`/`no`/`-`.

## Известные ограничения

- **tdata импорт не работает** с Telegram Desktop 4.x+: opentele 1.15 не поддерживает новый формат. Ошибка: `TDataReadMapDataFailed`. Альтернатива: экспортировать .session через pyrogram или старый TDesktop.
- `.session импорт работает** (ZIP и локальная папка).
