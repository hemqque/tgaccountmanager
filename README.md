# Telegram Bot Manager (ферма аккаунтов) (жертва вайбкодинга)

Бот-менеджер на **aiogram 3.x** + множество юзерботов на **Telethon**,
SQLite через **aiosqlite**, SOCKS5-прокси через **python-socks**.

## Структура

```
config.py             # API_ID/API_HASH/BOT_TOKEN, тайминги, INITIAL_ADMIN_IDS
db.py                 # CRUD для accounts/ldv_tasks/xo_tasks/whitelist/admins/
                      # user_proxies/autoreply_settings/user_settings/reg_state
global_proxy.py       # пул прокси, проверка живости, назначение аккаунтам
account_setup.py      # SetPrivacy + генерация username из 100 анг. слов
autoreply_rules.py    # 13 блоков паттернов + DEFAULT_REPLY_TEXT
autoreply.py          # AutoreplyManager (start/stop/_handle_message)
reg_resume.py         # возобновляемая регистрация LDV/XO
xo_functions.py       # register_one_xo, _xo_click_button, лайкер @xo_xo
ldv_functions.py      # register_one_ldv, лайкер @leomatchbot, ldv_scheduler
store.py              # глобальный in-memory Store
task_queue.py         # очередь «тяжёлых» задач (Semaphore 2)
progress.py           # прогресс-бар (⬛🔷✅❌, pin/edit/cleanup)
utils.py              # rand_sleep, ask_with_cancel, validate_*, is_allowed
main.py               # хендлеры aiogram, меню, секции, bootstrap
requirements.txt
```

## Запуск

### 1. Установка зависимостей

Рекомендуется виртуальное окружение:

```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

pip install -r requirements.txt
```

Версии:
- Python **3.10+** (используется новый синтаксис asyncio).
- aiogram **3.4+**, Telethon **1.34+**.

### 2. Конфигурация

Откройте `config.py` и заполните:

```python
API_ID = 123456                  # с https://my.telegram.org
API_HASH = "ваш_api_hash"
BOT_TOKEN = "0000000000:AAA…"    # от @BotFather
INITIAL_ADMIN_IDS = [123456789]  # ваш Telegram user_id
```

(Опционально) измените `LDV_LISTEN_LO/HI`, `XO_PREMIUM_PHRASE`,
`MAX_CONCURRENT_TASKS`, `AUTO_JOIN_CHANNELS` и т. д.

### 3. Создайте каталоги

Они создадутся автоматически при первом запуске, но можно заранее:

```bash
mkdir sessions   # сюда складываются <phone>.session от Telethon
mkdir temp       # сюда временно качаются фото от пользователя
```

### 4. Запуск

```bash
python main.py
```

Логи идут в stdout (`level=INFO`). Бот начинает поллинг.

### 5. Использование

1. Откройте чат с вашим ботом-менеджером.
2. `/start` — появится главное меню.
3. **⚙️ Аккаунты → ➕ Добавить аккаунт** — вводите номера, прокси, SMS-коды.
4. **🤖 Автоматизация** — массовый залив профилей, регистрации в @leomatchbot
   и @xo_xo, подписка на каналы, автоответы.
5. **📊 Управление** — ручной пролайк, пауза/удаление LDV-циклов,
   управление XO-задачами.
6. **📈 Прогресс** — статистика и переключатель логов.
7. **👑 Админ** (только для админов) — whitelist, админы, все аккаунты.

## Особенности

* **Возобновление регистрации:** если процесс упал во время регистрации в
  @leomatchbot или @xo_xo — после рестарта бот посмотрит на последние
  сообщения от бота и продолжит с правильного шага.
* **Фоновые планировщики:**
  * `ldv_scheduler` — каждые 10с поднимает pending LDV-циклы;
  * `xo_liking_scheduler` — поднимает XO-лайкинги;
  * `run_health_check_loop` — раз в 10 минут проверяет все прокси.
* **Очередь задач:** не более `MAX_CONCURRENT_TASKS=2` одновременных
  «тяжёлых» операций; пользователь видит позицию в очереди.
* **Доступ:** любой не-админ и не-whitelist получает «⛔ нет доступа».

## Безопасность

* Не коммитьте `manager.db`, `sessions/` и `temp/` в публичные репозитории.
* Файлы `.session` — это полный контроль над аккаунтом, относитесь к ним
  как к паролю.

## Решение проблем

* **«FloodWaitError»** — Telegram попросил подождать; обычно лайкинг
  автоматически снизит активность и перезайдёт через указанный интервал.
* **«Прокси dead»** — `🔑 Мои прокси → 🔍 Проверить все`. После починки
  можно снова `📡 Назначить на аккаунты`.
* **«Не подключился»** — проверьте, что прокси жив и сессия не отозвана
  (`📨 Получить код` поможет увидеть свежие сообщения от 777000).
