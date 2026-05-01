# -*- coding: utf-8 -*-
"""
main.py — Бот-менеджер фермы Telegram-аккаунтов (aiogram 3.x).

Файл разбит на логические секции — внутри файла ищите заголовки вида
"── СЕКЦИЯ ──". Большие блоки приведены в порядке: служебное → главное
меню → разделы (Аккаунты / Автоматизация / Управление / Прогресс) →
Админ-панель → bootstrap & main().
"""

# =================================================================
# ── СЕКЦИЯ: ИМПОРТЫ ──
# =================================================================
import asyncio
import glob as _glob
import logging
import os
import random
import re
import secrets
import shutil
import time
from typing import Any, Dict, List, Optional, Tuple

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    BufferedInputFile, FSInputFile,
)
from aiogram.exceptions import TelegramBadRequest

from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    FloodWaitError,
)
from telethon.tl.functions.account import UpdateProfileRequest, UpdateUsernameRequest
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.photos import (
    UploadProfilePhotoRequest, DeletePhotosRequest, GetUserPhotosRequest,
)
from telethon.tl.types import (
    InputPhoto,
    InputPrivacyValueAllowAll, InputPrivacyValueDisallowAll,
    InputPrivacyKeyStatusTimestamp, InputPrivacyKeyProfilePhoto,
    InputPrivacyKeyForwards, InputPrivacyKeyPhoneCall,
    InputPrivacyKeyVoiceMessages, InputPrivacyKeyPhoneNumber,
    InputPrivacyKeyChatInvite,
)
from telethon.tl.functions.account import SetPrivacyRequest

import config
import db
import utils
from utils import (
    is_allowed, restore_main_menu, ask_with_cancel, ask_with_retry,
    validate_phone, validate_proxy, safe_delete_folder, auto_join_channels,
    rand_sleep, main_menu_keyboard, attach_pending_router, has_pending,
    register_pending_text, cancel_pending_ask,
)
from store import Store
from task_queue import TaskQueue
from autoreply import AutoreplyManager
from autoreply_rules import DEFAULT_REPLY_TEXT
from progress import _start_progress, _update_progress, _finish_progress
from account_setup import setup_account
import global_proxy
from global_proxy import (
    parse_proxy_string, proxy_to_telethon, get_proxy_for_account,
    check_proxy_connection, run_health_check_loop, reassign_phones,
    count_alive_socks5,
    get_sticky_global_proxy, mask_proxy, proxy_host,
    apply_global_to_unproxied, set_admin_notifier,
)
from ldv_functions import (
    register_one_ldv, ldv_liking_task, ldv_scheduler, ldv_attach_listener,
)
import client_pool as _client_pool
from client_pool import session_watchdog as _session_watchdog
from xo_functions import (
    register_one_xo, xo_liking_task, xo_liking_scheduler,
)
from reg_resume import register_ldv_resumable, register_xo_resumable


# =================================================================
# ── СЕКЦИЯ: ЛОГИРОВАНИЕ ──
# =================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("main")


# =================================================================
# ── СЕКЦИЯ: ГЛОБАЛЬНЫЕ ОБЪЕКТЫ ──
# =================================================================
bot = Bot(
    token=config.BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML"),
)
dp = Dispatcher()

# Главный объект состояния
store = Store()

# Очередь «тяжёлых» задач (массовый залив, регистрации и т. п.)
task_queue = TaskQueue(max_concurrent=config.MAX_CONCURRENT_TASKS)

# Менеджер автоответов
ar_manager = AutoreplyManager()

# Сессии добавления аккаунта (ввод кода/пароля): uid -> dict
_signin_sessions: Dict[int, Dict[str, Any]] = {}
_batch_cancel: Dict[int, bool] = {}

# Юзернейм бота (заполняется при старте, нужен для генерации ссылок)
_bot_username: str = ""

# Временное хранилище выбранных телефонов для передачи: uid -> List[str]
_transfer_pending: Dict[int, List[str]] = {}

# Множество выбранных телефонов в интерактивном списке: uid -> set[str]
_trf_selection: Dict[int, set] = {}
_TRF_SEL_PER_PAGE = 6

# Универсальный ручной выбор (mass/ldv/xo/subdv/rtag)
_man_sel_ctx: Dict[int, str] = {}   # uid → prefix ("mass_t", "ldvr_t", …)
_man_selection: Dict[int, set] = {} # uid → set of selected phones
_MAN_SEL_PER_PAGE = 6

# 100 слов для генерации рандомных username
_USERNAME_WORDS: List[str] = [
    "blue", "red", "sun", "moon", "star", "fox", "wolf", "bear", "lake",
    "rock", "fire", "ice", "sky", "wind", "rain", "snow", "gold", "silver",
    "iron", "oak", "pine", "rose", "lily", "hawk", "eagle", "tiger", "lion",
    "shark", "whale", "deer", "cat", "dog", "bird", "fish", "frog", "crow",
    "dark", "light", "fast", "cool", "wild", "brave", "free", "true", "pure",
    "swift", "keen", "sharp", "bold", "calm", "cold", "warm", "soft", "hard",
    "tall", "deep", "wide", "bright", "strong", "quick", "silent", "clever",
    "river", "ocean", "storm", "cloud", "wave", "peak", "cliff", "grove",
    "field", "hill", "vale", "crest", "ridge", "shore", "bay", "cape",
    "black", "white", "grey", "amber", "jade", "ruby", "opal", "onyx",
    "north", "south", "east", "west", "dawn", "dusk", "night", "day",
    "volt", "nova", "apex", "core", "echo", "flux",
]


def _gen_username() -> str:
    """Генерирует username: 3 случайных слова + число 1–100."""
    return "".join(random.sample(_USERNAME_WORDS, 3)) + str(random.randint(1, 100))


# =================================================================
# ── СЕКЦИЯ: HELPER-ФУНКЦИИ ──
# =================================================================
async def notify_owner(owner_id: int, text: str) -> None:
    """Послать сообщение пользователю-владельцу. Без падений."""
    try:
        await bot.send_message(owner_id, text)
    except Exception as e:
        log.warning("notify_owner(%s): %s", owner_id, e)


async def user_log(uid: int, text: str) -> None:
    """Если у пользователя включены логи — отправить «📋 <text>»."""
    try:
        s = await db.db_user_settings_get(uid)
        if s.get("logs_enabled"):
            await bot.send_message(uid, f"📋 {text}")
    except Exception:
        pass


async def get_or_create_account_client(phone: str,
                                       owner_id: Optional[int] = None
                                       ) -> Optional[TelegramClient]:
    """
    Возвращает уже подключённый Telethon-клиент для аккаунта `phone`.
    Использует глобальный client_pool — один клиент на телефон.
    """
    proxy = await get_proxy_for_account(
        phone, owner_id if owner_id is not None else 0
    )
    tproxy = proxy_to_telethon(proxy or "")
    return await _client_pool.get_or_connect(
        phone, config.API_ID, config.API_HASH, config.SESSIONS_DIR,
        proxy=tproxy,
    )


def kb(*rows) -> InlineKeyboardMarkup:
    """Шорткат для построения inline-клавиатуры из туплов (text, callback)."""
    out = []
    for row in rows:
        out.append([
            InlineKeyboardButton(text=t, callback_data=c) for (t, c) in row
        ])
    return InlineKeyboardMarkup(inline_keyboard=out)


def home_btn() -> Tuple[str, str]:
    return ("🏠 Главное меню", "action_cancel")


# =================================================================
# ── СЕКЦИЯ: МИДЛВАРЬ ДОСТУПА ──
# =================================================================
@dp.update.outer_middleware()
async def access_middleware(handler, event, data):
    """
    Любой апдейт от неизвестного user_id → отказ.
    Только admins и whitelist допускаются.

    Служебные сообщения от ботов (уведомления о закреплении сообщений и т. п.)
    пропускаются без проверки доступа — у бота нет записи в таблице admins/whitelist,
    и без этой проверки middleware отправляло бы «⛔ нет доступа» в приватный чат.
    """
    uid = None
    msg_or_cb = None
    if event.message:
        # Пропускаем служебные обновления от ботов (в т. ч. от нас самих):
        # pin_chat_message порождает service-message с from_user = бот,
        # который не состоит в whitelist/admins.
        if event.message.from_user and event.message.from_user.is_bot:
            return await handler(event, data)
        uid = event.message.from_user.id if event.message.from_user else None
        msg_or_cb = event.message
    elif event.callback_query:
        uid = event.callback_query.from_user.id
        msg_or_cb = event.callback_query
    elif event.inline_query:
        uid = event.inline_query.from_user.id
    if uid is None:
        return await handler(event, data)

    try:
        allowed = await is_allowed(uid)
    except Exception as e:
        log.error("access_middleware: is_allowed(%s) raised %s — пропускаем", uid, e)
        return await handler(event, data)

    if allowed:
        return await handler(event, data)

    # отказ
    update_type = (
        "message" if event.message else
        "callback_query" if event.callback_query else
        "inline_query" if event.inline_query else "other"
    )
    log.warning("access_middleware: denied uid=%s type=%s chat=%s",
                uid, update_type,
                getattr(msg_or_cb, "chat", {}) if msg_or_cb else None)
    try:
        if isinstance(msg_or_cb, Message):
            await msg_or_cb.answer("⛔ У вас нет доступа к этому боту.")
        elif isinstance(msg_or_cb, CallbackQuery):
            await msg_or_cb.answer("⛔ Нет доступа.", show_alert=True)
    except Exception:
        pass
    return  # не передаём дальше


# =================================================================
# ── СЕКЦИЯ: ROUTER «ПЕРЕХВАТ ОТВЕТОВ ask_with_cancel» ──
# =================================================================
# должен идти ПЕРВЫМ, чтобы отлавливать текст ДО других хендлеров.
pending_router = Router(name="pending")
attach_pending_router(pending_router, store)
dp.include_router(pending_router)


# =================================================================
# ── СЕКЦИЯ: GUARD CALLBACK при active_action (через middleware) ──
# =================================================================
@dp.callback_query.outer_middleware()
async def _callback_guard_mw(handler, event: CallbackQuery, data):
    """
    Middleware-страж: любая кнопка имеет высший приоритет.
    Если у пользователя висит pending-ввод (ask_with_retry/cancel) —
    он отменяется, action сбрасывается, и кнопка выполняется.
    """
    if not event.data:
        return await handler(event, data)
    uid = event.from_user.id if event.from_user else 0
    if uid:
        cancel_pending_ask(uid)
        store.set_action(uid, None)
    return await handler(event, data)


# =================================================================
# ── СЕКЦИЯ: /start, главное меню ──
# =================================================================
@dp.message(CommandStart())
async def handle_start(msg: Message):
    uid = msg.from_user.id

    # ── Deep-link: передача аккаунтов ──
    parts = (msg.text or "").split(maxsplit=1)
    args = parts[1].strip() if len(parts) > 1 else ""
    if args.startswith("tr_"):
        await _handle_transfer_incoming(msg, uid, args[3:])
        return

    is_admin = await db.db_admins_check(uid)
    name = msg.from_user.first_name or "друг"
    text = (
        f"👋 <b>Привет, {name}!</b>\n\n"
        "Добро пожаловать в <b>менеджер аккаунтов</b> — "
        "твой инструмент для управления фермой Telegram.\n\n"
        "📌 Что умеет бот:\n"
        "  • Добавлять и импортировать аккаунты\n"
        "  • Регистрировать в LDV и XO\n"
        "  • Управлять лайкингом и автоответами\n"
        "  • Массово менять имена, фото, био\n\n"
        "Выбери раздел ниже 👇"
    )
    await msg.answer(text, reply_markup=main_menu_keyboard(is_admin))


@dp.message(Command("cancel"))
async def handle_cancel(msg: Message):
    uid = msg.from_user.id
    cancel_pending_ask(uid)
    store.reset_user(uid)
    _grp_index_cache.pop(uid, None)
    _signin_sessions.pop(uid, None)
    _trf_selection.pop(uid, None)
    _transfer_pending.pop(uid, None)
    _man_sel_ctx.pop(uid, None)
    _man_selection.pop(uid, None)
    await restore_main_menu(bot, msg.chat.id, uid, "✅ Действие отменено.")


@dp.message(Command("help"))
async def handle_help(msg: Message):
    await msg.answer(
        "📖 <b>Справка по командам</b>\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        "/start — главное меню\n"
        "/cancel — отменить текущее действие\n"
        "/help — эта справка\n\n"
        "<b>Разделы меню:</b>\n"
        "⚙️ <b>Аккаунты</b> — добавление, импорт, управление\n"
        "🤖 <b>Автоматизация</b> — залив, регистрация LDV/XO, автоответы\n"
        "📊 <b>Управление</b> — лайкинг, задачи, отмена регистраций\n"
        "📈 <b>Прогресс</b> — статистика и логи\n"
        "👑 <b>Админ</b> — whitelist, прокси, все аккаунты"
    )


@dp.message(F.text == "🏠 Главное меню")
async def handle_home(msg: Message):
    uid = msg.from_user.id
    store.reset_user(uid)
    _grp_index_cache.pop(uid, None)
    _signin_sessions.pop(uid, None)
    _trf_selection.pop(uid, None)
    _transfer_pending.pop(uid, None)
    _man_sel_ctx.pop(uid, None)
    _man_selection.pop(uid, None)
    await restore_main_menu(bot, msg.chat.id, uid,
                            "Возврат в главное меню.")


@dp.callback_query(F.data == "action_cancel")
async def cb_action_cancel(cb: CallbackQuery):
    uid = cb.from_user.id
    store.reset_user(uid)
    _tdata_sessions.pop(uid, None)
    _signin_sessions.pop(uid, None)
    _grp_index_cache.pop(uid, None)
    _trf_selection.pop(uid, None)
    _transfer_pending.pop(uid, None)
    _man_sel_ctx.pop(uid, None)
    _man_selection.pop(uid, None)
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await restore_main_menu(bot, cb.message.chat.id, uid,
                            "Возврат в главное меню.")
    await cb.answer()


# =================================================================
# ── СЕКЦИЯ: ПЕРЕКЛЮЧАТЕЛИ ВЕРХНИХ РАЗДЕЛОВ ──
# =================================================================
@dp.message(F.text == "⚙️ Аккаунты")
async def handle_section_accounts(msg: Message):
    await msg.answer(
        "⚙️ <b>Аккаунты</b>\n\n"
        "Добавляйте аккаунты вручную или импортируйте "
        "из TData / .session-файлов.",
        reply_markup=kb(
            [("➕ Добавить аккаунт", "acc_add")],
            [("📦 TData (ZIP)", "acc_tdata"),
             ("📂 TData (папка)", "acc_tdata_local")],
            [("📥 Сессии (ZIP)", "acc_session_zip"),
             ("📥 Сессии (папка)", "acc_session_local")],
            [("📱 Мои аккаунты", "acc_list:0"),
             ("🔑 Мои прокси", "px_list")],
            [("📡 Применить глобальные прокси", "acc_apply_global")],
            [("🔄 Передать аккаунты", "acc_transfer")],
            [home_btn()],
        ),
    )


@dp.callback_query(F.data == "acc_apply_global")
async def cb_acc_apply_global(cb: CallbackQuery):
    """
    Применить глобальные прокси к аккаунтам пользователя, у которых
    accounts.proxy="". Перед применением — health-check всех глобалов
    (Q2 — обязательная проверка). Раздаём с повторением (Q13 C).
    """
    uid = cb.from_user.id
    await cb.answer("Проверяю и применяю…")
    res = await apply_global_to_unproxied(uid, recheck=True)
    await cb.message.answer(
        "📡 <b>Применение глобал-прокси</b>\n"
        f"Живых глобалов после проверки: <b>{res['alive_globals']}</b>\n"
        f"Назначено аккаунтам: <b>{res['updated']}</b>\n"
        f"Без прокси осталось: <b>{res['skipped']}</b>"
    )


@dp.message(F.text == "🤖 Автоматизация")
async def handle_section_auto(msg: Message):
    await msg.answer(
        "🤖 <b>Автоматизация</b>\n\n"
        "Массовые операции над аккаунтами: заливка профилей, "
        "регистрация в приложениях, автоответы.",
        reply_markup=kb(
            [("🚀 Массовый залив", "auto_mass")],
            [("🏷 Смена тега (username)", "auto_rtag")],
            [("🤖 Регистрация LDV", "auto_ldv"),
             ("💘 Регистрация XO", "auto_xo")],
            [("📺 Подписка @leoday", "auto_subdv")],
            [("💬 Автоответы", "auto_ar")],
            [home_btn()],
        ),
    )


@dp.message(F.text == "📊 Управление")
async def handle_section_manage(msg: Message):
    await msg.answer(
        "📊 <b>Управление</b>\n\n"
        "Ручной запуск лайкинга, управление задачами "
        "и отмена регистраций.",
        reply_markup=kb(
            [("❤️ Пролайк LDV", "mng_manual_ldv"),
             ("💘 Пролайк XO", "mng_manual_xo")],
            [("⚙️ Задачи LDV", "mng_ldv"),
             ("💘 Задачи XO", "mng_xo_panel")],
            [("🛑 Отмена регистрации", "mng_regcancel")],
            [home_btn()],
        ),
    )


@dp.message(F.text == "📈 Прогресс")
async def handle_section_progress(msg: Message):
    uid = msg.from_user.id
    s = task_queue.status()
    accs = await db.db_get_accounts_by_owner(uid)
    ldv_tasks = await db.db_get_ldv_tasks_by_owner(uid)
    xo_tasks = await db.db_get_xo_tasks_by_owner(uid)
    user_settings = await db.db_user_settings_get(uid)
    logs_on = bool(user_settings.get("logs_enabled"))

    ldv_run  = sum(1 for t in ldv_tasks if t["status"] == "running")
    ldv_pend = sum(1 for t in ldv_tasks if t["status"] == "pending")
    xo_run   = sum(1 for t in xo_tasks  if t["status"] == "running")
    xo_paus  = sum(1 for t in xo_tasks  if t["status"] == "paused")
    text = (
        "📈 <b>Прогресс и статистика</b>\n\n"
        f"👤  Аккаунтов:   <b>{len(accs)}</b>\n\n"
        f"🤖  LDV-задач:   <b>{len(ldv_tasks)}</b>\n"
        f"    ▸ активных: {ldv_run}  ▸ ожидают: {ldv_pend}\n\n"
        f"💘  XO-задач:    <b>{len(xo_tasks)}</b>\n"
        f"    ▸ активных: {xo_run}  ▸ на паузе: {xo_paus}\n\n"
        f"⚙️  Очередь задач:  "
        f"активно {s['running']} / ожидает {s['waiting']} / макс. {s['max']}\n\n"
        f"📋  Логи уведомлений: "
        f"{'✅ включены' if logs_on else '❌ выключены'}"
    )
    logs_btn_text = "📋 Выключить логи" if logs_on else "📋 Включить логи"
    await msg.answer(
        text,
        reply_markup=kb(
            [(logs_btn_text, "prog_logs_toggle")],
            [home_btn()],
        ),
    )


@dp.callback_query(F.data == "prog_logs_toggle")
async def cb_prog_logs_toggle(cb: CallbackQuery):
    uid = cb.from_user.id
    s = await db.db_user_settings_get(uid)
    new_val = not bool(s.get("logs_enabled"))
    await db.db_user_settings_set_logs(uid, new_val)
    await cb.answer(f"Логи {'включены' if new_val else 'выключены'}.")


@dp.message(F.text == "👑 Админ")
async def handle_section_admin(msg: Message):
    if not await db.db_admins_check(msg.from_user.id):
        return await msg.answer("⛔ Нет доступа.")
    await msg.answer(
        "👑 <b>Администрирование</b>\n\n"
        "Управление доступом пользователей, "
        "глобальными прокси и просмотр всех аккаунтов.",
        reply_markup=kb(
            [("👥 Whitelist", "adm_wl"),
             ("👮 Администраторы", "adm_admins")],
            [("🌐 Глобальные прокси", "gpx_list"),
             ("📋 Все аккаунты", "adm_all_accs")],
            [home_btn()],
        ),
    )


# =================================================================
# ── СЕКЦИЯ: ГЛОБАЛЬНЫЙ СБОРЩИК ФОТО ──
# =================================================================
@dp.message(F.photo)
async def handle_photo(msg: Message):
    """
    Если пользователь сейчас «собирает» фото (store.photo_collecting[uid]),
    то скачиваем фото в temp/<uid>/ и добавляем путь в store.temp_photos[uid].
    Дедупликация по file_unique_id.
    """
    uid = msg.from_user.id
    # Если есть активный ask_with_cancel — пусть отлавливает текст,
    # а фото игнорим (или, если ожидание текста — кидаем "не текст")
    if has_pending(uid):
        return
    if not store.photo_collecting.get(uid):
        return

    # Берём максимальный размер
    photo = msg.photo[-1]
    fuid = photo.file_unique_id
    seen = store.collected_photos.setdefault(uid, set())
    if fuid in seen:
        return
    seen.add(fuid)

    folder = os.path.join(config.TEMP_DIR, f"u_{uid}")
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{int(time.time()*1000)}_{fuid}.jpg")
    try:
        f = await bot.get_file(photo.file_id)
        await bot.download_file(f.file_path, destination=path)
        store.add_temp_photo(uid, path)
    except Exception as e:
        log.warning("download photo: %s", e)
        return

    n = len(store.get_temp_photos(uid))
    try:
        await msg.answer(f"📷 Фото {n} принято.")
    except Exception:
        pass


# =================================================================
# ── СЕКЦИЯ: ⚙️ АККАУНТЫ — ДОБАВЛЕНИЕ ──
# =================================================================
@dp.callback_query(F.data == "acc_add")
async def cb_acc_add(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Уже идёт другое действие.", show_alert=True)
    store.set_action(uid, "acc_add")
    await cb.answer()

    def _has_phones(text: str) -> bool:
        return any(validate_phone(t.strip())
                   for t in re.split(r"[,\n;]+", text) if t.strip())

    try:
        raw = await ask_with_retry(
            bot, cb.message.chat.id, uid,
            "📱 Пришлите номера телефонов (через запятую или с новой строки).\n"
            "Пример:\n+79991112233\n+79994445566",
            validator=_has_phones,
            error_msg="❌ Не нашёл валидных номеров. Формат: +79991112233",
        )
        if raw is None:
            await restore_main_menu(bot, cb.message.chat.id, uid, "Отменено.")
            return
        phones = []
        for tok in re.split(r"[,\n;]+", raw):
            tok = tok.strip()
            if tok:
                p = validate_phone(tok)
                if p:
                    phones.append(p)
        phones = list(dict.fromkeys(phones))

        _batch_cancel[uid] = False
        cancel_msg = await bot.send_message(
            cb.message.chat.id,
            f"📋 <b>Добавление аккаунтов</b>\n\nВ очереди: <b>{len(phones)}</b> номеров",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🛑 Прервать добавление",
                                     callback_data="acc_add_cancel")
            ]]),
        )

        async def _run_add():
            await _start_progress(bot, cb.message.chat.id, uid,
                                  total=len(phones), store=store,
                                  title="➕ Добавление аккаунтов")
            ok_count = 0
            for ph in phones:
                if _batch_cancel.get(uid):
                    break
                await _update_progress(bot, uid, store, current=ph)
                try:
                    res = await _add_one_account(uid, cb.message.chat.id, ph)
                    if res:
                        ok_count += 1
                    await _update_progress(bot, uid, store, done_inc=1,
                                           current=None,
                                           error=None if res
                                           else f"{ph}: не добавлен")
                except Exception as e:
                    await _update_progress(bot, uid, store, done_inc=1,
                                           current=None,
                                           error=f"{ph}: {e}")
            was_cancelled = bool(_batch_cancel.get(uid))
            _batch_cancel.pop(uid, None)
            try:
                await cancel_msg.delete()
            except Exception:
                pass
            cancelled_note = " (отменено)" if was_cancelled else ""
            await _finish_progress(bot, uid, store,
                                   summary_extra=f"Добавлено: {ok_count}/"
                                                 f"{len(phones)}{cancelled_note}")
            await restore_main_menu(bot, cb.message.chat.id, uid)

        await task_queue.submit(
            _run_add, owner_id=uid, notify=notify_owner,
            title=f"Добавление {len(phones)} аккаунтов",
        )
    finally:
        store.set_action(uid, None)


async def _add_one_account(uid: int, chat_id: int, phone: str) -> bool:
    """Полный сценарий добавления одного аккаунта. True/False."""
    # 1. proxy
    proxy_raw = await ask_with_retry(
        bot, chat_id, uid,
        f"🛡 Прокси для <code>{phone}</code> (host:port:user:pass или «Нет»):",
        validator=validate_proxy,
        error_msg="❌ Некорректный формат. Введите host:port:user:pass или «Нет».",
        parse_mode="HTML",
    )
    if proxy_raw is None:
        await bot.send_message(uid, f"⏭ Пропущено: {phone}")
        return False
    if proxy_raw.strip().lower() in ("нет", "no", "none", "-", "без прокси"):
        g = await get_sticky_global_proxy(phone)
        proxy_str = g["proxy_str"] if g else ""
    else:
        proxy_str = proxy_raw.strip()

    # 2. Telethon connect + send_code
    os.makedirs(config.SESSIONS_DIR, exist_ok=True)
    session_path = os.path.join(config.SESSIONS_DIR, phone)
    tproxy = proxy_to_telethon(proxy_str)
    # Если аккаунт уже есть в пуле — отключаем его, чтобы избежать
    # конфликта блокировок SQLite на одном .session файле.
    await _client_pool.remove(phone)
    client = TelegramClient(session_path, config.API_ID, config.API_HASH,
                            proxy=tproxy)
    try:
        await client.connect()
        if await client.is_user_authorized():
            existing = await db.db_get_account(phone)
            if existing:
                # Уже в БД — только обновляем прокси, группу не трогаем
                if proxy_str and existing.get("proxy") != proxy_str:
                    await db.db_update_account_field(phone, "proxy", proxy_str)
                await bot.send_message(
                    uid,
                    f"ℹ️ <code>{phone}</code> уже есть в базе — пропускаю.",
                    parse_mode="HTML",
                )
                await client.disconnect()
                return True
            # Авторизован, но не в БД — добавляем (setup + выбор группы)
            await bot.send_message(
                uid,
                f"ℹ️ <code>{phone}</code>: уже авторизован, добавляю в базу…",
                parse_mode="HTML",
            )
        else:
            sent = await client.send_code_request(phone)
            signed_in = False
            for code_attempt in range(5):
                code = await ask_with_cancel(
                    bot, chat_id, uid,
                    f"🔢 Введите SMS-код для <code>{phone}</code>:",
                    parse_mode="HTML",
                )
                if not code:
                    await bot.send_message(uid, f"⏭ Отменено: {phone}")
                    await client.disconnect()
                    return False
                try:
                    await client.sign_in(phone=phone, code=code.strip(),
                                         phone_code_hash=sent.phone_code_hash)
                    signed_in = True
                    break
                except SessionPasswordNeededError:
                    for pwd_attempt in range(5):
                        pwd = await ask_with_cancel(
                            bot, chat_id, uid,
                            f"🔐 2FA-пароль для <code>{phone}</code>:",
                            parse_mode="HTML",
                        )
                        if not pwd:
                            await bot.send_message(uid, f"⏭ Отменено: {phone}")
                            await client.disconnect()
                            return False
                        try:
                            await client.sign_in(password=pwd.strip())
                            signed_in = True
                            break
                        except Exception:
                            remaining = 4 - pwd_attempt
                            if remaining <= 0:
                                await bot.send_message(uid, f"❌ {phone}: 2FA — попытки исчерпаны.")
                                await client.disconnect()
                                return False
                            await bot.send_message(uid, f"❌ Неверный 2FA-пароль. Осталось: {remaining}.")
                    break
                except (PhoneCodeInvalidError, PhoneCodeExpiredError):
                    remaining = 4 - code_attempt
                    if remaining <= 0:
                        await bot.send_message(uid, f"❌ {phone}: код — попытки исчерпаны.")
                        await client.disconnect()
                        return False
                    await bot.send_message(uid, f"❌ Неверный код. Осталось: {remaining}.")
            if not signed_in:
                await client.disconnect()
                return False

        # 3. setup
        async def _logf(t: str):
            await user_log(uid, f"{phone}: {t}")
        setup_res = await setup_account(client, uid, _logf)
        username = setup_res.get("username_set") or ""

        # 4. auto_join_channels
        await auto_join_channels(client, _logf)

        # 5. Сохраняем аккаунт СРАЗУ (без группы) — не блокируем task_queue.
        # Группу пользователь выбирает через inline-кнопки асинхронно.
        await db.db_add_account(phone, proxy_str, "", "", username, uid)

        groups = await db.db_get_groups_by_owner(uid)
        rows = []
        for g in groups[:8]:
            rows.append([(f"📁 {g}", f"acc_grpset:{phone}:{g}")])
        rows.append([("➕ Новая группа", f"acc_grpnew:{phone}")])
        rows.append([("❌ Без группы", f"acc_grpset:{phone}:")])
        # Сохраняем только для нужд acc_grpnew (имя нового тега)
        _signin_sessions[uid] = {
            "phone": phone,
            "chat_id": chat_id,
        }
        await bot.send_message(
            chat_id,
            f"✅ <code>{phone}</code> добавлен (@{username or '—'}).\n"
            f"Выберите группу (можно сделать позже через карточку):",
            reply_markup=kb(*rows),
        )
        await client.disconnect()
        return True
    except Exception as e:
        log.warning("add account %s: %s", phone, e)
        try:
            await client.disconnect()
        except Exception:
            pass
        await bot.send_message(uid, f"❌ {phone}: {e}")
        return False


@dp.callback_query(F.data == "acc_add_cancel")
async def cb_acc_add_cancel(cb: CallbackQuery):
    uid = cb.from_user.id
    _batch_cancel[uid] = True
    cancel_pending_ask(uid)
    await cb.answer("⛔ Добавление отменено.", show_alert=True)
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("acc_grpset:"))
async def cb_acc_grpset(cb: CallbackQuery):
    """callback_data: acc_grpset:<phone>:<group>"""
    parts = cb.data.split(":", 2)
    if len(parts) < 3:
        return await cb.answer("Bad data", show_alert=True)
    phone = parts[1]
    grp = parts[2]
    uid = cb.from_user.id
    # Аккаунт уже в БД — просто обновляем группу
    await db.db_update_account_field(phone, "grp", grp)
    _signin_sessions.pop(uid, None)
    await cb.message.edit_text(
        f"✅ <code>{phone}</code> — группа: "
        f"<b>{grp or '— без группы —'}</b>"
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("acc_grpnew:"))
async def cb_acc_grpnew(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    await cb.answer()
    name = await ask_with_cancel(bot, cb.message.chat.id, uid,
                                 "✏️ Название новой группы:")
    if not name:
        return await cb.message.answer("Отменено.")
    name = name.strip()[:32]
    # Аккаунт уже в БД — просто обновляем группу
    await db.db_update_account_field(phone, "grp", name)
    _signin_sessions.pop(uid, None)
    await cb.message.answer(
        f"✅ <code>{phone}</code> — группа: <b>{name}</b>"
    )


# =================================================================
# ── СЕКЦИЯ: ⚙️ АККАУНТЫ — ИМПОРТ TData ──
# =================================================================
# uid -> {"chat_id": int, "group": Optional[str]} — состояние импорта
_tdata_sessions: Dict[int, Dict[str, Any]] = {}


@dp.callback_query(F.data == "acc_tdata")
async def cb_acc_tdata(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Уже идёт другое действие.", show_alert=True)
    store.set_action(uid, "acc_tdata_wait")
    _tdata_sessions[uid] = {"chat_id": cb.message.chat.id, "group": ""}
    await cb.answer()
    await bot.send_message(
        cb.message.chat.id,
        "📦 <b>Импорт TData</b>\n\n"
        "Пришлите <b>ZIP-архив</b> с папкой/папками <code>tdata</code> от "
        "Telegram Desktop. Внутри архива может быть несколько tdata — "
        "каждая будет импортирована как отдельный аккаунт.\n\n"
        "Папка tdata должна содержать файл <code>key_datas</code> — это "
        "стандартная структура от Telegram Desktop.\n\n"
        "После импорта аккаунты появятся в «📱 Мои аккаунты».",
        reply_markup=kb(
            [("❌ Отмена", "action_cancel")],
        ),
    )


@dp.message(F.document)
async def handle_document(msg: Message):
    """
    Принимаем .zip:
      • если active_action='acc_tdata_wait'    → импорт TData
      • если active_action='acc_session_wait'  → импорт .session-файлов
    Иначе игнорируем.
    """
    uid = msg.from_user.id
    mode = store.active_action.get(uid)
    if mode not in ("acc_tdata_wait", "acc_session_wait"):
        return
    sess = _tdata_sessions.get(uid) or {}
    chat_id = sess.get("chat_id") or msg.chat.id

    doc = msg.document
    file_name = (doc.file_name or "archive.zip")
    if not file_name.lower().endswith(".zip"):
        return await msg.answer("❌ Нужен файл с расширением .zip")

    # Скачиваем
    work_dir = os.path.join(
        config.TEMP_DIR,
        f"{'tdata' if mode == 'acc_tdata_wait' else 'session'}"
        f"_{uid}_{int(time.time())}",
    )
    os.makedirs(work_dir, exist_ok=True)
    zip_path = os.path.join(work_dir, file_name)
    try:
        f = await bot.get_file(doc.file_id)
        await bot.download_file(f.file_path, destination=zip_path)
    except Exception as e:
        store.set_action(uid, None)
        _tdata_sessions.pop(uid, None)
        return await msg.answer(f"❌ Не получилось скачать: {e}")

    # Ветка импорта сессий — отдельная функция
    if mode == "acc_session_wait":
        await msg.answer("⏳ Архив получен, ищу .session файлы…")

        async def _session_runner():
            from tdata_import import import_sessions_from_archive
            try:
                results = await import_sessions_from_archive(
                    archive_path=zip_path,
                    work_dir=work_dir,
                    sessions_dir=config.SESSIONS_DIR,
                    api_id=config.API_ID,
                    api_hash=config.API_HASH,
                    proxy=None,
                )
            except Exception as e:
                await bot.send_message(uid, f"❌ Импорт сессий упал: {e}")
                store.set_action(uid, None)
                _tdata_sessions.pop(uid, None)
                await restore_main_menu(bot, chat_id, uid)
                return
            await _write_session_report(
                uid, results, "📥 Импорт сессий завершён"
            )
            try:
                shutil.rmtree(work_dir, ignore_errors=True)
            except Exception:
                pass
            store.set_action(uid, None)
            _tdata_sessions.pop(uid, None)
            await restore_main_menu(bot, chat_id, uid)

        await task_queue.submit(
            _session_runner, owner_id=uid, notify=notify_owner,
            title="Импорт сессий",
        )
        return

    # Дальше — обычный импорт TData
    await msg.answer("⏳ Архив получен, распаковываю и импортирую…")

    # Запускаем в очереди задач
    async def _runner():
        from tdata_import import import_from_archive
        try:
            results = await import_from_archive(
                archive_path=zip_path,
                work_dir=work_dir,
                sessions_dir=config.SESSIONS_DIR,
                proxy=None,
            )
        except Exception as e:
            await bot.send_message(uid, f"❌ Импорт TData упал: {e}")
            store.set_action(uid, None)
            _tdata_sessions.pop(uid, None)
            await restore_main_menu(bot, chat_id, uid)
            return

        ok_count = 0
        err_count = 0
        lines: List[str] = []
        added_phones: List[str] = []
        for tdata_folder, phone, err in results:
            short = os.path.basename(tdata_folder.rstrip(os.sep))
            if phone and not err:
                # сохраняем в БД, без прокси, без группы
                try:
                    existing = await db.db_get_account(phone)
                    if existing:
                        # уже существует — просто перепишем owner_id
                        await db.db_add_account(
                            phone, existing.get("proxy") or "",
                            existing.get("note") or "",
                            existing.get("grp") or "",
                            existing.get("username") or "", uid,
                        )
                        lines.append(f"♻️ {phone} — обновлён (был в БД)")
                    else:
                        await db.db_add_account(
                            phone, "", "tdata-import", "", "", uid,
                        )
                        lines.append(f"✅ {phone} — импортирован")
                    added_phones.append(phone)
                    ok_count += 1
                except Exception as e:
                    err_count += 1
                    lines.append(f"❌ {short}: db error {e}")
            else:
                err_count += 1
                lines.append(f"❌ {short}: {err or 'unknown'}")

        # ответ пользователю
        text = (
            f"📦 <b>Импорт TData завершён</b>\n"
            f"Найдено TData: <b>{len(results)}</b>\n"
            f"✅ Успешно: <b>{ok_count}</b>\n"
            f"❌ Ошибок: <b>{err_count}</b>\n\n"
            + "\n".join(lines[:25])
        )
        if len(lines) > 25:
            text += f"\n…ещё {len(lines) - 25} строк"
        try:
            await bot.send_message(uid, text)
        except Exception:
            pass

        # Чистим work_dir (там распакованный tdata + zip — уже не нужны)
        try:
            shutil.rmtree(work_dir, ignore_errors=True)
        except Exception:
            pass

        # Возврат в меню
        store.set_action(uid, None)
        _tdata_sessions.pop(uid, None)
        await restore_main_menu(bot, chat_id, uid)

    await task_queue.submit(
        _runner, owner_id=uid, notify=notify_owner,
        title="Импорт TData",
    )


# ── Локальный импорт TData по пути на диске (без zip) ──
@dp.callback_query(F.data == "acc_tdata_local")
async def cb_acc_tdata_local(cb: CallbackQuery):
    """
    Импорт TData из локальной папки на диске. Удобно когда:
      • архив больше 20 MB (лимит bot api на скачивание),
      • у тебя уже всё распаковано локально,
      • папка содержит десятки/сотни tdata.
    """
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Уже идёт другое действие.",
                               show_alert=True)
    store.set_action(uid, "acc_tdata_local")
    await cb.answer()
    _confirmed = False
    try:
        path_raw = await ask_with_retry(
            bot, cb.message.chat.id, uid,
            "📂 <b>Импорт TData из локальной папки</b>\n\n"
            "Пришлите <b>абсолютный путь</b> к папке, в которой лежат "
            "папки с tdata (имена могут быть любыми — поиск идёт по файлу "
            "<code>key_datas</code>).\n\n"
            "Пример Windows:\n"
            "<code>C:\\Users\\dayhu\\Downloads\\tdata_pack</code>\n\n"
            "Поддерживаются вложенные структуры — все tdata будут найдены "
            "рекурсивно.",
            validator=lambda p: os.path.isdir(p.strip().strip('"').strip("'")),
            error_msg="❌ Папка не найдена. Укажите корректный абсолютный путь.",
            parse_mode="HTML",
        )
        if not path_raw:
            await restore_main_menu(bot, cb.message.chat.id, uid,
                                    "Отменено.")
            return
        local_path = path_raw.strip().strip('"').strip("'")

        # Превью — сколько и какие папки нашли (Q-improvement)
        from tdata_import import (
            find_tdata_folders, validate_tdata_structure,
            import_from_local_folder,
        )
        found = find_tdata_folders(local_path)
        if not found:
            await bot.send_message(
                uid,
                f"📭 В <code>{local_path}</code> не нашлось ни одной "
                f"tdata (отсутствует файл <code>key_datas</code>)."
            )
            await restore_main_menu(bot, cb.message.chat.id, uid)
            return

        # Прогоним структурную валидацию по каждой найденной папке
        valid: List[str] = []
        broken: List[Tuple[str, str]] = []  # (folder, reason)
        for p in found:
            v_err = validate_tdata_structure(p)
            if v_err:
                broken.append((p, v_err))
            else:
                valid.append(p)

        # Покажем список найденных и попросим подтверждение
        preview_lines = [
            f"🔎 Найдено TData: <b>{len(found)}</b>",
            f"   ✅ Годных: <b>{len(valid)}</b>",
            f"   ❌ Битых: <b>{len(broken)}</b>",
        ]
        if valid:
            preview_lines.append("\n<b>Годные</b> (первые 20):")
            for p in valid[:20]:
                short = os.path.relpath(p, local_path) or os.path.basename(p)
                preview_lines.append(f"  ✅ <code>{short}</code>")
            if len(valid) > 20:
                preview_lines.append(f"  …и ещё {len(valid) - 20}")
        if broken:
            preview_lines.append("\n<b>Битые</b> (первые 15):")
            for p, why in broken[:15]:
                short = os.path.relpath(p, local_path) or os.path.basename(p)
                preview_lines.append(f"  ❌ <code>{short}</code> — {why}")
            if len(broken) > 15:
                preview_lines.append(f"  …и ещё {len(broken) - 15}")
        # Сохраним путь для подтверждения
        _tdata_sessions[uid] = {
            "chat_id": cb.message.chat.id,
            "local_path": local_path,
        }
        _confirmed = True
        await bot.send_message(
            cb.message.chat.id,
            "\n".join(preview_lines)
            + "\n\nИмпортировать всё это?",
            reply_markup=kb(
                [("✅ Импортировать", "acc_tdata_local_run")],
                [("❌ Отмена", "action_cancel")],
            ),
        )
    finally:
        if not _confirmed:
            store.set_action(uid, None)


@dp.callback_query(F.data == "acc_tdata_local_run")
async def cb_acc_tdata_local_run(cb: CallbackQuery):
    uid = cb.from_user.id
    sess = _tdata_sessions.get(uid) or {}
    local_path = sess.get("local_path")
    chat_id = sess.get("chat_id") or cb.message.chat.id
    if not local_path:
        store.set_action(uid, None)
        return await cb.answer("Сессия импорта пропала, начни заново.",
                               show_alert=True)
    await cb.answer("⏳ Запускаю импорт…")

    async def _runner():
        from tdata_import import import_from_local_folder
        try:
            results = await import_from_local_folder(
                root=local_path,
                sessions_dir=config.SESSIONS_DIR,
                proxy=None,
            )
        except Exception as e:
            await bot.send_message(uid, f"❌ Импорт TData упал: {e}")
            store.set_action(uid, None)
            _tdata_sessions.pop(uid, None)
            await restore_main_menu(bot, chat_id, uid)
            return

        ok_count = 0
        err_count = 0
        lines: List[str] = []
        for tdata_folder, phone, err in results:
            short = os.path.basename(tdata_folder.rstrip(os.sep))
            if phone and not err:
                try:
                    existing = await db.db_get_account(phone)
                    if existing:
                        await db.db_add_account(
                            phone, existing.get("proxy") or "",
                            existing.get("note") or "",
                            existing.get("grp") or "",
                            existing.get("username") or "", uid,
                        )
                        lines.append(f"♻️ {phone} — обновлён")
                    else:
                        await db.db_add_account(
                            phone, "", "tdata-import", "", "", uid,
                        )
                        lines.append(f"✅ {phone} — импортирован")
                    ok_count += 1
                except Exception as e:
                    err_count += 1
                    lines.append(f"❌ {short}: db error {e}")
            else:
                err_count += 1
                lines.append(f"❌ {short}: {err or 'unknown'}")

        text = (
            f"📂 <b>Локальный импорт TData завершён</b>\n"
            f"Найдено TData: <b>{len(results)}</b>\n"
            f"✅ Успешно: <b>{ok_count}</b>\n"
            f"❌ Ошибок: <b>{err_count}</b>\n\n"
            + "\n".join(lines[:25])
        )
        if len(lines) > 25:
            text += f"\n…ещё {len(lines) - 25} строк"
        try:
            await bot.send_message(uid, text)
        except Exception:
            pass

        # Локальную папку НЕ удаляем — это пользовательские файлы
        store.set_action(uid, None)
        _tdata_sessions.pop(uid, None)
        await restore_main_menu(bot, chat_id, uid)

    await task_queue.submit(
        _runner, owner_id=uid, notify=notify_owner,
        title="Локальный импорт TData",
    )


# =================================================================
# ── СЕКЦИЯ: ⚙️ АККАУНТЫ — ИМПОРТ .SESSION (Telethon) ──
# =================================================================
# ZIP-вариант — присылаем архив с .session файлами в чат.
@dp.callback_query(F.data == "acc_session_zip")
async def cb_acc_session_zip(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Уже идёт другое действие.", show_alert=True)
    store.set_action(uid, "acc_session_wait")
    _tdata_sessions[uid] = {"chat_id": cb.message.chat.id}
    await cb.answer()
    await bot.send_message(
        cb.message.chat.id,
        "📥 <b>Импорт .session файлов (ZIP)</b>\n\n"
        "Пришлите ZIP-архив с одним или несколькими файлами "
        "<code>*.session</code> от Telethon. Папка/имена внутри — "
        "произвольные.\n\n"
        "Каждый .session — это уже готовая авторизация: ничего не "
        "конвертируется, бот просто проверит её валидность и добавит "
        "аккаунт в БД под номером, который вернёт Telegram.",
        reply_markup=kb([("❌ Отмена", "action_cancel")]),
    )


# Локальный вариант — указать путь к папке.
@dp.callback_query(F.data == "acc_session_local")
async def cb_acc_session_local(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Уже идёт другое действие.",
                               show_alert=True)
    store.set_action(uid, "acc_session_local")
    await cb.answer()
    _confirmed = False
    try:
        path_raw = await ask_with_retry(
            bot, cb.message.chat.id, uid,
            "📥 <b>Импорт .session файлов (локальная папка)</b>\n\n"
            "Пришлите абсолютный путь к папке, где лежат "
            "<code>*.session</code> файлы (рекурсивно). Имена файлов "
            "и подпапок — любые.\n\n"
            "Пример: <code>C:\\Users\\dayhu\\Downloads\\sessions_pack</code>",
            validator=lambda p: os.path.isdir(p.strip().strip('"').strip("'")),
            error_msg="❌ Папка не найдена. Укажите корректный абсолютный путь.",
            parse_mode="HTML",
        )
        if not path_raw:
            await restore_main_menu(bot, cb.message.chat.id, uid,
                                    "Отменено.")
            return
        local_path = path_raw.strip().strip('"').strip("'")

        # Превью
        from tdata_import import find_session_files
        files = find_session_files(local_path)
        if not files:
            await bot.send_message(
                uid,
                f"📭 В <code>{local_path}</code> не нашлось *.session "
                f"файлов."
            )
            await restore_main_menu(bot, cb.message.chat.id, uid)
            return

        preview_lines = [
            f"🔎 Найдено .session: <b>{len(files)}</b>",
            "Список (первые 30):",
        ]
        for p in files[:30]:
            short = os.path.relpath(p, local_path) or os.path.basename(p)
            preview_lines.append(f"  • <code>{short}</code>")
        if len(files) > 30:
            preview_lines.append(f"  …и ещё {len(files) - 30}")

        _tdata_sessions[uid] = {
            "chat_id": cb.message.chat.id,
            "local_path": local_path,
        }
        _confirmed = True
        await bot.send_message(
            cb.message.chat.id,
            "\n".join(preview_lines)
            + "\n\nИмпортировать?",
            reply_markup=kb(
                [("✅ Импортировать", "acc_session_local_run")],
                [("❌ Отмена", "action_cancel")],
            ),
        )
    finally:
        if not _confirmed:
            store.set_action(uid, None)


@dp.callback_query(F.data == "acc_session_local_run")
async def cb_acc_session_local_run(cb: CallbackQuery):
    uid = cb.from_user.id
    sess = _tdata_sessions.get(uid) or {}
    local_path = sess.get("local_path")
    chat_id = sess.get("chat_id") or cb.message.chat.id
    if not local_path:
        store.set_action(uid, None)
        return await cb.answer("Сессия импорта пропала.", show_alert=True)
    await cb.answer("⏳ Запускаю…")

    async def _runner():
        from tdata_import import import_sessions_from_folder
        try:
            results = await import_sessions_from_folder(
                root=local_path,
                sessions_dir=config.SESSIONS_DIR,
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                proxy=None,
            )
        except Exception as e:
            await bot.send_message(uid, f"❌ Импорт упал: {e}")
            store.set_action(uid, None)
            _tdata_sessions.pop(uid, None)
            await restore_main_menu(bot, chat_id, uid)
            return
        await _write_session_report(uid, results, "📥 Локальный импорт сессий")
        store.set_action(uid, None)
        _tdata_sessions.pop(uid, None)
        await restore_main_menu(bot, chat_id, uid)

    await task_queue.submit(
        _runner, owner_id=uid, notify=notify_owner,
        title="Локальный импорт сессий",
    )


async def _write_session_report(uid: int,
                                results: List[Tuple[str, Optional[str], Optional[str]]],
                                title: str) -> None:
    """Общий отчёт-формат для импорта сессий (ZIP/локальный)."""
    ok_count = 0
    err_count = 0
    lines: List[str] = []
    for src, phone, err in results:
        short = os.path.basename(src)
        if phone and not err:
            try:
                existing = await db.db_get_account(phone)
                if existing:
                    await db.db_add_account(
                        phone, existing.get("proxy") or "",
                        existing.get("note") or "",
                        existing.get("grp") or "",
                        existing.get("username") or "", uid,
                    )
                    lines.append(f"♻️ {phone} — обновлён")
                else:
                    await db.db_add_account(
                        phone, "", "session-import", "", "", uid,
                    )
                    lines.append(f"✅ {phone} — импортирован")
                ok_count += 1
            except Exception as e:
                err_count += 1
                lines.append(f"❌ {short}: db error {e}")
        else:
            err_count += 1
            lines.append(f"❌ {short}: {err or 'unknown'}")

    text = (
        f"{title}\n"
        f"Найдено сессий: <b>{len(results)}</b>\n"
        f"✅ Успешно: <b>{ok_count}</b>\n"
        f"❌ Ошибок: <b>{err_count}</b>\n\n"
        + "\n".join(lines[:25])
    )
    if len(lines) > 25:
        text += f"\n…ещё {len(lines) - 25} строк"
    try:
        await bot.send_message(uid, text)
    except Exception:
        pass


# =================================================================
# ── СЕКЦИЯ: ⚙️ АККАУНТЫ — СПИСОК (с пагинацией) ──
# =================================================================
@dp.callback_query(F.data.startswith("acc_list:"))
async def cb_acc_list(cb: CallbackQuery):
    uid = cb.from_user.id
    try:
        page = int(cb.data.split(":", 1)[1])
    except Exception:
        page = 0
    accs = await db.db_get_accounts_by_owner(uid)
    per = config.ACCOUNTS_PER_PAGE
    total = len(accs)
    pages = max(1, (total + per - 1) // per)
    page = max(0, min(page, pages - 1))
    chunk = accs[page * per:(page + 1) * per]

    rows = []
    for a in chunk:
        ph = a["phone"]
        un = a.get("username") or "-"
        nt = (a.get("note") or "")[:24]
        rows.append([
            (f"📱 {ph} (@{un})" + (f" — {nt}" if nt else ""),
             f"acc_card:{ph}")
        ])
    nav = []
    if page > 0:
        nav.append(("◀️", f"acc_list:{page-1}"))
    nav.append((f"{page+1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append(("▶️", f"acc_list:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([("♻️ Сбросить все сессии", "acc_reset_all")])
    rows.append([home_btn()])
    text = (
        f"📱 <b>Мои аккаунты</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Всего: <b>{total}</b>  ·  Стр. {page+1}/{pages}"
    )
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))
    await cb.answer()


@dp.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()


@dp.callback_query(F.data == "acc_reset_all")
async def cb_acc_reset_all(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    confirm = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "⚠️ <b>Удалить ВСЕ аккаунты?</b>\n\n"
        "Это действие безвозвратно удалит:\n"
        "  • все .session-файлы\n"
        "  • все записи в базе данных\n"
        "  • все задачи LDV и XO\n\n"
        "Для подтверждения напишите <b>ДА</b>:",
        validator=lambda t: t.strip().lower() == "да",
        error_msg='❌ Напишите именно "ДА" для подтверждения.',
        parse_mode="HTML",
    )
    if not confirm:
        return await cb.message.answer("✅ Отменено — аккаунты не удалены.")
    accs = await db.db_get_accounts_by_owner(uid)
    n = 0
    for a in accs:
        try:
            await db.db_delete_account(a["phone"])
            base = os.path.join(config.SESSIONS_DIR, a["phone"])
            for _sp in _glob.glob(base + "*.session*"):
                try:
                    os.remove(_sp)
                except Exception:
                    pass
            n += 1
        except Exception:
            pass
    await cb.message.answer(f"♻️ Удалено: {n} аккаунтов.")


# =================================================================
# ── СЕКЦИЯ: ⚙️ АККАУНТЫ — КАРТОЧКА ──
# =================================================================
async def _render_account_card(phone: str, owner_id: int):
    a = await db.db_get_account(phone)
    if not a or a.get("owner_id") != owner_id:
        return None, None
    ar_settings = await db.db_ar_get_settings(owner_id, phone)
    ar_on = bool(ar_settings.get("enabled"))

    # Q11 (C): если у аккаунта пусто в proxy — посмотрим, какой sticky-глобал
    # ему достанется на лету; покажем «(через глобал) host».
    own_proxy = (a.get("proxy") or "").strip()
    if own_proxy:
        proxy_line = own_proxy
    else:
        gp = await get_sticky_global_proxy(phone)
        if gp:
            host = proxy_host(gp["proxy_str"]) or "?"
            proxy_line = f"<i>(через глобал)</i> {host}"
        else:
            proxy_line = "— без прокси —"

    un = a.get("username") or "—"
    text = (
        f"📱 <b>{a['phone']}</b>  (@{un})\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📁  Группа:    <b>{a.get('grp') or '—'}</b>\n"
        f"📝  Заметка:   {a.get('note') or '—'}\n"
        f"🛡️  Прокси:    {proxy_line}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"💬  Автоответ: {'✅ включён' if ar_on else '❌ выключен'}"
    )
    rows = [
        [("✏️ Имя", f"acc_name:{phone}"),
         ("✏️ Био", f"acc_bio:{phone}"),
         ("✏️ Username", f"acc_uname:{phone}")],
        [("📷 Фото", f"acc_photo:{phone}"),
         ("📝 Заметка", f"acc_note:{phone}"),
         ("📁 Группа", f"acc_grp:{phone}")],
        [("🔒 Приватность", f"acc_priv:{phone}"),
         ("📨 Получить код", f"acc_code:{phone}")],
        [("🗑 Удалить аккаунт", f"acc_del:{phone}")],
        [("‹ Назад", "acc_list:0"), home_btn()],
    ]
    return text, kb(*rows)


@dp.callback_query(F.data.startswith("acc_card:"))
async def cb_acc_card(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    text, mk = await _render_account_card(phone, cb.from_user.id)
    if not text:
        return await cb.answer("Аккаунт не найден.", show_alert=True)
    try:
        await cb.message.edit_text(text, reply_markup=mk)
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=mk)
    await cb.answer()


# ─── Заметка ───
@dp.callback_query(F.data.startswith("acc_note:"))
async def cb_acc_note(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    await cb.answer()
    text = await ask_with_cancel(
        bot, cb.message.chat.id, uid,
        f"✏️ Новая заметка для <code>{phone}</code>:"
    )
    if text is None:
        return
    await db.db_update_account_field(phone, "note", text.strip()[:256])
    await cb.message.answer("✅ Заметка обновлена.")


# ─── Группа ───
@dp.callback_query(F.data.startswith("acc_grp:"))
async def cb_acc_grp(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    await cb.answer()
    groups = (await db.db_get_groups_by_owner(uid))[:8]
    rows = [[(f"📁 {g}", f"acc_grpset2:{phone}:{g}")] for g in groups]
    rows.append([("➕ Новая группа", f"acc_grpnew2:{phone}")])
    rows.append([("❌ Без группы", f"acc_grpset2:{phone}:")])
    rows.append([("‹ Назад", f"acc_card:{phone}")])
    await cb.message.answer(
        f"📁 Выберите группу для <code>{phone}</code>:",
        reply_markup=kb(*rows),
    )


@dp.callback_query(F.data.startswith("acc_grpset2:"))
async def cb_acc_grpset2(cb: CallbackQuery):
    parts = cb.data.split(":", 2)
    phone = parts[1]
    grp = parts[2] if len(parts) > 2 else ""
    await db.db_update_account_field(phone, "grp", grp)
    await cb.answer("Группа обновлена.")
    text, mk = await _render_account_card(phone, cb.from_user.id)
    if text:
        try:
            await cb.message.edit_text(text, reply_markup=mk)
        except TelegramBadRequest:
            await cb.message.answer(text, reply_markup=mk)


@dp.callback_query(F.data.startswith("acc_grpnew2:"))
async def cb_acc_grpnew2(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    await cb.answer()
    name = await ask_with_cancel(bot, cb.message.chat.id, uid,
                                 "✏️ Название новой группы:")
    if not name:
        return
    await db.db_update_account_field(phone, "grp", name.strip()[:32])
    text, mk = await _render_account_card(phone, uid)
    if text:
        await cb.message.answer(text, reply_markup=mk)


# ─── Имя ───
@dp.callback_query(F.data.startswith("acc_name:"))
async def cb_acc_name(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    await cb.answer()
    new_name = await ask_with_cancel(
        bot, cb.message.chat.id, uid,
        f"✏️ Новое имя (first_name) для <code>{phone}</code>:"
    )
    if not new_name:
        return
    cli = await get_or_create_account_client(phone, uid)
    if not cli:
        return await cb.message.answer("❌ Не удалось подключиться.")
    try:
        await cli(UpdateProfileRequest(first_name=new_name.strip()[:64]))
        await cb.message.answer("✅ Имя обновлено.")
    except Exception as e:
        await cb.message.answer(f"❌ {e}")


# ─── Био ───
@dp.callback_query(F.data.startswith("acc_bio:"))
async def cb_acc_bio(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    await cb.answer()
    bio = await ask_with_cancel(
        bot, cb.message.chat.id, uid,
        f"✏️ Новое био для <code>{phone}</code> (до 70 симв.):"
    )
    if bio is None:
        return
    cli = await get_or_create_account_client(phone, uid)
    if not cli:
        return await cb.message.answer("❌ Не удалось подключиться.")
    try:
        await cli(UpdateProfileRequest(about=bio.strip()[:70]))
        await cb.message.answer("✅ Био обновлено.")
    except Exception as e:
        await cb.message.answer(f"❌ {e}")


# ─── Фото ───
@dp.callback_query(F.data.startswith("acc_photo:"))
async def cb_acc_photo(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    await cb.answer()
    # включить сборщик и попросить фото
    store.photo_collecting[uid] = True
    store.clear_temp_photos(uid)
    await bot.send_message(
        cb.message.chat.id,
        f"📷 Пришлите ОДНО фото для <code>{phone}</code>.\n"
        f"После — нажмите «Готово».",
        reply_markup=kb([("✅ Готово", f"acc_photodone:{phone}")],
                        [("❌ Отмена", "action_cancel")]),
    )


@dp.callback_query(F.data.startswith("acc_photodone:"))
async def cb_acc_photodone(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    photos = store.get_temp_photos(uid)
    store.photo_collecting[uid] = False
    store.clear_temp_photos(uid)
    await cb.answer()
    if not photos:
        return await cb.message.answer("⚠️ Фото не получено.")
    cli = await get_or_create_account_client(phone, uid)
    if not cli:
        return await cb.message.answer("❌ Не удалось подключиться.")
    try:
        # Удалим текущие фото профиля
        try:
            existing = await cli(GetUserPhotosRequest(
                user_id="me", offset=0, max_id=0, limit=10))
            input_photos = []
            for p in existing.photos:
                input_photos.append(InputPhoto(id=p.id, access_hash=p.access_hash,
                                               file_reference=p.file_reference))
            if input_photos:
                await cli(DeletePhotosRequest(id=input_photos))
        except Exception:
            pass
        await cli(UploadProfilePhotoRequest(
            file=await cli.upload_file(photos[0])
        ))
        await cb.message.answer("✅ Фото профиля обновлено.")
    except Exception as e:
        await cb.message.answer(f"❌ {e}")


# ─── Username ───
@dp.callback_query(F.data.startswith("acc_uname:"))
async def cb_acc_uname(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    await cb.answer()
    new_un = await ask_with_cancel(
        bot, cb.message.chat.id, uid,
        f"✏️ Новый username для <code>{phone}</code> (без @):"
    )
    if not new_un:
        return
    new_un = new_un.strip().lstrip("@")
    cli = await get_or_create_account_client(phone, uid)
    if not cli:
        return await cb.message.answer("❌ Не удалось подключиться.")
    try:
        await cli(UpdateUsernameRequest(username=new_un))
        await db.db_update_account_field(phone, "username", new_un)
        await cb.message.answer(f"✅ Username @{new_un}.")
    except Exception as e:
        await cb.message.answer(f"❌ {e}")


# ─── Приватность ───
@dp.callback_query(F.data.startswith("acc_priv:"))
async def cb_acc_priv(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    await cb.answer()
    await cb.message.answer(
        f"🔒 <b>Приватность</b>  <code>{phone}</code>\n\n"
        "Настройте видимость профиля для других пользователей.\n"
        "Затрагивает: статус, фото, звонки, голосовые, переадресации, "
        "номер телефона, инвайты в чаты.",
        reply_markup=kb(
            [("🔒 Закрыть всё", f"acc_privset:{phone}:close"),
             ("🔓 Открыть всё", f"acc_privset:{phone}:open")],
            [("‹ Назад", f"acc_card:{phone}")],
        ),
    )


@dp.callback_query(F.data.startswith("acc_privset:"))
async def cb_acc_privset(cb: CallbackQuery):
    parts = cb.data.split(":", 2)
    phone, mode = parts[1], parts[2]
    uid = cb.from_user.id
    cli = await get_or_create_account_client(phone, uid)
    if not cli:
        return await cb.answer("❌ Не подключились.", show_alert=True)
    keys = [
        InputPrivacyKeyStatusTimestamp(), InputPrivacyKeyProfilePhoto(),
        InputPrivacyKeyForwards(), InputPrivacyKeyPhoneCall(),
        InputPrivacyKeyVoiceMessages(), InputPrivacyKeyPhoneNumber(),
        InputPrivacyKeyChatInvite(),
    ]
    rule = (InputPrivacyValueDisallowAll() if mode == "close"
            else InputPrivacyValueAllowAll())
    errs = 0
    for k in keys:
        try:
            await cli(SetPrivacyRequest(key=k, rules=[rule]))
        except Exception:
            errs += 1
    await cb.answer(
        ("Закрыто" if mode == "close" else "Открыто")
        + (f" ({errs} ошибок)" if errs else ""),
    )


# ─── Получить код ───
@dp.callback_query(F.data.startswith("acc_code:"))
async def cb_acc_code(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    cli = await get_or_create_account_client(phone, uid)
    if not cli:
        return await cb.answer("❌ Не подключились.", show_alert=True)
    found = None
    try:
        for sender in (777000, 42777):
            try:
                msgs = await cli.get_messages(sender, limit=1)
                if msgs:
                    found = msgs[0]
                    break
            except Exception:
                continue
    except Exception:
        pass
    await cb.answer()
    if not found:
        return await cb.message.answer("📭 Сообщений от Telegram не найдено.")
    text = found.text or found.message or ""
    await cb.message.answer(
        f"📨 <b>Последнее от Telegram:</b>\n<code>{text[:1000]}</code>"
    )


# ─── Удалить аккаунт ───
@dp.callback_query(F.data.startswith("acc_del:"))
async def cb_acc_del(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    await cb.answer()
    await cb.message.answer(
        f"⚠️ <b>Удалить аккаунт?</b>\n\n"
        f"<code>{phone}</code>\n\n"
        "Будут безвозвратно удалены:\n"
        "• .session-файл\n"
        "• Задачи LDV и XO\n"
        "• Настройки автоответа",
        reply_markup=kb(
            [("🗑 Да, удалить", f"acc_del2:{phone}")],
            [("← Отмена", f"acc_card:{phone}")],
        ),
    )


@dp.callback_query(F.data.startswith("acc_del2:"))
async def cb_acc_del2(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    # стопнуть автоответ, если включён
    try:
        await ar_manager.stop(phone)
    except Exception:
        pass
    # стопнуть xo_liking
    t = store.xo_liking_tasks.pop(phone, None)
    if t and not t.done():
        t.cancel()
    # отметить как cancelled для LDV
    store.cancelled_phones.add(phone)
    # удалить клиент из общего пула
    await _client_pool.remove(phone)
    # удалить из БД
    await db.db_delete_account(phone)
    # стереть .session и .session-journal
    base = os.path.join(config.SESSIONS_DIR, phone)
    for _sp in _glob.glob(base + "*.session*"):
        try:
            os.remove(_sp)
        except Exception:
            pass
    await cb.answer("Удалён.")
    await cb.message.edit_text(f"🗑 <code>{phone}</code> удалён.")


# =================================================================
# ── СЕКЦИЯ: 🔑 ПРОКСИ ──
# =================================================================
@dp.callback_query(F.data == "px_list")
async def cb_px_list(cb: CallbackQuery):
    uid = cb.from_user.id
    proxies = await db.db_proxy_get_by_owner(uid)
    alive = sum(1 for p in proxies if p.get("status") == "alive")
    dead  = sum(1 for p in proxies if p.get("status") == "dead")
    if not proxies:
        text = (
            "🔑 <b>Мои прокси</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "Прокси не добавлены."
        )
    else:
        text = (
            f"🔑 <b>Мои прокси</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"Всего: <b>{len(proxies)}</b>  ·  "
            f"✅ {alive}  ·  ❌ {dead}\n"
        )
        for p in proxies[:20]:
            mark = ("✅" if p.get("status") == "alive"
                    else "❌" if p.get("status") == "dead" else "❓")
            note = f" — <i>{p['note']}</i>" if p.get("note") else ""
            text += f"\n{mark} #{p['id']}  <code>{p['proxy_str']}</code>{note}"
        if len(proxies) > 20:
            text += f"\n…ещё {len(proxies) - 20}"

    rows = []
    for p in proxies[:20]:
        mark = ("✅" if p.get("status") == "alive"
                else "❌" if p.get("status") == "dead" else "❓")
        rows.append([(f"{mark} #{p['id']}", f"px_view:{p['id']}")])
    rows.append([("➕ Добавить прокси", "px_add"),
                 ("🔍 Проверить все", "px_checkall")])
    rows.append([("📡 Назначить на аккаунты", "px_reassign")])
    rows.append([("🌐 Глобальные прокси", "gpx_list")])
    rows.append([home_btn()])
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))
    await cb.answer()


@dp.callback_query(F.data == "px_add")
async def cb_px_add(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    def _has_valid_proxy(text: str) -> bool:
        return any(parse_proxy_string(l.strip())
                   for l in text.splitlines() if l.strip())

    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "🔑 Пришлите прокси (host:port:user:pass).\n"
        "Можно несколько строк.",
        validator=_has_valid_proxy,
        error_msg="❌ Не нашёл валидных прокси. Формат: host:port:user:pass",
    )
    if raw is None:
        return
    added = 0
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        if not parse_proxy_string(line):
            continue
        await db.db_proxy_add(uid, line, "")
        added += 1
    await cb.message.answer(f"✅ Добавлено: {added}")


@dp.callback_query(F.data == "px_checkall")
async def cb_px_checkall(cb: CallbackQuery):
    uid = cb.from_user.id
    proxies = await db.db_proxy_get_by_owner(uid)
    await cb.answer(f"⏳ Проверяю {len(proxies)} прокси…")
    _sem = asyncio.Semaphore(10)

    async def _check(p):
        async with _sem:
            ok = await check_proxy_connection(p["proxy_str"])
            await db.db_proxy_update_status(p["id"], "alive" if ok else "dead")
            return ok

    results = await asyncio.gather(*[_check(p) for p in proxies])
    alive = sum(results)
    await cb.message.answer(
        f"🔍 <b>Проверка завершена</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Проверено: <b>{len(proxies)}</b>\n"
        f"✅ Живых: <b>{alive}</b>\n"
        f"❌ Мёртвых: <b>{len(proxies) - alive}</b>"
    )


@dp.callback_query(F.data == "px_reassign")
async def cb_px_reassign(cb: CallbackQuery):
    uid = cb.from_user.id
    res = await reassign_phones(uid)
    await cb.answer()
    await cb.message.answer(
        f"📡 <b>Назначение прокси завершено</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Обновлено аккаунтов: <b>{res['updated']}</b>\n"
        f"⚠️ Без прокси осталось: <b>{res['skipped']}</b>"
    )


@dp.callback_query(F.data.startswith("px_view:"))
async def cb_px_view(cb: CallbackQuery):
    pid = int(cb.data.split(":", 1)[1])
    p = await db.db_proxy_get_by_id(pid)
    if not p or p["owner_id"] != cb.from_user.id:
        return await cb.answer("Не найдено.", show_alert=True)
    mark = ("✅" if p["status"] == "alive"
            else "❌" if p["status"] == "dead" else "❓")
    text = (
        f"🔑 <b>Прокси #{pid}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📡  Адрес:    <code>{p['proxy_str']}</code>\n"
        f"📊  Статус:   {mark} {p['status']}\n"
        f"📝  Заметка:  {p.get('note') or '—'}"
    )
    await cb.message.edit_text(
        text,
        reply_markup=kb(
            [("🔍 Проверить", f"px_check:{pid}"),
             ("✏️ Заметка", f"px_note:{pid}")],
            [("🗑 Удалить прокси", f"px_del:{pid}")],
            [("‹ Назад", "px_list")],
        ),
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("px_check:"))
async def cb_px_check(cb: CallbackQuery):
    pid = int(cb.data.split(":", 1)[1])
    p = await db.db_proxy_get_by_id(pid)
    if not p:
        return await cb.answer("Не найдено.", show_alert=True)
    ok = await check_proxy_connection(p["proxy_str"])
    await db.db_proxy_update_status(pid, "alive" if ok else "dead")
    await cb.answer("✅ Жив" if ok else "❌ Мёртв")
    await cb_px_view(cb)


@dp.callback_query(F.data.startswith("px_note:"))
async def cb_px_note(cb: CallbackQuery):
    pid = int(cb.data.split(":", 1)[1])
    uid = cb.from_user.id
    await cb.answer()
    txt = await ask_with_cancel(bot, cb.message.chat.id, uid,
                                f"✏️ Заметка для прокси #{pid}:")
    if txt is None:
        return
    await db.db_proxy_update_note(pid, txt.strip()[:128])
    await cb.message.answer("✅ Заметка обновлена.")


@dp.callback_query(F.data.startswith("px_del:"))
async def cb_px_del(cb: CallbackQuery):
    pid = int(cb.data.split(":", 1)[1])
    await db.db_proxy_delete(pid)
    await cb.answer("Удалён.")
    await cb_px_list(cb)


# =================================================================
# ── СЕКЦИЯ: 🌐 ГЛОБАЛЬНЫЕ ПРОКСИ ──
# CRUD для админов, read-only с маскировкой для остальных.
# =================================================================
def _gpx_render_row(g: Dict[str, Any], is_admin: bool) -> str:
    mark = ("✅" if g.get("status") == "alive"
            else "❌" if g.get("status") == "dead" else "❓")
    if is_admin:
        body = f"<code>{g['proxy_str']}</code>"
    else:
        body = f"<code>{mask_proxy(g['proxy_str'])}</code>"
    note = (f" — {g['note']}" if g.get("note") else "")
    return f"{mark} #{g['id']} — {body}{note}"


@dp.callback_query(F.data == "gpx_list")
async def cb_gpx_list(cb: CallbackQuery):
    uid = cb.from_user.id
    is_admin = await db.db_admins_check(uid)
    globs = await db.db_gproxy_get_all()
    alive = sum(1 for g in globs if g.get("status") == "alive")
    dead  = sum(1 for g in globs if g.get("status") == "dead")
    if not globs:
        text = (
            "🌐 <b>Глобальные прокси</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "Глобальные прокси не добавлены."
        )
    else:
        text = (
            f"🌐 <b>Глобальные прокси</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"Всего: <b>{len(globs)}</b>  ·  "
            f"✅ {alive}  ·  ❌ {dead}\n"
        )
        for g in globs[:20]:
            text += "\n" + _gpx_render_row(g, is_admin)
        if len(globs) > 20:
            text += f"\n…ещё {len(globs) - 20}"
        if not is_admin:
            text += "\n\n<i>🔒 Только для чтения — управление у админов</i>"

    rows = []
    for g in globs[:20]:
        mark = ("✅" if g.get("status") == "alive"
                else "❌" if g.get("status") == "dead" else "❓")
        rows.append([(f"{mark} #{g['id']}", f"gpx_view:{g['id']}")])
    if is_admin:
        rows.append([("➕ Добавить", "gpx_add"),
                     ("🔍 Проверить все", "gpx_checkall")])
    rows.append([("‹ Назад", "px_list"), home_btn()])
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))
    await cb.answer()


@dp.callback_query(F.data == "gpx_add")
async def cb_gpx_add(cb: CallbackQuery):
    uid = cb.from_user.id
    if not await db.db_admins_check(uid):
        return await cb.answer("⛔ Только админ.", show_alert=True)
    await cb.answer()
    def _has_valid_proxy_g(text: str) -> bool:
        return any(parse_proxy_string(l.strip())
                   for l in text.splitlines() if l.strip())

    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "🌐 Пришлите глобальные прокси (host:port:user:pass).\n"
        "Можно несколько строк. Дубликаты будут проигнорированы.",
        validator=_has_valid_proxy_g,
        error_msg="❌ Не нашёл валидных прокси. Формат: host:port:user:pass",
    )
    if raw is None:
        return
    added = 0
    skipped_dup = 0
    skipped_bad = 0
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        if not parse_proxy_string(line):
            skipped_bad += 1
            continue
        new_id = await db.db_gproxy_add(line, "")
        if new_id is None:
            skipped_dup += 1
        else:
            added += 1
    text = f"✅ Добавлено: <b>{added}</b>"
    if skipped_dup:
        text += f"\n♻️ Дубликатов пропущено: {skipped_dup}"
    if skipped_bad:
        text += f"\n❌ Невалидных строк: {skipped_bad}"
    await cb.message.answer(text)


@dp.callback_query(F.data == "gpx_checkall")
async def cb_gpx_checkall(cb: CallbackQuery):
    uid = cb.from_user.id
    if not await db.db_admins_check(uid):
        return await cb.answer("⛔ Только админ.", show_alert=True)
    globs = await db.db_gproxy_get_all()
    await cb.answer(f"⏳ Проверяю {len(globs)} прокси…")
    _sem = asyncio.Semaphore(10)

    async def _check(g):
        async with _sem:
            ok = await check_proxy_connection(g["proxy_str"])
            await db.db_gproxy_update_status(g["id"], "alive" if ok else "dead")
            return ok

    results = await asyncio.gather(*[_check(g) for g in globs])
    alive = sum(results)
    await cb.message.answer(
        f"🔍 <b>Проверка завершена</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Проверено: <b>{len(globs)}</b>\n"
        f"✅ Живых: <b>{alive}</b>\n"
        f"❌ Мёртвых: <b>{len(globs) - alive}</b>"
    )


@dp.callback_query(F.data.startswith("gpx_view:"))
async def cb_gpx_view(cb: CallbackQuery):
    uid = cb.from_user.id
    is_admin = await db.db_admins_check(uid)
    pid = int(cb.data.split(":", 1)[1])
    g = await db.db_gproxy_get_by_id(pid)
    if not g:
        return await cb.answer("Не найдено.", show_alert=True)
    mark = ("✅" if g["status"] == "alive"
            else "❌" if g["status"] == "dead" else "❓")
    body = (g['proxy_str'] if is_admin else mask_proxy(g['proxy_str']))
    text = (
        f"🌐 <b>Глобал #{pid}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📡  Адрес:    <code>{body}</code>\n"
        f"📊  Статус:   {mark} {g['status']}\n"
        f"📝  Заметка:  {g.get('note') or '—'}"
    )
    rows: List[List[Tuple[str, str]]] = []
    if is_admin:
        rows.append([("🔍 Проверить", f"gpx_check:{pid}"),
                     ("✏️ Заметка", f"gpx_note:{pid}")])
        rows.append([("🗑 Удалить прокси", f"gpx_del:{pid}")])
    rows.append([("‹ Назад", "gpx_list")])
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))
    await cb.answer()


@dp.callback_query(F.data.startswith("gpx_check:"))
async def cb_gpx_check(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔ Только админ.", show_alert=True)
    pid = int(cb.data.split(":", 1)[1])
    g = await db.db_gproxy_get_by_id(pid)
    if not g:
        return await cb.answer("Не найдено.", show_alert=True)
    ok = await check_proxy_connection(g["proxy_str"])
    await db.db_gproxy_update_status(pid, "alive" if ok else "dead")
    await cb.answer("✅ Жив" if ok else "❌ Мёртв")
    await cb_gpx_view(cb)


@dp.callback_query(F.data.startswith("gpx_note:"))
async def cb_gpx_note(cb: CallbackQuery):
    uid = cb.from_user.id
    if not await db.db_admins_check(uid):
        return await cb.answer("⛔ Только админ.", show_alert=True)
    pid = int(cb.data.split(":", 1)[1])
    await cb.answer()
    txt = await ask_with_cancel(
        bot, cb.message.chat.id, uid,
        f"✏️ Заметка для глобал-прокси #{pid}:"
    )
    if txt is None:
        return
    await db.db_gproxy_update_note(pid, txt.strip()[:128])
    await cb.message.answer("✅ Заметка обновлена.")


@dp.callback_query(F.data.startswith("gpx_del:"))
async def cb_gpx_del(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔ Только админ.", show_alert=True)
    pid = int(cb.data.split(":", 1)[1])
    await db.db_gproxy_delete(pid)
    await cb.answer("Удалён.")
    await cb_gpx_list(cb)


# =================================================================
# ── СЕКЦИЯ: 🤖 АВТОМАТИЗАЦИЯ — ОБЩИЕ HELPER-Ы ──
# =================================================================
# Хранилище списка групп (по uid) для коротких callback'ов «по индексу».
_grp_index_cache: Dict[int, List[str]] = {}


async def _send_target_picker(chat_id: int, prefix: str, title: str):
    """
    Показать инлайн-меню выбора целей: [Все] [Группа] [Вручную].
    prefix используется в callback_data: <prefix>:all / <prefix>:grp /
    <prefix>:man.
    """
    await bot.send_message(
        chat_id,
        title + "\n\n🎯 <b>Выберите цели:</b>",
        reply_markup=kb(
            [("📋 Все аккаунты", f"{prefix}:all")],
            [("📁 По группе", f"{prefix}:grp"),
             ("✏️ Вручную", f"{prefix}:man")],
            [home_btn()],
        ),
    )


async def _send_groups_picker(uid: int, chat_id: int, prefix: str):
    groups = await db.db_get_groups_by_owner(uid)
    if not groups:
        await bot.send_message(chat_id, "📁 У вас нет групп.")
        return
    _grp_index_cache[uid] = groups
    rows = []
    for i, g in enumerate(groups[:30]):
        rows.append([(f"📁 {g}", f"{prefix}:gi:{i}")])
    rows.append([("‹ Отмена", "action_cancel")])
    await bot.send_message(chat_id, "📁 Выберите группу:",
                           reply_markup=kb(*rows))


async def _resolve_targets_all(uid: int) -> List[Dict[str, Any]]:
    return await db.db_get_accounts_by_owner(uid)


async def _resolve_targets_group(uid: int, gi: int) -> List[Dict[str, Any]]:
    groups = _grp_index_cache.get(uid, [])
    if 0 <= gi < len(groups):
        return await db.db_get_accounts_by_group(uid, groups[gi])
    return []


async def _resolve_targets_manual(uid: int, chat_id: int
                                  ) -> List[Dict[str, Any]]:
    raw = await ask_with_retry(
        bot, chat_id, uid,
        "📱 Пришлите номера (через запятую или с новой строки):",
        validator=lambda t: any(
            validate_phone(tok) for tok in re.split(r"[,\n;\s]+", t) if tok
        ),
        error_msg="❌ Не нашёл валидных номеров. Формат: +79991112233",
    )
    if not raw:
        return []
    phones = []
    for tok in re.split(r"[,\n;\s]+", raw):
        p = validate_phone(tok)
        if p:
            phones.append(p)
    out = []
    for p in phones:
        a = await db.db_get_account(p)
        if a and a.get("owner_id") == uid:
            out.append(a)
    return out


# =================================================================
# ── СЕКЦИЯ: УНИВЕРСАЛЬНЫЙ РУЧНОЙ ВЫБОР АККАУНТОВ ──
# =================================================================
async def _show_man_submenu(cb: CallbackQuery) -> None:
    """
    Показывает подменю выбора метода для ручного выбора аккаунтов:
    «Ввести номера» или «Выбрать из списка».
    Контекст сохранён в _man_sel_ctx[uid].
    """
    text = "✏️ <b>Ручной выбор аккаунтов</b>\n\nКак хотите выбрать?"
    markup = kb(
        [("✏️ Ввести номера", "man_type")],
        [("📋 Выбрать из списка", "man_sel:0")],
        [("❌ Отмена", "action_cancel")],
    )
    try:
        await cb.message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=markup)


@dp.callback_query(F.data == "man_type")
async def cb_man_type(cb: CallbackQuery):
    """Пользователь выбрал «Ввести номера» в универсальном выборщике."""
    uid = cb.from_user.id
    await cb.answer()
    targets = await _resolve_targets_manual(uid, cb.message.chat.id)
    if not targets:
        return await bot.send_message(uid, "❌ Аккаунтов не найдено.")
    await _man_after_confirm(cb, uid, [a["phone"] for a in targets])


async def _render_man_selector(cb: CallbackQuery, uid: int, page: int) -> None:
    """Рисует чекбокс-список аккаунтов для универсального выборщика."""
    accs = await db.db_get_accounts_by_owner(uid)
    if not accs:
        await cb.message.answer("❌ У вас нет аккаунтов.")
        return

    selected = _man_selection.setdefault(uid, set())
    total = len(accs)
    pages = max(1, (total + _MAN_SEL_PER_PAGE - 1) // _MAN_SEL_PER_PAGE)
    page = max(0, min(page, pages - 1))
    chunk = accs[page * _MAN_SEL_PER_PAGE:(page + 1) * _MAN_SEL_PER_PAGE]

    rows = []
    for a in chunk:
        ph = a["phone"]
        un = f" (@{a['username']})" if a.get("username") else ""
        grp = f" 📁{a['grp']}" if a.get("grp") else ""
        icon = "✅" if ph in selected else "⬜"
        rows.append([(f"{icon} {ph}{un}{grp}", f"man_tog:{ph}:{page}")])

    nav = []
    if page > 0:
        nav.append(("◀️", f"man_sel:{page - 1}"))
    nav.append((f"{page + 1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append(("▶️", f"man_sel:{page + 1}"))
    if nav:
        rows.append(nav)

    n_sel = len(selected)
    if n_sel > 0:
        rows.append([(f"✅ Подтвердить ({n_sel} выбрано)", "man_confirm")])
    rows.append([("❌ Отмена", "action_cancel")])

    text = (
        f"📋 <b>Выбор аккаунтов</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Всего: <b>{total}</b>  ·  Выбрано: <b>{n_sel}</b>  ·  "
        f"Стр. {page + 1}/{pages}\n\n"
        f"Нажмите на аккаунт чтобы отметить/снять:"
    )
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))


@dp.callback_query(F.data.startswith("man_sel:"))
async def cb_man_sel(cb: CallbackQuery):
    """Пагинация универсального выборщика."""
    uid = cb.from_user.id
    try:
        page = int(cb.data.split(":", 1)[1])
    except Exception:
        page = 0
    await cb.answer()
    await _render_man_selector(cb, uid, page)


@dp.callback_query(F.data.startswith("man_tog:"))
async def cb_man_tog(cb: CallbackQuery):
    """Переключить галочку у аккаунта в универсальном выборщике."""
    uid = cb.from_user.id
    parts = cb.data.split(":")
    phone = parts[1]
    try:
        page = int(parts[2])
    except Exception:
        page = 0
    selected = _man_selection.setdefault(uid, set())
    if phone in selected:
        selected.discard(phone)
    else:
        selected.add(phone)
    await cb.answer()
    await _render_man_selector(cb, uid, page)


@dp.callback_query(F.data == "man_confirm")
async def cb_man_confirm(cb: CallbackQuery):
    """Подтвердить выбор в универсальном выборщике."""
    uid = cb.from_user.id
    selected = _man_selection.pop(uid, set())
    if not selected:
        return await cb.answer("Не выбрано ни одного аккаунта.",
                               show_alert=True)
    await cb.answer()
    await _man_after_confirm(cb, uid, sorted(selected))


async def _man_after_confirm(cb: CallbackQuery, uid: int,
                              phones: List[str]) -> None:
    """
    Маршрутизатор: после подтверждения ручного выбора передаёт
    список телефонов в нужный обработчик в зависимости от контекста
    _man_sel_ctx[uid].
    """
    ctx = _man_sel_ctx.pop(uid, "")

    # Восстанавливаем объекты аккаунтов из телефонов
    targets: List[Dict[str, Any]] = []
    for ph in phones:
        a = await db.db_get_account(ph)
        if a and a.get("owner_id") == uid:
            targets.append(a)

    if not targets:
        return await bot.send_message(uid, "❌ Аккаунтов не найдено.")

    if ctx == "mass_t":
        await _mass_after_targets(cb, uid, targets)
    elif ctx == "ldvr_t":
        await _ldvr_after_targets(cb, uid, targets)
    elif ctx == "xor_t":
        await _xor_after_targets(cb, uid, targets)
    elif ctx == "subdv_t":
        await _subdv_after_targets(cb, uid, targets)
    elif ctx == "rtag_t":
        await _rtag_after_targets(cb, uid, targets)
    else:
        await bot.send_message(uid, "❌ Контекст выбора потерян.")


# =================================================================
# ── СЕКЦИЯ: 🚀 МАССОВЫЙ ЗАЛИВ ──
# =================================================================
@dp.callback_query(F.data == "auto_mass")
async def cb_auto_mass(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Завершите текущее действие.",
                               show_alert=True)
    store.mass_data[uid] = {}
    await cb.answer()
    await _send_target_picker(
        cb.message.chat.id, "mass_t",
        "🚀 <b>Массовый залив</b>\n\n"
        "Массово обновит выбранные параметры профиля "
        "для выбранных аккаунтов."
    )


@dp.callback_query(F.data.startswith("mass_t:"))
async def cb_mass_t(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    mode = parts[1]

    if mode == "man":
        _man_sel_ctx[uid] = "mass_t"
        _man_selection.pop(uid, None)
        await cb.answer()
        await _show_man_submenu(cb)
        return

    targets: List[Dict[str, Any]] = []
    if mode == "all":
        targets = await _resolve_targets_all(uid)
        await cb.answer()
    elif mode == "grp" and len(parts) == 2:
        await cb.answer()
        return await _send_groups_picker(uid, cb.message.chat.id, "mass_t")
    elif mode == "gi" and len(parts) == 3:
        await cb.answer()
        targets = await _resolve_targets_group(uid, int(parts[2]))
    else:
        return await cb.answer("Bad", show_alert=True)

    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    await _mass_after_targets(cb, uid, targets)


async def _mass_after_targets(cb: CallbackQuery, uid: int,
                               targets: List[Dict[str, Any]]) -> None:
    """Сохраняет цели и показывает экран выбора что именно менять."""
    store.mass_data[uid] = {
        "targets": [a["phone"] for a in targets],
        "what_sel": set(),
    }
    await _mass_render_what(cb, uid, send_new=True,
                             header=f"✅ Целей: <b>{len(targets)}</b>.\n\n")


async def _mass_render_what(cb: CallbackQuery, uid: int,
                             send_new: bool = False,
                             header: str = "") -> None:
    """Рисует экран выбора параметров для залива (имя/био/фото)."""
    what = store.mass_data.get(uid, {}).get("what_sel", set())
    rows = [
        [(f"{'✅' if 'name'  in what else '⬜'} ✏️ Имена",  "mass_what_tog:name")],
        [(f"{'✅' if 'bio'   in what else '⬜'} 📝 Био",    "mass_what_tog:bio")],
        [(f"{'✅' if 'photo' in what else '⬜'} 📷 Фото",   "mass_what_tog:photo")],
    ]
    if what:
        rows.append([("✅ Продолжить", "mass_what_confirm")])
    rows.append([("❌ Отмена", "action_cancel")])
    text = (
        header +
        "🚀 <b>Что изменить?</b>\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        "Выберите один или несколько пунктов:"
    )
    if send_new:
        await bot.send_message(cb.message.chat.id, text,
                               reply_markup=kb(*rows))
    else:
        try:
            await cb.message.edit_text(text, reply_markup=kb(*rows))
        except TelegramBadRequest:
            pass


@dp.callback_query(F.data.startswith("mass_what_tog:"))
async def cb_mass_what_tog(cb: CallbackQuery):
    uid = cb.from_user.id
    item = cb.data.split(":", 1)[1]
    what = store.mass_data.setdefault(uid, {}).setdefault("what_sel", set())
    if item in what:
        what.discard(item)
    else:
        what.add(item)
    await cb.answer()
    await _mass_render_what(cb, uid)


@dp.callback_query(F.data == "mass_what_confirm")
async def cb_mass_what_confirm(cb: CallbackQuery):
    uid = cb.from_user.id
    what = store.mass_data.get(uid, {}).get("what_sel", set())
    if not what:
        return await cb.answer("Выберите хотя бы один пункт.", show_alert=True)
    await cb.answer()
    chat_id = cb.message.chat.id
    md = store.mass_data.get(uid, {})

    if "name" in what:
        txt = await ask_with_cancel(
            bot, chat_id, uid,
            "✏️ Пришлите ИМЕНА (каждое с новой строки).\n"
            "Рандомно раздаются по аккаунтам:")
        if not txt:
            return await restore_main_menu(bot, chat_id, uid, "Отменено.")
        md["names"] = [s.strip() for s in txt.splitlines() if s.strip()]

    if "bio" in what:
        txt = await ask_with_cancel(
            bot, chat_id, uid,
            "📝 Пришлите БИО (каждое с новой строки).\n"
            "Рандомно раздаются по аккаунтам:")
        if txt is None:
            return await restore_main_menu(bot, chat_id, uid, "Отменено.")
        md["bios"] = [s.strip() for s in txt.splitlines() if s.strip()]

    if "photo" in what:
        store.photo_collecting[uid] = True
        store.clear_temp_photos(uid)
        await bot.send_message(
            chat_id,
            "📸 Пришлите ФОТО (несколько). Затем нажмите «📸 Готово».",
            reply_markup=kb(
                [("📸 Готово", "mass_photodone")],
                [("❌ Отмена", "action_cancel")],
            ),
        )
    else:
        # Фото не нужно — запускаем сразу
        await _mass_run(chat_id, uid, md)


@dp.callback_query(F.data == "mass_photodone")
async def cb_mass_photodone(cb: CallbackQuery):
    uid = cb.from_user.id
    photos = store.get_temp_photos(uid)
    store.photo_collecting[uid] = False
    md = store.mass_data.get(uid) or {}
    md["photos"] = photos
    await cb.answer()
    if not md.get("targets"):
        return await cb.message.answer("❌ Цели потеряны.")
    await _mass_run(cb.message.chat.id, uid, md)


async def _mass_run(chat_id: int, uid: int, md: dict) -> None:
    """Запускает задачу массового залива для параметров из md."""
    targets = md.get("targets") or []
    what = md.get("what_sel", set())
    if not targets:
        await bot.send_message(uid, "❌ Цели потеряны.")
        return

    async def _runner():
        await _start_progress(bot, chat_id, uid, total=len(targets),
                              store=store, title="🚀 Массовый залив")
        ok = 0
        for ph in targets:
            await _update_progress(bot, uid, store, current=ph)
            try:
                cli = await get_or_create_account_client(ph, uid)
                if not cli:
                    await _update_progress(bot, uid, store, done_inc=1,
                                           current=None,
                                           error=f"{ph}: не подключился")
                    continue
                kwargs: Dict[str, Any] = {}
                if "name" in what and md.get("names"):
                    kwargs["first_name"] = random.choice(md["names"])[:64]
                if "bio" in what and md.get("bios"):
                    kwargs["about"] = random.choice(md["bios"])[:70]
                if kwargs:
                    await cli(UpdateProfileRequest(**kwargs))
                if "photo" in what and md.get("photos"):
                    photo_path = random.choice(md["photos"])
                    try:
                        existing = await cli(GetUserPhotosRequest(
                            user_id="me", offset=0, max_id=0, limit=10))
                        ips = [InputPhoto(id=p.id,
                                          access_hash=p.access_hash,
                                          file_reference=p.file_reference)
                               for p in existing.photos]
                        if ips:
                            await cli(DeletePhotosRequest(id=ips))
                    except Exception:
                        pass
                    await cli(UploadProfilePhotoRequest(
                        file=await cli.upload_file(photo_path)
                    ))
                ok += 1
                await _update_progress(bot, uid, store, done_inc=1,
                                       current=None)
            except Exception as e:
                await _update_progress(bot, uid, store, done_inc=1,
                                       current=None, error=f"{ph}: {e}")
            await asyncio.sleep(random.uniform(7, 20))

        changed = []
        if "name" in what:  changed.append("имена")
        if "bio" in what:   changed.append("био")
        if "photo" in what: changed.append("фото")
        await _finish_progress(
            bot, uid, store,
            summary_extra=(f"Обновлено: {ok}/{len(targets)}\n"
                           f"Изменено: {', '.join(changed)}"),
        )
        store.clear_temp_photos(uid)
        store.mass_data.pop(uid, None)
        await restore_main_menu(bot, chat_id, uid)

    await task_queue.submit(
        _runner, owner_id=uid, notify=notify_owner,
        title=f"Массовый залив {len(targets)}",
    )


# =================================================================
# ── СЕКЦИЯ: 🤖 РЕГА ЛДВ ──
# =================================================================
@dp.callback_query(F.data == "auto_ldv")
async def cb_auto_ldv(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Занято.", show_alert=True)
    store.ldv_data[uid] = {}
    await cb.answer()
    await _send_target_picker(
        cb.message.chat.id, "ldvr_t",
        "🤖 <b>Регистрация LDV</b>\n\n"
        "Зарегистрирует аккаунты в @leomatchbot "
        "и запланирует автолайкинг."
    )


async def _ldvr_after_targets(cb: CallbackQuery, uid: int,
                               targets: List[Dict[str, Any]]) -> None:
    """Продолжает поток регистрации ЛДВ после выбора целей."""
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    store.ldv_data.setdefault(uid, {})["targets"] = [a["phone"] for a in targets]

    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "📋 Пришлите данные (6 строк через Enter):\n"
        "1. Возраст (можно несколько через запятую)\n"
        "2. Пол: «Я девушка» или «Я парень»\n"
        "3. Кого показывать: «Парни» или «Девушки»\n"
        "4. Город (можно несколько через запятую)\n"
        "5. Имя (можно несколько через запятую)\n"
        "6. Задержка в минутах перед стартом",
        validator=lambda t: len([s.strip() for s in t.splitlines() if s.strip()]) >= 6,
        error_msg="❌ Нужно 6 непустых строк.",
    )
    if not raw:
        return await restore_main_menu(bot, cb.message.chat.id, uid)
    lines = [s.strip() for s in raw.splitlines() if s.strip()]
    ages   = [s.strip() for s in lines[0].split(",") if s.strip()]
    sex    = lines[1]
    target = lines[2]
    cities = [s.strip() for s in lines[3].split(",") if s.strip()]
    names  = [s.strip() for s in lines[4].split(",") if s.strip()]
    try:
        delay_min = float(lines[5])
    except Exception:
        delay_min = 0.0
    store.ldv_data[uid].update({
        "ages": ages, "sex": sex, "target": target,
        "cities": cities, "names": names, "delay_min": delay_min,
    })
    store.photo_collecting[uid] = True
    store.clear_temp_photos(uid)
    await bot.send_message(
        cb.message.chat.id,
        "📸 Пришлите ФОТО (несколько). Затем нажмите «📸 Готово».",
        reply_markup=kb(
            [("📸 Готово", "ldvr_photodone")],
            [("🛑 Отменить регистрацию ЛДВ", "ldvr_cancel_all")],
            [("❌ Отмена", "action_cancel")],
        ),
    )


@dp.callback_query(F.data.startswith("ldvr_t:"))
async def cb_ldvr_t(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    mode = parts[1]

    if mode == "man":
        _man_sel_ctx[uid] = "ldvr_t"
        _man_selection.pop(uid, None)
        await cb.answer()
        await _show_man_submenu(cb)
        return

    targets: List[Dict[str, Any]] = []
    if mode == "all":
        targets = await _resolve_targets_all(uid); await cb.answer()
    elif mode == "grp" and len(parts) == 2:
        await cb.answer()
        return await _send_groups_picker(uid, cb.message.chat.id, "ldvr_t")
    elif mode == "gi":
        await cb.answer()
        targets = await _resolve_targets_group(uid, int(parts[2]))
    else:
        return await cb.answer("Bad", show_alert=True)
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    await _ldvr_after_targets(cb, uid, targets)


@dp.callback_query(F.data == "ldvr_cancel_all")
async def cb_ldvr_cancel_all(cb: CallbackQuery):
    uid = cb.from_user.id
    d = store.ldv_data.get(uid) or {}
    n = 0
    for ph in d.get("targets") or []:
        store.ldv_reg_cancel.add(ph)
        n += 1
    await cb.answer(f"Отменяю партию ЛДВ ({n}).", show_alert=True)


@dp.callback_query(F.data == "ldvr_photodone")
async def cb_ldvr_photodone(cb: CallbackQuery):
    uid = cb.from_user.id
    photos = store.get_temp_photos(uid)
    store.photo_collecting[uid] = False
    d = store.ldv_data.get(uid) or {}
    d["photos"] = photos
    targets = d.get("targets") or []
    await cb.answer()
    if not targets or not photos:
        return await cb.message.answer("❌ Целей или фото нет.")

    async def _runner():
        if d.get("delay_min", 0) > 0:
            await bot.send_message(
                uid, f"⏱ Старт через {d['delay_min']:.1f} мин."
            )
            await asyncio.sleep(d["delay_min"] * 60)
        await _start_progress(bot, cb.message.chat.id, uid,
                              total=len(targets), store=store,
                              title="🤖 Рега ЛДВ")
        success = []

        # Перемешиваем списки один раз — города и имена не повторяются
        # до исчерпания списка, затем начинают цикл заново
        ages_list   = list(d.get("ages")   or ["20"])
        cities_list = list(d.get("cities") or ["Москва"])
        names_list  = list(d.get("names")  or ["Аня"])
        random.shuffle(ages_list)
        random.shuffle(cities_list)
        random.shuffle(names_list)

        for i, ph in enumerate(targets):
            if ph in store.ldv_reg_cancel:
                store.ldv_reg_cancel.discard(ph)
                await _update_progress(bot, uid, store, done_inc=1,
                                       error=f"{ph}: отменено")
                continue
            await _update_progress(bot, uid, store,
                                   current=f"{ph} — подключение…")
            try:
                cli = await get_or_create_account_client(ph, uid)
                if not cli:
                    raise RuntimeError("connect failed")

                # Назначаем уникальные (без повторов до конца списка) данные
                per_acc = dict(d)
                per_acc.pop("ages", None)
                per_acc.pop("cities", None)
                per_acc.pop("names", None)
                per_acc["age"]  = str(ages_list[i % len(ages_list)])
                per_acc["city"] = cities_list[i % len(cities_list)]
                per_acc["name"] = names_list[i % len(names_list)]

                await _update_progress(bot, uid, store,
                                       current=f"{ph} — регистрация…")
                # пытаемся возобновить, если в reg_state есть состояние
                state = await db.db_get_reg_state(ph, config.LDV_BOT)
                if state:
                    ok = await register_ldv_resumable(
                        cli, ph, per_acc, uid,
                        notify_func=lambda t, _u=uid:
                            user_log(_u, t),
                        photos_request_func=None,
                        cancel_set=store.ldv_reg_cancel,
                    )
                else:
                    ok = await register_one_ldv(
                        cli, ph, per_acc,
                        notify_func=lambda t, _u=uid:
                            user_log(_u, t),
                        owner_id=uid,
                        cancel_set=store.ldv_reg_cancel,
                    )
                if ok:
                    success.append(ph)
                await _update_progress(bot, uid, store, done_inc=1,
                                       current=None,
                                       error=None if ok
                                       else f"{ph}: не зарегистрирован")
            except Exception as e:
                await _update_progress(bot, uid, store, done_inc=1,
                                       error=f"{ph}: {e}")
        # запланировать лайкинг через 10 часов
        nxt = time.time() + config.LDV_INITIAL_DELAY_HOURS * 3600
        for ph in success:
            await db.db_schedule_ldv_task(ph, uid, nxt, step=0,
                                          status="pending")
        await _finish_progress(
            bot, uid, store,
            summary_extra=(f"Зарегистрировано: {len(success)}/{len(targets)}\n"
                           f"📅 Лайкинг запланирован через "
                           f"{config.LDV_INITIAL_DELAY_HOURS}ч "
                           f"для {len(success)} аккаунтов."),
        )
        store.clear_temp_photos(uid)
        store.ldv_data.pop(uid, None)
        await restore_main_menu(bot, cb.message.chat.id, uid)

    await task_queue.submit(
        _runner, owner_id=uid, notify=notify_owner,
        title=f"Рега ЛДВ {len(targets)}",
    )


# =================================================================
# ── СЕКЦИЯ: 💘 РЕГА XO ──
# =================================================================
@dp.callback_query(F.data == "auto_xo")
async def cb_auto_xo(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Занято.", show_alert=True)
    store.xo_data[uid] = {}
    await cb.answer()
    await _send_target_picker(
        cb.message.chat.id, "xor_t",
        "💘 <b>Регистрация XO</b>\n\n"
        "Зарегистрирует аккаунты в XO-боте "
        "и запустит автолайкинг."
    )


async def _xor_after_targets(cb: CallbackQuery, uid: int,
                              targets: List[Dict[str, Any]]) -> None:
    """Продолжает поток регистрации XO после выбора целей."""
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    store.xo_data.setdefault(uid, {})["targets"] = [a["phone"] for a in targets]

    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "📋 Пришлите данные (4 строки через Enter):\n"
        "1. Пол: «💁‍♀️ я девушка» или «🙋‍♂️ я парень»\n"
        "2. Дата рождения (дд.мм.гггг)\n"
        "3. Город\n"
        "4. Имя",
        validator=lambda t: len([s.strip() for s in t.splitlines() if s.strip()]) >= 4,
        error_msg="❌ Нужно 4 непустых строк.",
    )
    if not raw:
        return await restore_main_menu(bot, cb.message.chat.id, uid)
    lines = [s.strip() for s in raw.splitlines() if s.strip()]
    store.xo_data[uid].update({
        "sex":      lines[0],
        "birthday": lines[1],
        "cities":   [c.strip() for c in lines[2].split(",") if c.strip()],
        "names":    [n.strip() for n in lines[3].split(",") if n.strip()],
    })
    store.photo_collecting[uid] = True
    store.clear_temp_photos(uid)
    await bot.send_message(
        cb.message.chat.id,
        "📸 Пришлите ФОТО для XO. Затем «📸 Готово».",
        reply_markup=kb(
            [("📸 Готово", "xor_photodone")],
            [("🛑 Отменить регистрацию XO", "xor_cancel_all")],
            [("❌ Отмена", "action_cancel")],
        ),
    )


@dp.callback_query(F.data.startswith("xor_t:"))
async def cb_xor_t(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    mode = parts[1]

    if mode == "man":
        _man_sel_ctx[uid] = "xor_t"
        _man_selection.pop(uid, None)
        await cb.answer()
        await _show_man_submenu(cb)
        return

    targets: List[Dict[str, Any]] = []
    if mode == "all":
        targets = await _resolve_targets_all(uid); await cb.answer()
    elif mode == "grp" and len(parts) == 2:
        await cb.answer()
        return await _send_groups_picker(uid, cb.message.chat.id, "xor_t")
    elif mode == "gi":
        await cb.answer()
        targets = await _resolve_targets_group(uid, int(parts[2]))
    else:
        return await cb.answer("Bad", show_alert=True)
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    await _xor_after_targets(cb, uid, targets)


@dp.callback_query(F.data == "xor_cancel_all")
async def cb_xor_cancel_all(cb: CallbackQuery):
    uid = cb.from_user.id
    d = store.xo_data.get(uid) or {}
    for ph in d.get("targets") or []:
        store.xo_reg_cancel.add(ph)
    await cb.answer("Отменяю партию XO.", show_alert=True)


@dp.callback_query(F.data == "xor_photodone")
async def cb_xor_photodone(cb: CallbackQuery):
    uid = cb.from_user.id
    photos = store.get_temp_photos(uid)
    store.photo_collecting[uid] = False
    d = store.xo_data.get(uid) or {}
    d["photos"] = photos
    targets = d.get("targets") or []
    await cb.answer()
    if not targets or not photos:
        return await cb.message.answer("❌ Целей или фото нет.")

    async def _runner():
        await _start_progress(bot, cb.message.chat.id, uid,
                              total=len(targets), store=store,
                              title="💘 Рега XO")
        success = []

        # Перемешиваем списки один раз — без повторов до исчерпания
        cities_list = list(d.get("cities") or [d.get("city") or "Москва"])
        names_list  = list(d.get("names")  or [d.get("name")  or "Аня"])
        random.shuffle(cities_list)
        random.shuffle(names_list)

        for i, ph in enumerate(targets):
            if ph in store.xo_reg_cancel:
                store.xo_reg_cancel.discard(ph)
                await _update_progress(bot, uid, store, done_inc=1,
                                       error=f"{ph}: отменено")
                continue
            await _update_progress(bot, uid, store,
                                   current=f"{ph} — подключение…")
            try:
                cli = await get_or_create_account_client(ph, uid)
                if not cli:
                    raise RuntimeError("connect failed")

                # Назначаем уникальные данные для этого аккаунта
                per_acc = dict(d)
                per_acc.pop("cities", None)
                per_acc.pop("names", None)
                per_acc["city"] = cities_list[i % len(cities_list)]
                per_acc["name"] = names_list[i % len(names_list)]

                await _update_progress(bot, uid, store,
                                       current=f"{ph} — регистрация…")
                state = await db.db_get_reg_state(ph, config.XO_BOT)
                if state:
                    ok = await register_xo_resumable(
                        cli, ph, per_acc, uid,
                        notify_func=lambda t, _u=uid: user_log(_u, t),
                        cancel_set=store.xo_reg_cancel,
                    )
                else:
                    ok = await register_one_xo(
                        cli, ph, per_acc,
                        notify_func=lambda t, _u=uid: user_log(_u, t),
                        cancel_set=store.xo_reg_cancel,
                        owner_id=uid,
                    )
                if ok:
                    success.append(ph)
                await _update_progress(
                    bot, uid, store, done_inc=1, current=None,
                    error=None if ok else f"{ph}: не зарегистрирован",
                )
            except Exception as e:
                await _update_progress(bot, uid, store, done_inc=1,
                                       error=f"{ph}: {e}")

        # запустить лайкинг для успешных
        for ph in success:
            await db.db_schedule_xo_task(ph, uid, time.time() + 5,
                                         status="pending")
        await _finish_progress(
            bot, uid, store,
            summary_extra=(f"Зарегистрировано: {len(success)}/{len(targets)}\n"
                           f"💘 XO-лайкинг запланирован."),
        )
        store.clear_temp_photos(uid)
        store.xo_data.pop(uid, None)
        for ph in targets:
            store.xo_reg_cancel.discard(ph)
        await restore_main_menu(bot, cb.message.chat.id, uid)

    await task_queue.submit(
        _runner, owner_id=uid, notify=notify_owner,
        title=f"Рега XO {len(targets)}",
    )


# =================================================================
# ── СЕКЦИЯ: 📺 ПОДПИСКА НА ДВ ──
# =================================================================
@dp.callback_query(F.data == "auto_subdv")
async def cb_auto_subdv(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Занято.", show_alert=True)
    await cb.answer()
    await _send_target_picker(
        cb.message.chat.id, "subdv_t",
        "📺 <b>Подписка на @leoday</b>\n\n"
        "Подпишет выбранные аккаунты на канал @leoday."
    )


async def _subdv_after_targets(cb: CallbackQuery, uid: int,
                               targets: List[Dict[str, Any]]) -> None:
    """Запускает подписку на @leoday для выбранных аккаунтов."""
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")

    async def _runner():
        await _start_progress(bot, cb.message.chat.id, uid,
                              total=len(targets), store=store,
                              title="📺 Подписка @leoday")
        ok = 0
        for a in targets:
            ph = a["phone"]
            await _update_progress(bot, uid, store, current=ph)
            try:
                cli = await get_or_create_account_client(ph, uid)
                if not cli:
                    raise RuntimeError("connect failed")
                await asyncio.wait_for(cli(JoinChannelRequest("leoday")),
                                       timeout=5)
                ok += 1
                await _update_progress(bot, uid, store, done_inc=1,
                                       current=None)
            except Exception as e:
                await _update_progress(bot, uid, store, done_inc=1,
                                       error=f"{ph}: {e}")
            await asyncio.sleep(random.uniform(5, 15))
        await _finish_progress(
            bot, uid, store,
            summary_extra=f"Подписано: {ok}/{len(targets)}",
        )
        await restore_main_menu(bot, cb.message.chat.id, uid)

    await task_queue.submit(
        _runner, owner_id=uid, notify=notify_owner,
        title=f"Подписка @leoday {len(targets)}",
    )


@dp.callback_query(F.data.startswith("subdv_t:"))
async def cb_subdv_t(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    mode = parts[1]

    if mode == "man":
        _man_sel_ctx[uid] = "subdv_t"
        _man_selection.pop(uid, None)
        await cb.answer()
        await _show_man_submenu(cb)
        return

    targets: List[Dict[str, Any]] = []
    if mode == "all":
        targets = await _resolve_targets_all(uid); await cb.answer()
    elif mode == "grp" and len(parts) == 2:
        await cb.answer()
        return await _send_groups_picker(uid, cb.message.chat.id, "subdv_t")
    elif mode == "gi":
        await cb.answer()
        targets = await _resolve_targets_group(uid, int(parts[2]))
    else:
        return await cb.answer("Bad", show_alert=True)
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    await _subdv_after_targets(cb, uid, targets)


# =================================================================
# ── СЕКЦИЯ: 🏷 СМЕНА ТЕГА (USERNAME) ──
# =================================================================
async def _rtag_after_targets(cb: CallbackQuery, uid: int,
                               targets: List[Dict[str, Any]]) -> None:
    """Запускает массовую смену username для выбранных аккаунтов."""
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    phones = [a["phone"] for a in targets]

    async def _runner():
        await _start_progress(bot, cb.message.chat.id, uid,
                              total=len(phones), store=store,
                              title="🏷 Смена username")
        ok = 0
        for ph in phones:
            await _update_progress(bot, uid, store, current=ph)
            try:
                cli = await get_or_create_account_client(ph, uid)
                if not cli:
                    raise RuntimeError("connect failed")
                new_un = _gen_username()
                await cli(UpdateUsernameRequest(username=new_un))
                await db.db_update_account_field(ph, "username", new_un)
                ok += 1
                await _update_progress(bot, uid, store, done_inc=1,
                                       current=None)
            except Exception as e:
                await _update_progress(bot, uid, store, done_inc=1,
                                       error=f"{ph}: {e}")
            await asyncio.sleep(random.uniform(3, 8))
        await _finish_progress(
            bot, uid, store,
            summary_extra=f"Обновлено: {ok}/{len(phones)}",
        )
        await restore_main_menu(bot, cb.message.chat.id, uid)

    await task_queue.submit(
        _runner, owner_id=uid, notify=notify_owner,
        title=f"Смена username {len(phones)}",
    )


@dp.callback_query(F.data == "auto_rtag")
async def cb_auto_rtag(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Занято.", show_alert=True)
    await cb.answer()
    await _send_target_picker(
        cb.message.chat.id, "rtag_t",
        "🏷 <b>Смена тега (username)</b>\n\n"
        "Сгенерирует случайный username из 3 слов + число 1–100 "
        "для выбранных аккаунтов.",
    )


@dp.callback_query(F.data.startswith("rtag_t:"))
async def cb_rtag_t(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    mode = parts[1]

    if mode == "man":
        _man_sel_ctx[uid] = "rtag_t"
        _man_selection.pop(uid, None)
        await cb.answer()
        await _show_man_submenu(cb)
        return

    targets: List[Dict[str, Any]] = []
    if mode == "all":
        targets = await _resolve_targets_all(uid); await cb.answer()
    elif mode == "grp" and len(parts) == 2:
        await cb.answer()
        return await _send_groups_picker(uid, cb.message.chat.id, "rtag_t")
    elif mode == "gi":
        await cb.answer()
        targets = await _resolve_targets_group(uid, int(parts[2]))
    else:
        return await cb.answer("Bad", show_alert=True)
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    await _rtag_after_targets(cb, uid, targets)


# =================================================================
# ── СЕКЦИЯ: 💬 АВТООТВЕТЫ — UI ──
# =================================================================
@dp.callback_query(F.data == "auto_ar")
async def cb_auto_ar(cb: CallbackQuery):
    uid = cb.from_user.id
    accs = await db.db_get_accounts_by_owner(uid)
    # Один bulk-запрос вместо N отдельных db_ar_is_enabled()
    ar_bulk = await db.db_ar_get_settings_bulk(uid)
    n_on = 0
    n_run = 0
    rows = []
    for a in accs[:30]:
        ph = a["phone"]
        on = bool((ar_bulk.get(ph) or {}).get("enabled"))
        running = ar_manager.is_running(ph)
        if on: n_on += 1
        if running: n_run += 1
        mark = "✅" if (on and running) else ("🟡" if on else "❌")
        rows.append([(f"{mark} {ph}", f"ar_view:{ph}")])
    rows.append([("✅ Включить все", "ar_enable_all"),
                 ("❌ Выключить все", "ar_disable_all")])
    rows.append([("📁 По группе", "ar_by_group"),
                 ("✏️ Текст всем", "ar_text_all")])
    rows.append([home_btn()])
    text = (
        "💬 <b>Автоответы</b>\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        f"Аккаунтов: <b>{len(accs)}</b>  ·  "
        f"Вкл: <b>{n_on}</b>  ·  Работает: <b>{n_run}</b>\n\n"
        "✅ включён и работает\n"
        "🟡 включён, клиент не запущен\n"
        "❌ выключен"
    )
    await cb.message.edit_text(text, reply_markup=kb(*rows))
    await cb.answer()


@dp.callback_query(F.data.startswith("ar_view:"))
async def cb_ar_view(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    s = await db.db_ar_get_settings(uid, phone)
    on = bool(s.get("enabled"))
    running = ar_manager.is_running(phone)
    custom = s.get("custom_text") or "—"
    silenced = ar_manager.silenced_count(phone)
    if on and running:
        status_str = "✅ Включён и работает"
    elif on:
        status_str = "🟡 Включён (клиент не запущен)"
    else:
        status_str = "❌ Выключен"
    text = (
        f"💬 <b>Автоответ</b>  <code>{phone}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊  Статус:          {status_str}\n"
        f"🔇  Замолчано чатов: <b>{silenced}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"✏️  Текст ответа:\n"
        f"<i>{(custom[:200] if custom else '— (используется стандартный) —')}</i>"
    )
    toggle_text = "❌ Выключить" if on else "✅ Включить"
    rows = [
        [(toggle_text, f"ar_toggle:{phone}"),
         ("✏️ Текст", f"ar_text:{phone}")],
        [("🔇 Сбросить молчание", f"ar_reset:{phone}")],
        [("‹ Назад", "auto_ar")],
    ]
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))
    await cb.answer()


@dp.callback_query(F.data.startswith("ar_toggle:"))
async def cb_ar_toggle(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    on = await db.db_ar_is_enabled(uid, phone)
    new = not on
    await db.db_ar_set_enabled(uid, phone, new)
    if new:
        # запустить
        proxy = await get_proxy_for_account(phone, uid)
        s = await db.db_ar_get_settings(uid, phone)
        ok = await ar_manager.start(phone, uid, proxy,
                                    custom_text=s.get("custom_text"))
        await cb.answer("Включён." if ok else "Не запустился.",
                        show_alert=not ok)
    else:
        await ar_manager.stop(phone)
        await cb.answer("Выключен.")
    # перерисовать
    await cb_ar_view(cb)


@dp.callback_query(F.data.startswith("ar_text:"))
async def cb_ar_text(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    await cb.answer()
    txt = await ask_with_cancel(
        bot, cb.message.chat.id, uid,
        f"✏️ Свой текст для <code>{phone}</code> "
        f"(или «-» чтобы сбросить):"
    )
    if txt is None:
        return
    txt = txt.strip()
    if txt == "-":
        new = None
    else:
        new = txt[:200]
    await db.db_ar_set_custom_text(uid, phone, new)
    ar_manager.set_custom_text(phone, new)
    await cb.message.answer("✅ Текст обновлён.")


@dp.callback_query(F.data.startswith("ar_reset:"))
async def cb_ar_reset(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    n = ar_manager.reset_silenced(phone)
    await cb.answer(f"Сброшено: {n}")


async def _ar_set_all(uid: int, value: bool, group: Optional[str] = None):
    if group:
        accs = await db.db_get_accounts_by_group(uid, group)
    else:
        accs = await db.db_get_accounts_by_owner(uid)

    # Один bulk-запрос для настроек
    ar_bulk = await db.db_ar_get_settings_bulk(uid)

    async def _set_one(a):
        ph = a["phone"]
        await db.db_ar_set_enabled(uid, ph, value)
        if value:
            proxy = await get_proxy_for_account(ph, uid)
            custom = (ar_bulk.get(ph) or {}).get("custom_text")
            await ar_manager.start(ph, uid, proxy, custom_text=custom)
        else:
            await ar_manager.stop(ph)

    # Параллельно, но не более 10 одновременных подключений
    sem = asyncio.Semaphore(10)
    async def _limited(a):
        async with sem:
            await _set_one(a)

    await asyncio.gather(*[_limited(a) for a in accs])
    return len(accs)


@dp.callback_query(F.data == "ar_enable_all")
async def cb_ar_enable_all(cb: CallbackQuery):
    n = await _ar_set_all(cb.from_user.id, True)
    await cb.answer(f"Включено: {n}", show_alert=True)


@dp.callback_query(F.data == "ar_disable_all")
async def cb_ar_disable_all(cb: CallbackQuery):
    n = await _ar_set_all(cb.from_user.id, False)
    await cb.answer(f"Выключено: {n}", show_alert=True)


@dp.callback_query(F.data == "ar_by_group")
async def cb_ar_by_group(cb: CallbackQuery):
    uid = cb.from_user.id
    groups = await db.db_get_groups_by_owner(uid)
    if not groups:
        return await cb.answer("Групп нет.", show_alert=True)
    _grp_index_cache[uid] = groups
    rows = []
    for i, g in enumerate(groups[:20]):
        rows.append([(f"✅ {g}", f"ar_grp:on:{i}"),
                     (f"❌ {g}", f"ar_grp:off:{i}")])
    rows.append([("‹ Назад", "auto_ar")])
    await cb.message.edit_text(
        "📁 <b>Автоответ по группе</b>\n\n"
        "✅ — включить группу  |  ❌ — выключить группу",
        reply_markup=kb(*rows),
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("ar_grp:"))
async def cb_ar_grp(cb: CallbackQuery):
    parts = cb.data.split(":")
    mode = parts[1]
    gi = int(parts[2])
    uid = cb.from_user.id
    groups = _grp_index_cache.get(uid, [])
    if not (0 <= gi < len(groups)):
        return await cb.answer("Bad", show_alert=True)
    n = await _ar_set_all(uid, mode == "on", group=groups[gi])
    await cb.answer(f"Готово: {n} аккаунтов.", show_alert=True)


@dp.callback_query(F.data == "ar_text_all")
async def cb_ar_text_all(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    txt = await ask_with_cancel(
        bot, cb.message.chat.id, uid,
        "✏️ Свой текст для ВСЕХ аккаунтов (или «-» чтобы сбросить):"
    )
    if txt is None:
        return
    new = None if txt.strip() == "-" else txt.strip()[:200]
    accs = await db.db_get_accounts_by_owner(uid)
    n = 0
    for a in accs:
        await db.db_ar_set_custom_text(uid, a["phone"], new)
        ar_manager.set_custom_text(a["phone"], new)
        n += 1
    await cb.message.answer(f"✅ Применено к {n} аккаунтам.")


# =================================================================
# ── СЕКЦИЯ: 📊 УПРАВЛЕНИЕ — ПОДРАЗДЕЛЫ ──
# =================================================================

# ── 💘 Рега XO (alias на auto_xo)
@dp.callback_query(F.data == "mng_xo")
async def cb_mng_xo(cb: CallbackQuery):
    await cb_auto_xo(cb)


# ── ❤️ Ручной пролайк ДВ ──
@dp.callback_query(F.data == "mng_manual_ldv")
async def cb_mng_manual_ldv(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "❤️ Пришлите номера для немедленного пролайка ДВ "
        "(через запятую или с новой строки):",
        validator=lambda t: any(
            validate_phone(tok) for tok in re.split(r"[,\n;\s]+", t) if tok
        ),
        error_msg="❌ Не нашёл валидных номеров. Формат: +79991112233",
    )
    if not raw:
        return
    phones = []
    for tok in re.split(r"[,\n;\s]+", raw):
        p = validate_phone(tok)
        if p:
            phones.append(p)
    n = 0
    not_found = []
    for ph in phones:
        a = await db.db_get_account(ph)
        if a:
            owner = a.get("owner_id") or uid
            await db.db_schedule_ldv_task(ph, owner, time.time() + 2,
                                          step=0, status="pending")
            store.cancelled_phones.discard(ph)
            store.paused_phones.discard(ph)
            n += 1
        else:
            not_found.append(ph)
    text = f"✅ Запланировано ДВ: {n}."
    if not_found:
        text += f"\n⚠️ Не найдены в БД: {', '.join(not_found)}"
    await cb.message.answer(text)


# ── 💘 Ручной пролайк XO ──
@dp.callback_query(F.data == "mng_manual_xo")
async def cb_mng_manual_xo(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "💘 Пришлите номера для немедленного пролайка XO "
        "(через запятую или с новой строки):",
        validator=lambda t: any(
            validate_phone(tok) for tok in re.split(r"[,\n;\s]+", t) if tok
        ),
        error_msg="❌ Не нашёл валидных номеров. Формат: +79991112233",
    )
    if not raw:
        return
    phones = []
    for tok in re.split(r"[,\n;\s]+", raw):
        p = validate_phone(tok)
        if p:
            phones.append(p)
    n = 0
    not_found = []
    for ph in phones:
        a = await db.db_get_account(ph)
        if a:
            owner = a.get("owner_id") or uid
            await db.db_schedule_xo_task(ph, owner, time.time() + 2,
                                         status="pending")
            store.xo_liking_paused.discard(ph)
            n += 1
        else:
            not_found.append(ph)
    text = f"✅ Запланировано XO: {n}."
    if not_found:
        text += f"\n⚠️ Не найдены в БД: {', '.join(not_found)}"
    await cb.message.answer(text)


# ── ⚙️ Управление лайкингом ДВ ──
@dp.callback_query(F.data == "mng_ldv")
async def cb_mng_ldv(cb: CallbackQuery):
    await cb.message.edit_text(
        "🤖 <b>Управление лайкингом LDV</b>\n\n"
        "Просматривайте активные циклы, ставьте на паузу "
        "или удаляйте задачи.",
        reply_markup=kb(
            [("📋 Активные циклы", "mng_ldv_list:0")],
            [("🗑 Сбросить все", "mng_ldv_resetall")],
            [("📁 Сбросить по группе", "mng_ldv_resetgrp"),
             ("🎯 Сбросить выборочно", "mng_ldv_resetman")],
            [("‹ Назад", "back_manage"), home_btn()],
        ),
    )
    await cb.answer()


@dp.callback_query(F.data == "back_manage")
async def cb_back_manage(cb: CallbackQuery):
    # перерисуем меню «Управление»
    await cb.message.edit_text(
        "📊 <b>Управление</b>\n\n"
        "Ручной запуск лайкинга, управление задачами "
        "и отмена регистраций.",
        reply_markup=kb(
            [("❤️ Пролайк LDV", "mng_manual_ldv"),
             ("💘 Пролайк XO", "mng_manual_xo")],
            [("⚙️ Задачи LDV", "mng_ldv"),
             ("💘 Задачи XO", "mng_xo_panel")],
            [("🛑 Отмена регистрации", "mng_regcancel")],
            [home_btn()],
        ),
    )
    await cb.answer()


# обёртка над панелью XO (чтобы можно было войти из меню «Управление»)
@dp.callback_query(F.data == "mng_xo_panel")
async def cb_mng_xo_panel(cb: CallbackQuery):
    await _xo_manage_render(cb.message.chat.id, cb.from_user.id,
                            edit_msg=cb.message)
    await cb.answer()


async def _render_ldv_list(cb: CallbackQuery, page: int = 0) -> None:
    """Отрисовывает список LDV-задач. Не вызывает cb.answer()."""
    uid = cb.from_user.id
    tasks = await db.db_get_ldv_tasks_by_owner(uid)
    if not tasks:
        await cb.message.edit_text(
            "📋 <b>LDV-циклы</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "Активных циклов нет.",
            reply_markup=kb([("‹ Назад", "mng_ldv")]),
        )
        return

    per = 8
    total = len(tasks)
    pages = max(1, (total + per - 1) // per)
    page = max(0, min(page, pages - 1))
    chunk = tasks[page * per:(page + 1) * per]

    n_run  = sum(1 for t in tasks if t["phone"] in store.current_liking_phones)
    n_paus = sum(1 for t in tasks if t["phone"] in store.paused_phones)
    lines = [
        f"📋 <b>LDV-циклы</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Задач: <b>{total}</b>  ·  "
        f"▶️ активных: {n_run}  ·  ⏸ пауза: {n_paus}\n"
    ]
    rows = []
    for t in chunk:
        ph = t["phone"]
        st = t["status"]
        nxt = time.strftime("%d.%m %H:%M",
                            time.localtime(t["next_run"] or 0))
        is_paused = ph in store.paused_phones
        running_icon = ("▶️" if ph in store.current_liking_phones
                        else ("⏸" if is_paused else "⏳"))
        lines.append(
            f"{running_icon} {ph} — {st}  /  next: {nxt}  /  шаг {t['step']}"
        )
        rows.append([
            ((("▶️ Resume" if is_paused else "⏸ Pause"),
              f"mng_ldv_pp:{ph}")),
            (("🗑 Удалить", f"mng_ldv_del:{ph}")),
        ])
    nav = []
    if page > 0:
        nav.append(("◀️", f"mng_ldv_list:{page-1}"))
    nav.append((f"{page+1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append(("▶️", f"mng_ldv_list:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([("‹ Назад", "mng_ldv")])
    await cb.message.edit_text("\n".join(lines), reply_markup=kb(*rows))


@dp.callback_query(F.data.startswith("mng_ldv_list:"))
async def cb_mng_ldv_list(cb: CallbackQuery):
    try:
        page = int(cb.data.split(":", 1)[1])
    except Exception:
        page = 0
    await _render_ldv_list(cb, page)
    await cb.answer()


@dp.callback_query(F.data.startswith("mng_ldv_pp:"))
async def cb_mng_ldv_pp(cb: CallbackQuery):
    ph = cb.data.split(":", 1)[1]
    if ph in store.paused_phones:
        store.paused_phones.discard(ph)
        await cb.answer("▶️ Возобновлён.")
    else:
        store.paused_phones.add(ph)
        await cb.answer("⏸ На паузе.")
    await _render_ldv_list(cb, page=0)


@dp.callback_query(F.data.startswith("mng_ldv_del:"))
async def cb_mng_ldv_del(cb: CallbackQuery):
    ph = cb.data.split(":", 1)[1]
    store.cancelled_phones.add(ph)
    await db.db_delete_ldv_task(ph)
    await cb.answer("🗑 Задача удалена.")
    await _render_ldv_list(cb, page=0)


@dp.callback_query(F.data == "mng_ldv_resetall")
async def cb_mng_ldv_resetall(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    confirm = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "⚠️ <b>Удалить ВСЕ LDV-задачи?</b>\n\n"
        "Все циклы лайкинга будут остановлены и удалены из базы.\n"
        "Для подтверждения напишите <b>ДА</b>:",
        validator=lambda t: t.strip().lower() == "да",
        error_msg='❌ Напишите именно "ДА" для подтверждения.',
        parse_mode="HTML",
    )
    if not confirm:
        return await cb.message.answer("✅ Отменено.")
    tasks = await db.db_get_ldv_tasks_by_owner(uid)
    for t in tasks:
        store.cancelled_phones.add(t["phone"])
    n = await db.db_delete_ldv_tasks_by_owner(uid)
    await cb.message.answer(f"🗑 Удалено: {n}")


@dp.callback_query(F.data == "mng_ldv_resetgrp")
async def cb_mng_ldv_resetgrp(cb: CallbackQuery):
    uid = cb.from_user.id
    groups = await db.db_get_groups_by_owner(uid)
    if not groups:
        return await cb.answer("Групп нет.", show_alert=True)
    _grp_index_cache[uid] = groups
    rows = [[(f"📁 {g}", f"mng_ldv_grpdel:{i}")]
            for i, g in enumerate(groups[:30])]
    rows.append([("‹ Назад", "mng_ldv")])
    await cb.message.edit_text("📁 Выберите группу:",
                               reply_markup=kb(*rows))
    await cb.answer()


@dp.callback_query(F.data.startswith("mng_ldv_grpdel:"))
async def cb_mng_ldv_grpdel(cb: CallbackQuery):
    uid = cb.from_user.id
    gi = int(cb.data.split(":", 1)[1])
    groups = _grp_index_cache.get(uid, [])
    if not (0 <= gi < len(groups)):
        return await cb.answer("Bad", show_alert=True)
    grp = groups[gi]
    accs = await db.db_get_accounts_by_group(uid, grp)
    for a in accs:
        store.cancelled_phones.add(a["phone"])
    n = await db.db_delete_ldv_tasks_by_group(uid, grp)
    await cb.answer(f"🗑 Удалено: {n}", show_alert=True)


@dp.callback_query(F.data == "mng_ldv_resetman")
async def cb_mng_ldv_resetman(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "🎯 Пришлите номера для удаления LDV-задач:",
        validator=lambda t: any(
            validate_phone(tok) for tok in re.split(r"[,\n;\s]+", t) if tok
        ),
        error_msg="❌ Не нашёл валидных номеров. Формат: +79991112233",
    )
    if not raw:
        return
    n = 0
    for tok in re.split(r"[,\n;\s]+", raw):
        p = validate_phone(tok)
        if p:
            store.cancelled_phones.add(p)
            await db.db_delete_ldv_task(p)
            n += 1
    await cb.message.answer(f"🗑 Удалено: {n}")


# =================================================================
# ── СЕКЦИЯ: 💘 УПРАВЛЕНИЕ XO ──
# =================================================================
# В управление XO добавим переход через дополнительную кнопку — она
# доступна как часть «Управление» через /xo_manage. Но удобнее показать
# при отсутствии задач сразу из меню «Управление».
@dp.message(Command("xo_manage"))
async def cmd_xo_manage(msg: Message):
    await _xo_manage_render(msg.chat.id, msg.from_user.id)


async def _xo_manage_render(chat_id: int, uid: int, edit_msg=None):
    tasks = await db.db_get_xo_tasks_by_owner(uid)
    if not tasks:
        text = (
            "💘 <b>Управление XO</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "Активных задач нет."
        )
        rows = [[("‹ Назад", "back_manage"), home_btn()]]
    else:
        n_run   = sum(1 for t in tasks if t["status"] == "running")
        n_paus  = sum(1 for t in tasks if t["status"] == "paused")
        text_lines = [
            f"💘 <b>Управление XO</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"Задач: <b>{len(tasks)}</b>  ·  "
            f"▶️ активных: {n_run}  ·  ⏸ пауза: {n_paus}\n"
        ]
        rows = []
        for t in tasks[:30]:
            ph = t["phone"]
            st = t["status"]
            nxt = time.strftime("%d.%m %H:%M",
                                time.localtime(t["next_run"] or 0))
            is_paused = ph in store.xo_liking_paused
            paused_icon = "⏸" if is_paused else "▶️"
            text_lines.append(f"{paused_icon} {ph} — {st}  /  next: {nxt}")
            rows.append([
                ((("▶️ Resume" if is_paused else "⏸ Pause"),
                  f"mng_xo_pp:{ph}")),
                ("🛑 Стоп", f"mng_xo_stop:{ph}"),
                ("🗑 Удалить", f"mng_xo_del:{ph}"),
            ])
        rows.append([("‹ Назад", "back_manage"), home_btn()])
        text = "\n".join(text_lines)
    if edit_msg:
        try:
            await edit_msg.edit_text(text, reply_markup=kb(*rows))
            return
        except TelegramBadRequest:
            pass
    await bot.send_message(chat_id, text, reply_markup=kb(*rows))


@dp.callback_query(F.data.startswith("mng_xo_pp:"))
async def cb_mng_xo_pp(cb: CallbackQuery):
    ph = cb.data.split(":", 1)[1]
    if ph in store.xo_liking_paused:
        store.xo_liking_paused.discard(ph)
        await db.db_update_xo_task(ph, status="running")
        await cb.answer("▶️ Возобновлён.")
    else:
        store.xo_liking_paused.add(ph)
        await db.db_update_xo_task(ph, status="paused")
        await cb.answer("⏸ На паузе.")
    await _xo_manage_render(cb.message.chat.id, cb.from_user.id,
                            edit_msg=cb.message)


@dp.callback_query(F.data.startswith("mng_xo_stop:"))
async def cb_mng_xo_stop(cb: CallbackQuery):
    ph = cb.data.split(":", 1)[1]
    t = store.xo_liking_tasks.pop(ph, None)
    if t and not t.done():
        t.cancel()
    await db.db_update_xo_task(ph, status="stopped")
    await cb.answer("🛑 Остановлен.")
    await _xo_manage_render(cb.message.chat.id, cb.from_user.id,
                            edit_msg=cb.message)


@dp.callback_query(F.data.startswith("mng_xo_del:"))
async def cb_mng_xo_del(cb: CallbackQuery):
    ph = cb.data.split(":", 1)[1]
    t = store.xo_liking_tasks.pop(ph, None)
    if t and not t.done():
        t.cancel()
    await db.db_delete_xo_task(ph)
    await cb.answer("🗑 Удалена.")
    await _xo_manage_render(cb.message.chat.id, cb.from_user.id,
                            edit_msg=cb.message)


# =================================================================
# ── СЕКЦИЯ: 🛑 ОТМЕНА РЕГИСТРАЦИИ (LDV / XO) ──
# =================================================================
# Логика: пользователь добавляет номера в store.ldv_reg_cancel или
# store.xo_reg_cancel. register_one_ldv / register_xo_resumable / etc.
# проверяют этот set между шагами и прерываются.
#
# Главное меню отмены:
@dp.callback_query(F.data == "mng_regcancel")
async def cb_mng_regcancel(cb: CallbackQuery):
    uid = cb.from_user.id
    pending_ldv = len(store.ldv_reg_cancel)
    pending_xo = len(store.xo_reg_cancel)
    active_ldv_targets = (store.ldv_data.get(uid) or {}).get("targets") or []
    active_xo_targets = (store.xo_data.get(uid) or {}).get("targets") or []
    text = (
        "🛑 <b>Отмена регистрации</b>\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        f"🤖  LDV — активная партия:  <b>{len(active_ldv_targets)}</b>\n"
        f"💘  XO  — активная партия:  <b>{len(active_xo_targets)}</b>\n\n"
        f"В очереди на отмену:\n"
        f"    ▸ LDV: <b>{pending_ldv}</b>\n"
        f"    ▸ XO:  <b>{pending_xo}</b>"
    )
    await cb.message.edit_text(
        text,
        reply_markup=kb(
            [("🤖 Отменить LDV", "rc_ldv"),
             ("💘 Отменить XO", "rc_xo")],
            [("♻️ Очистить стоп-лист", "rc_clear")],
            [("‹ Назад", "back_manage"), home_btn()],
        ),
    )
    await cb.answer()


@dp.callback_query(F.data == "rc_clear")
async def cb_rc_clear(cb: CallbackQuery):
    n = len(store.ldv_reg_cancel) + len(store.xo_reg_cancel)
    store.ldv_reg_cancel.clear()
    store.xo_reg_cancel.clear()
    await cb.answer(f"Очищено: {n}", show_alert=True)
    # cb_mng_regcancel не использует cb.data — вызываем напрямую
    await cb_mng_regcancel(cb)


# ── ЛДВ-отмена: меню вариантов ──
@dp.callback_query(F.data == "rc_ldv")
async def cb_rc_ldv(cb: CallbackQuery):
    await cb.message.edit_text(
        "🛑 <b>Отмена регистрации LDV</b>\n\n"
        "Выберите диапазон аккаунтов для отмены:",
        reply_markup=kb(
            [("📋 Все аккаунты", "rc_ldv_all")],
            [("📁 По группе", "rc_ldv_grp"),
             ("✏️ По номерам", "rc_ldv_man")],
            [("‹ Назад", "mng_regcancel"), home_btn()],
        ),
    )
    await cb.answer()


@dp.callback_query(F.data == "rc_ldv_all")
async def cb_rc_ldv_all(cb: CallbackQuery):
    uid = cb.from_user.id
    targets = (store.ldv_data.get(uid) or {}).get("targets") or []
    accs = await db.db_get_accounts_by_owner(uid)
    # отменяем ВСЕ — и активную партию, и про запас все аккаунты владельца
    n = 0
    for ph in targets:
        store.ldv_reg_cancel.add(ph); n += 1
    for a in accs:
        store.ldv_reg_cancel.add(a["phone"])
    await cb.answer(f"Отмена принята: {n} активных, "
                    f"{len(accs)} аккаунтов в стоп-листе.",
                    show_alert=True)


@dp.callback_query(F.data == "rc_ldv_grp")
async def cb_rc_ldv_grp(cb: CallbackQuery):
    uid = cb.from_user.id
    groups = await db.db_get_groups_by_owner(uid)
    if not groups:
        return await cb.answer("Групп нет.", show_alert=True)
    _grp_index_cache[uid] = groups
    rows = [[(f"📁 {g}", f"rc_ldv_gi:{i}")] for i, g in enumerate(groups[:30])]
    rows.append([("‹ Назад", "rc_ldv")])
    await cb.message.edit_text("📁 Выберите группу для отмены ЛДВ:",
                               reply_markup=kb(*rows))
    await cb.answer()


@dp.callback_query(F.data.startswith("rc_ldv_gi:"))
async def cb_rc_ldv_gi(cb: CallbackQuery):
    uid = cb.from_user.id
    gi = int(cb.data.split(":", 1)[1])
    groups = _grp_index_cache.get(uid, [])
    if not (0 <= gi < len(groups)):
        return await cb.answer("Bad", show_alert=True)
    accs = await db.db_get_accounts_by_group(uid, groups[gi])
    n = 0
    for a in accs:
        store.ldv_reg_cancel.add(a["phone"]); n += 1
    await cb.answer(f"🛑 В группе «{groups[gi]}» отмечено: {n}",
                    show_alert=True)


@dp.callback_query(F.data == "rc_ldv_man")
async def cb_rc_ldv_man(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "🛑 Пришлите номера для отмены ЛДВ-регистрации "
        "(через запятую или с новой строки):",
        validator=lambda t: any(
            validate_phone(tok) for tok in re.split(r"[,\n;\s]+", t) if tok
        ),
        error_msg="❌ Не нашёл валидных номеров. Формат: +79991112233",
    )
    if not raw:
        return
    n = 0
    for tok in re.split(r"[,\n;\s]+", raw):
        p = validate_phone(tok)
        if p:
            store.ldv_reg_cancel.add(p)
            n += 1
    await cb.message.answer(f"🛑 В стоп-лист ЛДВ добавлено: <b>{n}</b>")


# ── XO-отмена: меню вариантов ──
@dp.callback_query(F.data == "rc_xo")
async def cb_rc_xo(cb: CallbackQuery):
    await cb.message.edit_text(
        "🛑 <b>Отмена регистрации XO</b>\n\n"
        "Выберите диапазон аккаунтов для отмены:",
        reply_markup=kb(
            [("📋 Все аккаунты", "rc_xo_all")],
            [("📁 По группе", "rc_xo_grp"),
             ("✏️ По номерам", "rc_xo_man")],
            [("‹ Назад", "mng_regcancel"), home_btn()],
        ),
    )
    await cb.answer()


@dp.callback_query(F.data == "rc_xo_all")
async def cb_rc_xo_all(cb: CallbackQuery):
    uid = cb.from_user.id
    targets = (store.xo_data.get(uid) or {}).get("targets") or []
    accs = await db.db_get_accounts_by_owner(uid)
    n = 0
    for ph in targets:
        store.xo_reg_cancel.add(ph); n += 1
    for a in accs:
        store.xo_reg_cancel.add(a["phone"])
    await cb.answer(f"Отмена принята: {n} активных, "
                    f"{len(accs)} аккаунтов в стоп-листе.",
                    show_alert=True)


@dp.callback_query(F.data == "rc_xo_grp")
async def cb_rc_xo_grp(cb: CallbackQuery):
    uid = cb.from_user.id
    groups = await db.db_get_groups_by_owner(uid)
    if not groups:
        return await cb.answer("Групп нет.", show_alert=True)
    _grp_index_cache[uid] = groups
    rows = [[(f"📁 {g}", f"rc_xo_gi:{i}")] for i, g in enumerate(groups[:30])]
    rows.append([("‹ Назад", "rc_xo")])
    await cb.message.edit_text("📁 Выберите группу для отмены XO:",
                               reply_markup=kb(*rows))
    await cb.answer()


@dp.callback_query(F.data.startswith("rc_xo_gi:"))
async def cb_rc_xo_gi(cb: CallbackQuery):
    uid = cb.from_user.id
    gi = int(cb.data.split(":", 1)[1])
    groups = _grp_index_cache.get(uid, [])
    if not (0 <= gi < len(groups)):
        return await cb.answer("Bad", show_alert=True)
    accs = await db.db_get_accounts_by_group(uid, groups[gi])
    n = 0
    for a in accs:
        store.xo_reg_cancel.add(a["phone"]); n += 1
    await cb.answer(f"🛑 В группе «{groups[gi]}» отмечено: {n}",
                    show_alert=True)


@dp.callback_query(F.data == "rc_xo_man")
async def cb_rc_xo_man(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "🛑 Пришлите номера для отмены XO-регистрации "
        "(через запятую или с новой строки):",
        validator=lambda t: any(
            validate_phone(tok) for tok in re.split(r"[,\n;\s]+", t) if tok
        ),
        error_msg="❌ Не нашёл валидных номеров. Формат: +79991112233",
    )
    if not raw:
        return
    n = 0
    for tok in re.split(r"[,\n;\s]+", raw):
        p = validate_phone(tok)
        if p:
            store.xo_reg_cancel.add(p)
            n += 1
    await cb.message.answer(f"🛑 В стоп-лист XO добавлено: <b>{n}</b>")


# =================================================================
# ── СЕКЦИЯ: 👑 АДМИН-ПАНЕЛЬ ──
# =================================================================
@dp.callback_query(F.data == "adm_wl")
async def cb_adm_wl(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    rows_db = await db.db_whitelist_get_all()
    text_lines = [
        f"👥 <b>Whitelist</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Пользователей: <b>{len(rows_db)}</b>\n"
    ]
    if rows_db:
        for r in rows_db[:30]:
            uname = f" — @{r['username']}" if r.get("username") else ""
            text_lines.append(f"• <code>{r['user_id']}</code>{uname}")
        if len(rows_db) > 30:
            text_lines.append(f"…ещё {len(rows_db) - 30}")
    else:
        text_lines.append("— список пуст —")
    await cb.message.edit_text(
        "\n".join(text_lines),
        reply_markup=kb(
            [("➕ Добавить", "adm_wl_add"),
             ("🗑 Удалить", "adm_wl_del")],
            [("‹ Назад", "adm_back"), home_btn()],
        ),
    )
    await cb.answer()


@dp.callback_query(F.data == "adm_back")
async def cb_adm_back(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    await cb.message.edit_text(
        "👑 <b>Администрирование</b>\n\n"
        "Управление доступом пользователей, "
        "глобальными прокси и просмотр всех аккаунтов.",
        reply_markup=kb(
            [("👥 Whitelist", "adm_wl"),
             ("👮 Администраторы", "adm_admins")],
            [("🌐 Глобальные прокси", "gpx_list"),
             ("📋 Все аккаунты", "adm_all_accs")],
            [home_btn()],
        ),
    )
    await cb.answer()


@dp.callback_query(F.data == "adm_wl_add")
async def cb_adm_wl_add(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, cb.from_user.id,
        "👤 Пришлите user_id или @username для добавления в whitelist:",
        validator=lambda t: t.strip().startswith("@") or t.strip().lstrip("-").isdigit(),
        error_msg="❌ Введите числовой user_id или @username.",
    )
    if not raw:
        return
    raw = raw.strip()
    user_id = None
    username = ""
    if raw.startswith("@"):
        username = raw[1:]
        try:
            chat = await bot.get_chat(raw)
            user_id = chat.id
            if chat.username:
                username = chat.username
        except Exception:
            return await cb.message.answer("❌ Не нашёл такого пользователя.")
    else:
        try:
            user_id = int(raw)
        except Exception:
            return await cb.message.answer("❌ user_id должен быть числом.")
    await db.db_whitelist_add(user_id, username)
    await cb.message.answer(
        f"✅ Добавлен: <code>{user_id}</code>"
        + (f" (@{username})" if username else "")
    )


@dp.callback_query(F.data == "adm_wl_del")
async def cb_adm_wl_del(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, cb.from_user.id,
        "🗑 Пришлите user_id для удаления из whitelist:",
        validator=lambda t: t.strip().lstrip("-").isdigit(),
        error_msg="❌ user_id должен быть числом.",
    )
    if not raw:
        return
    user_id = int(raw.strip())
    await db.db_whitelist_remove(user_id)
    await cb.message.answer(f"🗑 Удалён: <code>{user_id}</code>")


@dp.callback_query(F.data == "adm_admins")
async def cb_adm_admins(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    rows_db = await db.db_admins_get_all()
    text_lines = [
        f"👮 <b>Администраторы</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Всего: <b>{len(rows_db)}</b>\n"
    ]
    if rows_db:
        for r in rows_db:
            text_lines.append(f"• <code>{r['user_id']}</code>")
    else:
        text_lines.append("— список пуст —")
    await cb.message.edit_text(
        "\n".join(text_lines),
        reply_markup=kb(
            [("➕ Добавить", "adm_admins_add"),
             ("🗑 Удалить", "adm_admins_del")],
            [("‹ Назад", "adm_back"), home_btn()],
        ),
    )
    await cb.answer()


@dp.callback_query(F.data == "adm_admins_add")
async def cb_adm_admins_add(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, cb.from_user.id,
        "👮 user_id нового админа:",
        validator=lambda t: t.strip().lstrip("-").isdigit(),
        error_msg="❌ user_id должен быть числом.",
    )
    if not raw:
        return
    user_id = int(raw.strip())
    await db.db_admins_add(user_id)
    await cb.message.answer(f"✅ Админ: <code>{user_id}</code>")


@dp.callback_query(F.data == "adm_admins_del")
async def cb_adm_admins_del(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, cb.from_user.id,
        "🗑 user_id админа для удаления:",
        validator=lambda t: t.strip().lstrip("-").isdigit(),
        error_msg="❌ user_id должен быть числом.",
    )
    if not raw:
        return
    user_id = int(raw.strip())
    await db.db_admins_remove(user_id)
    await cb.message.answer(f"🗑 Админ удалён: <code>{user_id}</code>")


@dp.callback_query(F.data == "adm_all_accs")
async def cb_adm_all_accs(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    rows_db = await db.db_get_all_accounts()
    if not rows_db:
        text = (
            "📋 <b>Все аккаунты</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "Аккаунтов нет."
        )
    else:
        by_owner: Dict[int, List[Dict[str, Any]]] = {}
        for a in rows_db:
            by_owner.setdefault(a.get("owner_id") or 0, []).append(a)
        lines = [
            f"📋 <b>Все аккаунты</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"Всего: <b>{len(rows_db)}</b>  ·  "
            f"Пользователей: <b>{len(by_owner)}</b>"
        ]
        for owner_id, accs in by_owner.items():
            lines.append(f"\n👤 <code>{owner_id}</code> — {len(accs)} аккаунтов:")
            for a in accs[:15]:
                grp = f" 📁{a['grp']}" if a.get("grp") else ""
                lines.append(
                    f"  • {a['phone']} (@{a.get('username') or '—'}){grp}"
                )
            if len(accs) > 15:
                lines.append(f"  …ещё {len(accs) - 15}")
        text = "\n".join(lines)
    if len(text) > 3800:
        text = text[:3800] + "\n…(обрезано)…"
    await cb.message.edit_text(
        text,
        reply_markup=kb([("‹ Назад", "adm_back"), home_btn()]),
    )
    await cb.answer()


# =================================================================
# ── СЕКЦИЯ: 🔄 ПЕРЕДАЧА АККАУНТОВ ──
# =================================================================

@dp.callback_query(F.data == "acc_transfer")
async def cb_acc_transfer(cb: CallbackQuery):
    uid = cb.from_user.id
    accs = await db.db_get_accounts_by_owner(uid)
    if not accs:
        return await cb.answer("У вас нет аккаунтов для передачи.",
                               show_alert=True)
    await cb.answer()
    await _send_target_picker(
        cb.message.chat.id, "trf_t",
        "🔄 <b>Передача аккаунтов</b>\n\n"
        "Выберите аккаунты, которые хотите передать другому пользователю.\n\n"
        "⚠️ При передаче:\n"
        "  • Группа и личный прокси очистятся\n"
        "  • Все задачи LDV/XO/автоответы сбросятся\n"
        "  • Вы получите уведомление когда получатель примет",
    )


async def _trf_show_preview(cb: CallbackQuery, uid: int,
                            phones: List[str]) -> None:
    """Сохраняет выбор и показывает превью + кнопку создания ссылки."""
    _transfer_pending[uid] = phones
    preview = "\n".join(f"  • <code>{ph}</code>" for ph in phones[:20])
    if len(phones) > 20:
        preview += f"\n  …и ещё {len(phones) - 20}"
    await bot.send_message(
        cb.message.chat.id,
        f"🔄 <b>Передача аккаунтов</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Выбрано: <b>{len(phones)}</b>\n\n"
        f"{preview}\n\n"
        f"Создать одноразовую ссылку передачи?",
        reply_markup=kb(
            [("🔗 Создать ссылку", "trf_create")],
            [("❌ Отмена", "action_cancel")],
        ),
    )


async def _render_trf_selector(cb: CallbackQuery, uid: int,
                               page: int) -> None:
    """Рисует интерактивный список аккаунтов с чекбоксами."""
    accs = await db.db_get_accounts_by_owner(uid)
    if not accs:
        await cb.message.answer("❌ У вас нет аккаунтов.")
        return

    selected = _trf_selection.setdefault(uid, set())
    total = len(accs)
    pages = max(1, (total + _TRF_SEL_PER_PAGE - 1) // _TRF_SEL_PER_PAGE)
    page = max(0, min(page, pages - 1))
    chunk = accs[page * _TRF_SEL_PER_PAGE:(page + 1) * _TRF_SEL_PER_PAGE]

    rows = []
    for a in chunk:
        ph = a["phone"]
        un = f" (@{a['username']})" if a.get("username") else ""
        grp = f" 📁{a['grp']}" if a.get("grp") else ""
        icon = "✅" if ph in selected else "⬜"
        rows.append([(f"{icon} {ph}{un}{grp}", f"trf_tog:{ph}:{page}")])

    nav = []
    if page > 0:
        nav.append(("◀️", f"trf_sel:{page - 1}"))
    nav.append((f"{page + 1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append(("▶️", f"trf_sel:{page + 1}"))
    if nav:
        rows.append(nav)

    n_sel = len(selected)
    if n_sel > 0:
        rows.append([(f"✅ Подтвердить ({n_sel} выбрано)", "trf_sel_confirm")])
    rows.append([("❌ Отмена", "action_cancel")])

    text = (
        f"📋 <b>Выбор аккаунтов</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Всего: <b>{total}</b>  ·  Выбрано: <b>{n_sel}</b>  ·  "
        f"Стр. {page + 1}/{pages}\n\n"
        f"Нажмите на аккаунт чтобы отметить/снять:"
    )
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))


@dp.callback_query(F.data.startswith("trf_t:"))
async def cb_trf_t(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    mode = parts[1]

    if mode == "man":
        # Показываем выбор метода: вручную / из списка
        _trf_selection.pop(uid, None)
        await cb.answer()
        try:
            await cb.message.edit_text(
                "✏️ <b>Ручной выбор аккаунтов</b>\n\n"
                "Как хотите выбрать?",
                reply_markup=kb(
                    [("✏️ Ввести номера", "trf_man_type")],
                    [("📋 Выбрать из списка", "trf_sel:0")],
                    [("❌ Отмена", "action_cancel")],
                ),
            )
        except TelegramBadRequest:
            await cb.message.answer(
                "✏️ <b>Ручной выбор аккаунтов</b>\n\nКак хотите выбрать?",
                reply_markup=kb(
                    [("✏️ Ввести номера", "trf_man_type")],
                    [("📋 Выбрать из списка", "trf_sel:0")],
                    [("❌ Отмена", "action_cancel")],
                ),
            )
        return

    targets: List[Dict[str, Any]] = []
    if mode == "all":
        targets = await _resolve_targets_all(uid)
        await cb.answer()
    elif mode == "grp" and len(parts) == 2:
        await cb.answer()
        return await _send_groups_picker(uid, cb.message.chat.id, "trf_t")
    elif mode == "gi":
        await cb.answer()
        targets = await _resolve_targets_group(uid, int(parts[2]))
    else:
        return await cb.answer("Bad", show_alert=True)

    if not targets:
        return await bot.send_message(uid, "❌ Аккаунтов для передачи нет.")
    await _trf_show_preview(cb, uid, [a["phone"] for a in targets])


@dp.callback_query(F.data == "trf_man_type")
async def cb_trf_man_type(cb: CallbackQuery):
    """Ввод номеров вручную текстом."""
    uid = cb.from_user.id
    await cb.answer()
    targets = await _resolve_targets_manual(uid, cb.message.chat.id)
    if not targets:
        return await bot.send_message(uid, "❌ Аккаунтов не найдено.")
    await _trf_show_preview(cb, uid, [a["phone"] for a in targets])


@dp.callback_query(F.data.startswith("trf_sel:"))
async def cb_trf_sel(cb: CallbackQuery):
    """Переход между страницами списка."""
    uid = cb.from_user.id
    try:
        page = int(cb.data.split(":", 1)[1])
    except Exception:
        page = 0
    await cb.answer()
    await _render_trf_selector(cb, uid, page)


@dp.callback_query(F.data.startswith("trf_tog:"))
async def cb_trf_tog(cb: CallbackQuery):
    """Переключить галочку у аккаунта."""
    uid = cb.from_user.id
    parts = cb.data.split(":")
    # формат: trf_tog:{phone}:{page}  (phone может содержать '+', но не ':')
    phone = parts[1]
    try:
        page = int(parts[2])
    except Exception:
        page = 0

    selected = _trf_selection.setdefault(uid, set())
    if phone in selected:
        selected.discard(phone)
    else:
        selected.add(phone)

    await cb.answer()
    await _render_trf_selector(cb, uid, page)


@dp.callback_query(F.data == "trf_sel_confirm")
async def cb_trf_sel_confirm(cb: CallbackQuery):
    """Подтвердить выбор из списка."""
    uid = cb.from_user.id
    selected = _trf_selection.pop(uid, set())
    if not selected:
        return await cb.answer("Не выбрано ни одного аккаунта.",
                               show_alert=True)
    await cb.answer()
    await _trf_show_preview(cb, uid, sorted(selected))


@dp.callback_query(F.data == "trf_create")
async def cb_trf_create(cb: CallbackQuery):
    uid = cb.from_user.id
    phones = _transfer_pending.pop(uid, None)
    if not phones:
        return await cb.answer("Сессия передачи устарела — начните заново.",
                               show_alert=True)

    token = secrets.token_urlsafe(16)
    await db.db_transfer_create(token, uid, phones)

    bot_un = _bot_username or "бот"
    link = f"https://t.me/{bot_un}?start=tr_{token}"
    await cb.answer()
    try:
        await cb.message.edit_text(
            f"🔗 <b>Ссылка передачи создана</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"Аккаунтов: <b>{len(phones)}</b>\n\n"
            f"Отправьте получателю эту ссылку:\n"
            f"<code>{link}</code>\n\n"
            f"⚠️ Ссылка <b>одноразовая</b> — после принятия сгорает.\n"
            f"Если получатель откажется — аккаунты остаются у вас.",
            reply_markup=kb([home_btn()]),
        )
    except Exception:
        await cb.message.answer(
            f"🔗 <b>Ссылка передачи:</b>\n<code>{link}</code>",
            reply_markup=kb([home_btn()]),
        )


async def _handle_transfer_incoming(msg: Message, uid: int,
                                    token: str) -> None:
    """
    Вызывается когда получатель переходит по ссылке t.me/БОТ?start=tr_TOKEN.
    Показывает превью и кнопки «Принять» / «Отклонить».
    """
    rec = await db.db_transfer_get(token)
    if not rec:
        await msg.answer(
            "❌ Ссылка передачи недействительна или уже использована."
        )
        await restore_main_menu(bot, msg.chat.id, uid)
        return

    from_uid = rec["from_uid"]
    phones: List[str] = rec["phones"]

    if from_uid == uid:
        await msg.answer("⚠️ Нельзя передать аккаунты самому себе.")
        await restore_main_menu(bot, msg.chat.id, uid)
        return

    # Проверяем, какие аккаунты ещё принадлежат отправителю
    valid: List[str] = []
    for ph in phones:
        a = await db.db_get_account(ph)
        if a and a.get("owner_id") == from_uid:
            valid.append(ph)

    if not valid:
        await db.db_transfer_delete(token)
        await msg.answer(
            "❌ Все аккаунты из этой ссылки уже недоступны\n"
            "(удалены или ранее переданы)."
        )
        await restore_main_menu(bot, msg.chat.id, uid)
        return

    preview = "\n".join(f"  • <code>{ph}</code>" for ph in valid[:20])
    if len(valid) > 20:
        preview += f"\n  …и ещё {len(valid) - 20}"

    await msg.answer(
        f"📦 <b>Вам предлагают аккаунты</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Количество: <b>{len(valid)}</b>\n\n"
        f"{preview}\n\n"
        f"⚠️ Все задачи LDV/XO/автоответы будут сброшены.\n"
        f"Принять передачу?",
        reply_markup=kb(
            [("✅ Принять", f"trf_accept:{token}")],
            [("❌ Отклонить", f"trf_decline:{token}")],
        ),
    )


@dp.callback_query(F.data.startswith("trf_accept:"))
async def cb_trf_accept(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    uid = cb.from_user.id

    rec = await db.db_transfer_get(token)
    if not rec:
        await cb.answer("❌ Ссылка уже использована.", show_alert=True)
        try:
            await cb.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    from_uid: int = rec["from_uid"]
    phones: List[str] = rec["phones"]

    # Одноразовый токен — удаляем немедленно, чтобы исключить двойное нажатие
    await db.db_transfer_delete(token)
    await cb.answer("⏳ Принимаю аккаунты…")

    ok: List[str] = []
    skipped: List[str] = []

    for ph in phones:
        a = await db.db_get_account(ph)
        if not a or a.get("owner_id") != from_uid:
            skipped.append(ph)  # удалён или уже передан другому
            continue

        # ── Останавливаем все активные процессы ──
        try:
            await ar_manager.stop(ph)
        except Exception:
            pass
        xo_task = store.xo_liking_tasks.pop(ph, None)
        if xo_task and not xo_task.done():
            xo_task.cancel()
        # Сигнализируем LDV-задаче остановиться
        store.cancelled_phones.add(ph)
        store.paused_phones.discard(ph)

        # ── Передаём (меняет owner_id, чистит grp/proxy, удаляет задачи) ──
        await db.db_transfer_account(ph, uid)
        ok.append(ph)

    # Для аккаунтов без активного LDV-лупа чистим cancelled_phones сами
    for ph in ok:
        if ph not in store.current_liking_phones:
            store.cancelled_phones.discard(ph)

    # ── Ответ получателю ──
    ok_text = "\n".join(f"  ✅ <code>{ph}</code>" for ph in ok)
    skip_text = "\n".join(f"  ❌ <code>{ph}</code>" for ph in skipped)
    result_text = (
        f"📦 <b>Передача завершена</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Принято: <b>{len(ok)}</b>"
    )
    if ok_text:
        result_text += f"\n{ok_text}"
    if skipped:
        result_text += (
            f"\n\nНедоступных (удалены/уже переданы): <b>{len(skipped)}</b>\n"
            f"{skip_text}"
        )
    try:
        await cb.message.edit_text(result_text, reply_markup=None)
    except Exception:
        await cb.message.answer(result_text)
    await restore_main_menu(bot, cb.message.chat.id, uid)

    # ── Уведомляем отправителя ──
    try:
        notif = (
            f"📦 <b>Передача принята</b>\n"
            f"Пользователь <code>{uid}</code> принял "
            f"<b>{len(ok)}</b> аккаунт(ов)."
        )
        if skipped:
            notif += f"\n⚠️ Недоступных: {len(skipped)}"
        await notify_owner(from_uid, notif)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("trf_decline:"))
async def cb_trf_decline(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    await db.db_transfer_delete(token)
    await cb.answer("Передача отклонена.")
    try:
        await cb.message.edit_text("❌ Передача аккаунтов отклонена.")
    except Exception:
        pass
    await restore_main_menu(bot, cb.message.chat.id, cb.from_user.id)


# =================================================================
# ── СЕКЦИЯ: BOOTSTRAP ──
# =================================================================
async def _bootstrap_autoreplies():
    """Поднять менеджер автоответов для всех enabled пар (owner, phone)."""
    rows = await db.db_ar_get_enabled_phones()
    started = 0
    for r in rows:
        owner_id = r.get("owner_id")
        phone = r.get("phone")
        custom = r.get("custom_text")
        if not phone or not owner_id:
            continue
        proxy = await get_proxy_for_account(phone, owner_id)
        try:
            ok = await ar_manager.start(phone, owner_id, proxy,
                                        custom_text=custom)
            if ok:
                started += 1
        except Exception as e:
            log.warning("bootstrap autoreply %s: %s", phone, e)
    log.info("Autoreplies started: %d", started)


async def _bootstrap_dirs():
    os.makedirs(config.SESSIONS_DIR, exist_ok=True)
    os.makedirs(config.TEMP_DIR, exist_ok=True)


async def _notify_admins(text: str) -> None:
    """Разослать text всем админам (для health-check глобал-прокси)."""
    try:
        admins = await db.db_admins_get_all()
    except Exception as e:
        log.warning("notify_admins: get list: %s", e)
        return
    for a in admins:
        uid = a.get("user_id")
        if uid:
            try:
                await bot.send_message(uid, text)
            except Exception as e:
                log.warning("notify_admins(%s): %s", uid, e)


async def _on_startup():
    global _bot_username
    await _bootstrap_dirs()
    await db.init_db()
    try:
        me = await bot.get_me()
        _bot_username = me.username or ""
    except Exception as e:
        log.warning("_on_startup: get_me() failed: %s", e)
    # уведомлятель для AutoreplyManager
    ar_manager.set_notifier(notify_owner)
    # уведомлятель для health-check глобал-прокси (alive→dead)
    set_admin_notifier(_notify_admins)
    # фоновые таски
    asyncio.create_task(run_health_check_loop())
    asyncio.create_task(ldv_scheduler(store, notify_func=notify_owner))
    asyncio.create_task(xo_liking_scheduler(store, notify_func=notify_owner))
    # автоответы из БД
    asyncio.create_task(_bootstrap_autoreplies())

    # сторож сессий: удаляет .session-файлы разлогиненных аккаунтов
    async def _watchdog_notify(phone: str) -> None:
        """Уведомить владельца аккаунта об удалении сессии."""
        try:
            acc = await db.db_get_account(phone)
            uid = acc["owner_id"] if acc else None
            if uid:
                await notify_owner(uid,
                    f"⚠️ Аккаунт <code>{phone}</code> вышел из системы — "
                    f"сессия удалена.")
        except Exception as e:
            log.debug("watchdog_notify %s: %s", phone, e)

    asyncio.create_task(
        _session_watchdog(config.SESSIONS_DIR,
                          interval=120,
                          notify_func=_watchdog_notify)
    )
    log.info("Менеджер запущен.")


async def _on_shutdown():
    log.info("Останавливаю менеджер…")
    try:
        await ar_manager.stop_all()
    except Exception:
        pass
    # отменим XO-таски
    for ph, t in list(store.xo_liking_tasks.items()):
        store.xo_liking_tasks.pop(ph, None)
        if not t.done():
            t.cancel()
    # отключим всех клиентов из пула
    for ph in _client_pool.all_phones():
        await _client_pool.remove(ph)
    try:
        await bot.session.close()
    except Exception:
        pass


# =================================================================
# ── СЕКЦИЯ: MAIN ──
# =================================================================
async def main():
    dp.startup.register(_on_startup)
    dp.shutdown.register(_on_shutdown)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Остановлено пользователем.")
