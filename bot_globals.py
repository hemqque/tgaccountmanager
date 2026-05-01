# -*- coding: utf-8 -*-
"""
bot_globals.py — Общие объекты, разделяемое состояние и вспомогательные
функции бота.  Все handler-модули импортируют отсюда bot, dp, store,
task_queue, ar_manager, вспомогательные функции и разделяемые словари.
"""

import logging
import random
from typing import Any, Dict, List, Optional, Tuple

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from telethon import TelegramClient

import config
import client_pool as _client_pool
from store import Store
from task_queue import TaskQueue
from autoreply import AutoreplyManager
from global_proxy import proxy_to_telethon, get_proxy_for_account

log = logging.getLogger("main")

# ─── Основные объекты ────────────────────────────────────────────────
# Если BOT_PROXY задан в .env — используем его для HTTP-сессии aiogram.
# Поддерживаются http:// и socks5:// (для socks5 нужен пакет aiohttp-socks).
_bot_session: Optional[AiohttpSession] = None
if config.BOT_PROXY:
    try:
        _bot_session = AiohttpSession(proxy=config.BOT_PROXY)
        log.info("Bot session proxy: %s", config.BOT_PROXY)
    except Exception as _e:
        log.warning("Failed to create bot proxy session: %s", _e)

bot = Bot(
    token=config.BOT_TOKEN,
    session=_bot_session,  # None → дефолтная сессия без прокси
    default=DefaultBotProperties(parse_mode="HTML"),
)
dp = Dispatcher()
store = Store()
task_queue = TaskQueue(max_concurrent=config.MAX_CONCURRENT_TASKS)
ar_manager = AutoreplyManager()

# ─── Разделяемые словари состояния ───────────────────────────────────
# Добавление аккаунтов: uid -> {"phone": str, "chat_id": int}
_signin_sessions: Dict[int, Dict[str, Any]] = {}
# Импорт TData/Session: uid -> {"chat_id": int, "local_path"?: str, ...}
_tdata_sessions: Dict[int, Dict[str, Any]] = {}
# Кэш групп для коротких callback'ов по индексу: uid -> [group1, group2, ...]
_grp_index_cache: Dict[int, List[str]] = {}
# Юзернейм бота (заполняется при старте)
_bot_username: str = ""

# Передача аккаунтов
_transfer_pending: Dict[int, List[str]] = {}   # uid -> список телефонов
_trf_selection: Dict[int, set] = {}            # uid -> выбранные телефоны
_TRF_SEL_PER_PAGE: int = 6

# Универсальный ручной выборщик аккаунтов
_man_sel_ctx: Dict[int, str] = {}   # uid -> контекст ("mass_t", "ldvr_t", …)
_man_selection: Dict[int, set] = {} # uid -> выбранные телефоны
_MAN_SEL_PER_PAGE: int = 6

# Генератор username
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


# ─── Вспомогательные функции ─────────────────────────────────────────
async def notify_owner(owner_id: int, text: str) -> None:
    """Послать сообщение пользователю-владельцу. Без падений."""
    try:
        await bot.send_message(owner_id, text)
    except Exception as e:
        log.warning("notify_owner(%s): %s", owner_id, e)


async def user_log(uid: int, text: str) -> None:
    """Если у пользователя включены логи — отправить «📋 <text>»."""
    import db
    try:
        s = await db.db_user_settings_get(uid)
        if s.get("logs_enabled"):
            await bot.send_message(uid, f"📋 {text}")
    except Exception:
        pass


async def get_or_create_account_client(
        phone: str, owner_id: Optional[int] = None
) -> Optional[TelegramClient]:
    """Возвращает подключённый Telethon-клиент для аккаунта phone."""
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
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t, callback_data=c) for (t, c) in row]
        for row in rows
    ])


def home_btn() -> Tuple[str, str]:
    return ("🏠 Главное меню", "action_cancel")
