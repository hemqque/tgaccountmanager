# -*- coding: utf-8 -*-
"""
utils.py — Различные мелкие помощники, используемые везде.

Содержит:
  • rand_sleep             — короткая случайная пауза.
  • validate_phone         — проверка формата телефона.
  • validate_proxy         — проверка строки прокси.
  • safe_delete_folder     — удаление каталога с осторожной обработкой ошибок.
  • restore_main_menu      — отправка главного меню (Reply keyboard).
  • auto_join_channels     — подписка на каналы из AUTO_JOIN_CHANNELS.
  • is_allowed             — проверка доступа (админ или whitelist).
  • ask_with_cancel        — диалоговый запрос ввода текста с кнопкой
                             «❌ Отмена», таймаут 180 с.
  • register_pending_text  — обработчик-роутер «текст пользователя →
                             ожидающий future» (вызывается из main.py).
"""

import asyncio
import os
import random
import re
import shutil
import logging
from typing import Optional, List, Dict

from aiogram import Bot, Router
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
)
from aiogram.filters import Command   # noqa: F401  (re-export-friendly)

import db
from config import AUTO_JOIN_CHANNELS, ASK_TIMEOUT, INITIAL_ADMIN_IDS
from global_proxy import parse_proxy_string

log = logging.getLogger("utils")


# ─────────────────────────────────────────────────────────────────
# Случайные паузы
# ─────────────────────────────────────────────────────────────────
async def rand_sleep(lo: float = 1.0, hi: float = 3.0) -> None:
    await asyncio.sleep(random.uniform(lo, hi))


# ─────────────────────────────────────────────────────────────────
# Валидаторы
# ─────────────────────────────────────────────────────────────────
_PHONE_RE = re.compile(r"^\+?\d{7,16}$")


def validate_phone(s: str) -> Optional[str]:
    """Возвращает нормализованный телефон '+123…' или None."""
    if not s:
        return None
    s = re.sub(r"[\s\-()]", "", s.strip())
    if _PHONE_RE.match(s):
        if not s.startswith("+"):
            s = "+" + s
        return s
    return None


def validate_proxy(s: str) -> bool:
    """True, если строка либо корректный SOCKS5-прокси, либо «нет/no/-/none»."""
    if not s:
        return False
    s = s.strip()
    if not s:
        return False
    if s.lower() in ("нет", "no", "none", "-", "без прокси"):
        return True
    return parse_proxy_string(s) is not None


# ─────────────────────────────────────────────────────────────────
# Файловые помощники
# ─────────────────────────────────────────────────────────────────
def safe_delete_folder(path: str) -> bool:
    """Удалить каталог, если есть. True, если получилось/нечего удалять."""
    try:
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
        return True
    except Exception as e:
        log.warning("safe_delete_folder %s: %s", path, e)
        return False


# ─────────────────────────────────────────────────────────────────
# Главное меню
# ─────────────────────────────────────────────────────────────────
def main_menu_keyboard(is_admin: bool) -> ReplyKeyboardMarkup:
    """ReplyKeyboard для главного меню, опционально с [👑 Админ]."""
    rows = [
        [KeyboardButton(text="⚙️ Аккаунты"),
         KeyboardButton(text="🤖 Автоматизация")],
        [KeyboardButton(text="📊 Управление"),
         KeyboardButton(text="📈 Прогресс")],
    ]
    if is_admin:
        rows.append([KeyboardButton(text="👑 Админ")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True,
                               is_persistent=True)


async def restore_main_menu(bot: Bot, chat_id: int, uid: int,
                            text: str = "Главное меню.") -> None:
    is_admin = await db.db_admins_check(uid)
    await bot.send_message(chat_id, text,
                           reply_markup=main_menu_keyboard(is_admin))


# ─────────────────────────────────────────────────────────────────
# Подписка на каналы (юзерботы)
# ─────────────────────────────────────────────────────────────────
async def auto_join_channels(client, log_func=None) -> None:
    """Подписать Telethon-клиент на каналы из AUTO_JOIN_CHANNELS."""
    for ch in AUTO_JOIN_CHANNELS:
        try:
            await asyncio.wait_for(client(_join(ch)), timeout=15)
            if log_func:
                try:
                    await _maybe_await(log_func(f"📺 Подписан на @{ch}"))
                except Exception:
                    pass
        except Exception as e:
            if log_func:
                try:
                    await _maybe_await(log_func(f"⚠️ @{ch}: {e}"))
                except Exception:
                    pass


def _join(channel: str):
    """Возвращает JoinChannelRequest. Импорт ленивый — чтобы utils.py
    можно было импортить без telethon на стадии CI/тестов."""
    from telethon.tl.functions.channels import JoinChannelRequest
    return JoinChannelRequest(channel)


async def _maybe_await(x):
    if hasattr(x, "__await__"):
        return await x
    return x


# ─────────────────────────────────────────────────────────────────
# Проверка доступа
# ─────────────────────────────────────────────────────────────────
async def is_allowed(user_id: int) -> bool:
    """True, если user_id в INITIAL_ADMIN_IDS, admins ИЛИ в whitelist.

    Захардкоженные начальные администраторы проверяются по памяти (без обращения
    к БД) — это исключает любые сбои доступа при временной недоступности БД.
    """
    # Быстрая проверка: начальные администраторы всегда допускаются
    if user_id in INITIAL_ADMIN_IDS:
        return True
    if await db.db_admins_check(user_id):
        return True
    if await db.db_whitelist_check(user_id):
        return True
    return False


# ─────────────────────────────────────────────────────────────────
# Диалоговый ввод с кнопкой «❌ Отмена»
# ─────────────────────────────────────────────────────────────────
# реестр ожидающих ответа диалогов: uid -> Future[str|None]
_pending_text: Dict[int, asyncio.Future] = {}

CANCEL_TEXT = "❌ Отмена"


async def ask_with_cancel(bot: Bot, chat_id: int, uid: int,
                          prompt: str,
                          timeout: float = ASK_TIMEOUT,
                          parse_mode: Optional[str] = None) -> Optional[str]:
    """
    Отправить prompt, ждать текстовый ответ в течение timeout секунд.
      • вернёт текст;
      • если пользователь прислал «отмена» или подобное → None;
      • если истёк таймаут → None.
    """
    # отменяем висящий запрос для этого uid, если был
    old = _pending_text.pop(uid, None)
    if old and not old.done():
        old.cancel()

    fut: asyncio.Future = asyncio.get_event_loop().create_future()
    _pending_text[uid] = fut

    try:
        await bot.send_message(
            chat_id, prompt,
            reply_markup=ReplyKeyboardRemove(),
            parse_mode=parse_mode,
        )
    except Exception as e:
        log.warning("ask_with_cancel send: %s", e)
        _pending_text.pop(uid, None)
        return None

    try:
        text = await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        text = None
    except asyncio.CancelledError:
        text = None
    finally:
        _pending_text.pop(uid, None)

    if text is None:
        return None
    if text.strip().lower() in ("❌ отмена", "отмена", "/cancel"):
        return None
    return text


async def ask_with_retry(
    bot: Bot,
    chat_id: int,
    uid: int,
    prompt: str,
    validator,
    error_msg: Optional[str] = None,
    max_attempts: int = 5,
    timeout: float = ASK_TIMEOUT,
    parse_mode: Optional[str] = None,
) -> Optional[str]:
    """
    Повторяет ask_with_cancel до max_attempts раз, пока validator(text) не вернёт True.
    Возвращает валидный текст или None при отмене/исчерпании попыток.
    """
    current_prompt = prompt
    for attempt in range(max_attempts):
        raw = await ask_with_cancel(bot, chat_id, uid, current_prompt,
                                    timeout=timeout, parse_mode=parse_mode)
        if raw is None:
            return None
        if validator(raw):
            return raw
        remaining = max_attempts - attempt - 1
        if remaining <= 0:
            return None
        err = error_msg or "❌ Неверный ввод."
        current_prompt = f"{err} Осталось попыток: {remaining}.\n\n{prompt}"
    return None


def register_pending_text(uid: int, text: str) -> bool:
    """
    Передать текст ожидающему ask_with_cancel(). Возвращает True, если
    был ожидающий и текст был отправлен (значит, исходное сообщение нужно
    «съесть» — не пускать в другие хендлеры).
    """
    fut = _pending_text.get(uid)
    if fut is not None and not fut.done():
        fut.set_result(text)
        return True
    return False


def has_pending(uid: int) -> bool:
    return uid in _pending_text


def cancel_pending_ask(uid: int) -> None:
    fut = _pending_text.pop(uid, None)
    if fut and not fut.done():
        fut.cancel()


# ─────────────────────────────────────────────────────────────────
# Универсальный аиограмный декоратор отлова текста для ask_with_cancel
# ─────────────────────────────────────────────────────────────────

# Тексты reply-кнопок главного меню — имеют высший приоритет:
# при их нажатии текущий pending-ввод отменяется, а не поглощается.
MENU_BUTTON_TEXTS: frozenset = frozenset({
    "⚙️ аккаунты", "🤖 автоматизация", "📊 управление",
    "📈 прогресс", "👑 админ", "🏠 главное меню",
})


def attach_pending_router(router: Router, store=None) -> None:
    """
    Главный роутер должен вызвать это, чтобы текстовые ответы пользователя
    автоматически попадали в ожидающие ask_with_cancel.
    Регистрирует обработчик ВЫСОКОГО приоритета — поэтому добавляйте
    этот router ПЕРВЫМ к диспетчеру.
    """
    @router.message()
    async def _catch_all(msg: Message):
        if not msg.from_user:
            return
        uid = msg.from_user.id
        text_lower = (msg.text or "").strip().lower()

        # Кнопки главного меню → отменяем pending-ввод и пропускаем
        # сообщение дальше к хендлерам разделов (не глотаем как ввод)
        if text_lower in MENU_BUTTON_TEXTS:
            cancel_pending_ask(uid)
            if store is not None:
                store.set_action(uid, None)
            return

        if not has_pending(uid):
            return  # пропустить — пусть обрабатывают другие хендлеры
        register_pending_text(uid, msg.text or "")
        return
