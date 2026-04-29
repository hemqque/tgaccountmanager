# -*- coding: utf-8 -*-
"""
account_setup.py — Первичная настройка только что авторизованного аккаунта.

setup_account(client, owner_id, log_func):
  1. Выставляет настройки приватности через Telethon raw API SetPrivacy:
       last_seen           → Nobody
       profile_photo       → Everybody
       forwards            → Nobody
       calls               → Nobody
       voice_messages      → Everybody
       phone_number        → Nobody
       chat_invite         → Everybody
  2. Если у аккаунта нет username — пытается сгенерировать его из 3 рандомных
     английских слов (3 попытки) и установить через UpdateUsernameRequest.
"""

import random
import string
from typing import Callable, Awaitable, Optional

from telethon import TelegramClient
from telethon.tl.functions.account import (
    SetPrivacyRequest,
    UpdateUsernameRequest,
)
from telethon.tl.types import (
    InputPrivacyValueAllowAll,
    InputPrivacyValueDisallowAll,
    InputPrivacyKeyStatusTimestamp,
    InputPrivacyKeyProfilePhoto,
    InputPrivacyKeyForwards,
    InputPrivacyKeyPhoneCall,
    InputPrivacyKeyVoiceMessages,
    InputPrivacyKeyPhoneNumber,
    InputPrivacyKeyChatInvite,
)


# ─────────────────────────────────────────────────────────────────
# Список из 100 английских слов для генерации username
# ─────────────────────────────────────────────────────────────────
ENGLISH_WORDS = [
    "apple", "tiger", "stone", "cloud", "river", "happy", "dance", "magic",
    "brave", "candy", "dream", "flame", "grace", "honey", "ivory", "jolly",
    "karma", "lemon", "merry", "noble", "ocean", "peace", "queen", "raven",
    "smile", "trend", "ultra", "vivid", "whale", "xenon", "youth", "zebra",
    "angel", "bliss", "crisp", "dwarf", "eager", "frost", "globe", "heart",
    "jewel", "knight", "lunar", "medal", "night", "olive", "pixel", "quick",
    "robin", "sweet", "tulip", "umber", "vapor", "wheat", "yeast", "zesty",
    "amber", "bloom", "coral", "daisy", "elder", "fable", "grain", "hazel",
    "inbox", "koala", "lilac", "mango", "nexus", "oasis", "poppy", "quilt",
    "rusty", "sable", "tango", "unity", "valve", "woven", "xerox", "yacht",
    "zonal", "arise", "blaze", "crane", "delve", "ember", "flock", "grasp",
    "haven", "image", "jumbo", "knack", "lapse", "mocha", "naval", "orbit",
    "prism", "quota", "ruler",
]


# ─────────────────────────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────────────────────────
async def _safe_log(log_func: Optional[Callable[[str], Awaitable[None]]],
                    text: str) -> None:
    """Защищённый вызов log_func: если её нет или она кинет — игнорируем."""
    if log_func is None:
        return
    try:
        res = log_func(text)
        if hasattr(res, "__await__"):
            await res
    except Exception:
        pass


def _gen_username() -> str:
    """3 рандомных слова + 1-2 цифры. Telegram требует начало с буквы и
    длину 5..32. Все символы — латиница/цифры/_."""
    words = random.sample(ENGLISH_WORDS, 3)
    suffix = "".join(random.choices(string.digits, k=random.randint(1, 2)))
    base = "_".join(words) + suffix
    base = "".join(c for c in base if c.isalnum() or c == "_")
    if not base or not base[0].isalpha():
        base = "u" + base
    return base[:32]


# ─────────────────────────────────────────────────────────────────
# Главная функция
# ─────────────────────────────────────────────────────────────────
async def setup_account(client: TelegramClient,
                        owner_id: int,
                        log_func: Optional[Callable[[str], Awaitable[None]]] = None
                        ) -> dict:
    """
    Выполняет настройку приватности и установку username.
    Возвращает dict с результатами:
      {
        'privacy_ok': bool,
        'username_set': Optional[str]   # установленный username, если установили
      }
    """
    result = {"privacy_ok": False, "username_set": None}

    # ── 1. Приватность ──
    privacy_settings = [
        (InputPrivacyKeyStatusTimestamp(),  InputPrivacyValueDisallowAll()),
        (InputPrivacyKeyProfilePhoto(),     InputPrivacyValueAllowAll()),
        (InputPrivacyKeyForwards(),         InputPrivacyValueDisallowAll()),
        (InputPrivacyKeyPhoneCall(),        InputPrivacyValueDisallowAll()),
        (InputPrivacyKeyVoiceMessages(),    InputPrivacyValueAllowAll()),
        (InputPrivacyKeyPhoneNumber(),      InputPrivacyValueDisallowAll()),
        (InputPrivacyKeyChatInvite(),       InputPrivacyValueAllowAll()),
    ]
    privacy_ok = True
    for key, rule in privacy_settings:
        try:
            await client(SetPrivacyRequest(key=key, rules=[rule]))
        except Exception as e:
            privacy_ok = False
            await _safe_log(
                log_func, f"⚠️ Приватность {type(key).__name__}: {e}"
            )
    result["privacy_ok"] = privacy_ok
    if privacy_ok:
        await _safe_log(log_func, "🔒 Приватность установлена.")

    # ── 2. Username ──
    try:
        me = await client.get_me()
        current = (getattr(me, "username", None) or "").strip()
    except Exception as e:
        current = ""
        await _safe_log(log_func, f"⚠️ get_me: {e}")

    if not current:
        attempts = 3
        for i in range(attempts):
            candidate = _gen_username()
            try:
                await client(UpdateUsernameRequest(username=candidate))
                result["username_set"] = candidate
                await _safe_log(log_func, f"✏️ Username: @{candidate}")
                break
            except Exception as e:
                await _safe_log(
                    log_func,
                    f"⚠️ Username «{candidate}» не подошёл ({e}). "
                    f"Попытка {i+1}/{attempts}.",
                )
        else:
            await _safe_log(log_func, "❌ Не удалось установить username.")
    else:
        result["username_set"] = current
        await _safe_log(log_func, f"ℹ️ Username уже установлен: @{current}")

    return result
