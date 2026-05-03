# -*- coding: utf-8 -*-
"""
profile_music.py — Установка дня рождения и музыки профиля.

set_birthday(client)        — ставит 11 февраля, работает для всех аккаунтов.
set_profile_music(client, mp3_path) — устанавливает профильную музыку (Telegram Premium).
                              Молча пропускает аккаунты без Premium.

Структура папок:
  music/           ← хранилище MP3
  music/{uid}/     ← временная папка пользователя, удаляется после залива
"""

import os
import shutil
import logging
import random

from telethon import TelegramClient
from telethon.tl.functions.account import UpdateBirthdayRequest
from telethon.tl.types import Birthday
from telethon.tl.functions.photos import UploadProfilePhotoRequest

log = logging.getLogger("profile_music")

MUSIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "music")
os.makedirs(MUSIC_DIR, exist_ok=True)


async def _safe_log(log_func, text: str) -> None:
    if log_func is None:
        return
    try:
        res = log_func(text)
        if hasattr(res, "__await__"):
            await res
    except Exception:
        pass


async def set_birthday(client: TelegramClient, log_func=None) -> bool:
    """Ставит дату рождения 11 февраля (без года). Работает для всех аккаунтов."""
    try:
        await client(UpdateBirthdayRequest(birthday=Birthday(day=11, month=2)))
        await _safe_log(log_func, "🎂 День рождения установлен (11.02).")
        return True
    except Exception as e:
        await _safe_log(log_func, f"🎂 Дата рождения: {e}")
        return False


async def set_profile_music(client: TelegramClient, mp3_path: str, log_func=None) -> bool:
    """
    Устанавливает MP3 как музыку профиля.
    Требует Telegram Premium — при отсутствии молча пропускает.
    """
    try:
        uploaded = await client.upload_file(mp3_path)
        await client(UploadProfilePhotoRequest(video=uploaded))
        await _safe_log(log_func, "🎵 Музыка профиля установлена.")
        return True
    except Exception as e:
        err = str(e)
        if "PREMIUM" in err.upper():
            await _safe_log(log_func, "🎵 Пропущено: нет Telegram Premium.")
        else:
            await _safe_log(log_func, f"🎵 Музыка профиля: {e}")
        return False


def get_user_music_dir(uid: int) -> str:
    """Возвращает путь к временной папке пользователя."""
    return os.path.join(MUSIC_DIR, str(uid))


def cleanup_user_music(uid: int) -> None:
    """Удаляет временную папку пользователя после завершения массового залива."""
    user_dir = get_user_music_dir(uid)
    if os.path.isdir(user_dir):
        shutil.rmtree(user_dir, ignore_errors=True)
        log.debug("music dir removed: %s", user_dir)
