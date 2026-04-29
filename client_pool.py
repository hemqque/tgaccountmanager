# -*- coding: utf-8 -*-
"""
client_pool.py — Глобальный пул Telethon-клиентов.

Гарантирует что для каждого номера телефона существует ровно ОДИН
TelegramClient в любой момент времени. Без этого autoreply, liking-задачи
и management-функции создают каждый свой клиент на один и тот же .session-файл,
что приводит к конфликту SQLite и Telegram сбрасывает соединение через 3-6 сек.
"""

import asyncio
import os
import logging
from typing import Dict, Optional

from telethon import TelegramClient

log = logging.getLogger("client_pool")

_pool: Dict[str, TelegramClient] = {}
_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock


async def get_or_connect(phone: str, api_id: int, api_hash: str,
                         session_dir: str, proxy=None) -> Optional[TelegramClient]:
    """
    Вернуть существующий подключённый клиент или создать/переподключить новый.
    Никогда не создаёт второй клиент для одного и того же phone.
    """
    async with _get_lock():
        cli = _pool.get(phone)
        if cli is not None and cli.is_connected():
            return cli

        session_path = os.path.join(session_dir, phone)
        os.makedirs(session_dir, exist_ok=True)

        if cli is None:
            cli = TelegramClient(session_path, api_id, api_hash, proxy=proxy)

        try:
            await cli.connect()
            if not await cli.is_user_authorized():
                log.warning("client_pool: %s not authorized", phone)
                await cli.disconnect()
                _pool.pop(phone, None)
                return None
            _pool[phone] = cli
            log.info("client_pool: %s connected", phone)
            return cli
        except Exception as e:
            log.warning("client_pool: %s connect error: %s", phone, e)
            try:
                await cli.disconnect()
            except Exception:
                pass
            _pool.pop(phone, None)
            return None


def get(phone: str) -> Optional[TelegramClient]:
    """Вернуть клиент из пула без подключения (может быть None)."""
    return _pool.get(phone)


def put(phone: str, client: TelegramClient) -> None:
    """Зарегистрировать уже подключённый клиент в пуле."""
    _pool[phone] = client


async def remove(phone: str) -> None:
    """Удалить клиент из пула и отключить."""
    cli = _pool.pop(phone, None)
    if cli:
        try:
            await cli.disconnect()
        except Exception:
            pass
        log.info("client_pool: %s removed", phone)


def all_phones() -> list:
    return list(_pool.keys())
