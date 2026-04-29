# -*- coding: utf-8 -*-
"""
autoreply.py — Менеджер автоответов для юзерботов.

Особенности:
  • Каждый аккаунт-юзербот живёт как Telethon-клиент с подписанным обработчиком
    приватных входящих сообщений.
  • При исходящем сообщении пользователя в чате — навсегда «замолчать» этот чат
    (этот аккаунт перестаёт отвечать в нём автоматически).
  • Любое исходящее → 45-секундная общая «заморозка» автоответчика.
  • На каждый паттерн-блок отвечаем в чате только ОДИН раз.
  • Если ни один блок не подошёл — через 5-7 мин отправляем дефолтный текст
    (один раз на чат).
"""

import asyncio
import os
import random
import time
import logging
from typing import Optional, Dict, Set, Callable, Awaitable, Tuple

from telethon import TelegramClient, events

import client_pool as _client_pool
from autoreply_rules import AUTOREPLY_RULES, DEFAULT_REPLY_TEXT
from config import API_ID, API_HASH, SESSIONS_DIR
from global_proxy import proxy_to_telethon, get_proxy_for_account

log = logging.getLogger("autoreply")


# ─────────────────────────────────────────────────────────────────
# Глобальные тайминги
# ─────────────────────────────────────────────────────────────────
REPLY_DELAY_LO = 15      # сек — перед ответом на сработавший паттерн
REPLY_DELAY_HI = 30
DEFAULT_DELAY_LO = 300   # сек — перед фоллбэк-ответом
DEFAULT_DELAY_HI = 420
OUTGOING_FREEZE_SEC = 45  # сек — общая заморозка после исходящего


# Тип callback-уведомления владельца ("🔕 phone: ...")
NotifyFn = Callable[[int, str], Awaitable[None]]


class AutoreplyManager:
    """Глобальный менеджер автоответов; одна инстанция на процесс."""

    def __init__(self, notify_owner: Optional[NotifyFn] = None):
        # phone -> timestamp до которого ничего не отвечаем
        self._frozen_until: Dict[str, float] = {}
        # phone -> set(chat_id), где автоответ навсегда замолчан
        self._silenced_chats: Dict[str, Set[int]] = {}
        # phone -> {chat_id: set(block_idx)}
        self._answered_blocks: Dict[str, Dict[int, Set[int]]] = {}
        # phone -> set(chat_id), куда уже ушёл дефолтный ответ
        self._default_sent: Dict[str, Set[int]] = {}
        # phone -> кастомный текст (вместо DEFAULT_REPLY_TEXT)
        self._custom_text: Dict[str, Optional[str]] = {}
        # phone -> owner_id
        self._owners: Dict[str, int] = {}
        # phone -> (on_in_fn, on_out_fn) — для удаления хендлеров при stop()
        self._handlers: Dict[str, Tuple] = {}
        # set активных телефонов (autoreply включён)
        self._active: Set[str] = set()
        # callback "сообщить владельцу" — задаётся из main.py
        self._notify_owner: Optional[NotifyFn] = notify_owner
        # Лок для одновременных стартов одного и того же phone
        self._lock = asyncio.Lock()

    # ============================================================
    # Публичные API
    # ============================================================
    def set_notifier(self, fn: NotifyFn) -> None:
        self._notify_owner = fn

    def is_running(self, phone: str) -> bool:
        return phone in self._active

    def silenced_count(self, phone: str) -> int:
        return len(self._silenced_chats.get(phone, set()))

    def get_custom_text(self, phone: str) -> Optional[str]:
        return self._custom_text.get(phone)

    def set_custom_text(self, phone: str, text: Optional[str]) -> None:
        self._custom_text[phone] = text

    def reset_silenced(self, phone: str) -> int:
        """Сбросить «замолчанные» чаты у конкретного аккаунта.
        Возвращает количество удалённых записей."""
        n = len(self._silenced_chats.get(phone, set()))
        self._silenced_chats[phone] = set()
        # снимаем ограничения и для дефолта/блоков
        self._answered_blocks[phone] = {}
        self._default_sent[phone] = set()
        return n

    # ============================================================
    # start / stop
    # ============================================================
    async def start(self, phone: str, owner_id: int,
                    proxy: Optional[str], custom_text: Optional[str] = None
                    ) -> bool:
        """
        Запустить автоответчик для аккаунта `phone`.
        Использует глобальный client_pool — не создаёт дублирующий клиент.
        Возвращает True, если успешно стартовал/уже работал.
        """
        async with self._lock:
            if phone in self._active:
                self._owners[phone] = owner_id
                if custom_text is not None:
                    self._custom_text[phone] = custom_text
                return True

            tproxy = proxy_to_telethon(proxy or "")
            client = await _client_pool.get_or_connect(
                phone, API_ID, API_HASH, SESSIONS_DIR, proxy=tproxy
            )
            if client is None:
                log.warning("autoreply.start: %s connect failed", phone)
                return False

            self._owners[phone] = owner_id
            self._silenced_chats.setdefault(phone, set())
            self._answered_blocks.setdefault(phone, {})
            self._default_sent.setdefault(phone, set())
            self._custom_text[phone] = custom_text
            self._active.add(phone)

            # Регистрируем хендлеры и сохраняем ссылки для удаления при stop()
            async def _on_in(event):
                if not event.is_private:
                    return
                if phone not in self._active:
                    return
                await self._handle_message(phone, owner_id, event.message)

            async def _on_out(event):
                if not event.is_private:
                    return
                if phone not in self._active:
                    return
                await self._handle_message(phone, owner_id, event.message)

            client.add_event_handler(_on_in, events.NewMessage(incoming=True))
            client.add_event_handler(_on_out, events.NewMessage(outgoing=True))
            self._handlers[phone] = (_on_in, _on_out)

            return True

    async def stop(self, phone: str) -> None:
        async with self._lock:
            self._active.discard(phone)
            self._owners.pop(phone, None)
            handlers = self._handlers.pop(phone, None)

        # Снимаем хендлеры с общего клиента (НЕ отключаем — клиент общий)
        client = _client_pool.get(phone)
        if client and handlers:
            try:
                client.remove_event_handler(handlers[0],
                                            events.NewMessage(incoming=True))
                client.remove_event_handler(handlers[1],
                                            events.NewMessage(outgoing=True))
            except Exception:
                pass

    async def stop_all(self) -> None:
        for phone in list(self._active):
            await self.stop(phone)

    # ============================================================
    # Обработчик сообщения
    # ============================================================
    async def _handle_message(self, phone: str, owner_id: int, msg) -> None:
        try:
            chat_id = msg.chat_id
            if chat_id is None:
                return

            # ── 1. Исходящее: заморозка + молчание навсегда в этом чате ──
            if getattr(msg, "out", False) or getattr(msg, "outgoing", False):
                self._frozen_until[phone] = time.time() + OUTGOING_FREEZE_SEC
                self._silenced_chats.setdefault(phone, set()).add(chat_id)
                # уведомить владельца
                if self._notify_owner:
                    try:
                        await self._notify_owner(
                            owner_id,
                            f"🔕 {phone}: автоответ замолчал в чате "
                            f"{chat_id} — вы написали сами."
                        )
                    except Exception:
                        pass
                return

            # ── 2. Заморожен ──
            if self._is_frozen(phone):
                return

            # ── 3. Чат замолчан ──
            if chat_id in self._silenced_chats.get(phone, set()):
                return

            text = (msg.text or msg.message or "")
            matched_reply, block_idx = self._match_reply(text)

            client = _client_pool.get(phone)
            if client is None:
                return

            # Сохраняем peer ДО сна — после переподключения кэш entity может
            # быть очищен, и chat_id (int) перестаёт резолвиться
            try:
                peer = await msg.get_input_chat()
            except Exception:
                peer = chat_id

            if matched_reply is not None:
                # уже отвечали этим блоком в этом чате?
                answered = self._answered_blocks.setdefault(
                    phone, {}).setdefault(chat_id, set())
                if block_idx in answered:
                    return

                delay = random.uniform(REPLY_DELAY_LO, REPLY_DELAY_HI)
                await asyncio.sleep(delay)

                # перепроверка перед отправкой
                if self._is_frozen(phone):
                    return
                if chat_id in self._silenced_chats.get(phone, set()):
                    return
                if block_idx in self._answered_blocks.get(
                        phone, {}).get(chat_id, set()):
                    return

                try:
                    await client.send_message(peer, matched_reply)
                    answered.add(block_idx)
                    try:
                        await client.send_read_acknowledge(peer)
                    except Exception:
                        pass
                except Exception as e:
                    log.warning("autoreply send (%s) failed: %s", phone, e)

            else:
                # фоллбэк: ни один блок не подошёл
                if chat_id in self._default_sent.get(phone, set()):
                    return

                delay = random.uniform(DEFAULT_DELAY_LO, DEFAULT_DELAY_HI)
                await asyncio.sleep(delay)

                if self._is_frozen(phone):
                    return
                if chat_id in self._silenced_chats.get(phone, set()):
                    return
                if chat_id in self._default_sent.get(phone, set()):
                    return

                default_text = (self._custom_text.get(phone)
                                or DEFAULT_REPLY_TEXT)
                try:
                    await client.send_message(peer, default_text)
                    self._default_sent.setdefault(phone, set()).add(chat_id)
                    try:
                        await client.send_read_acknowledge(peer)
                    except Exception:
                        pass
                except Exception as e:
                    log.warning("autoreply default (%s) failed: %s", phone, e)

        except Exception as e:
            log.warning("autoreply handle error %s: %s", phone, e)

    # ============================================================
    # Помощники
    # ============================================================
    def _is_frozen(self, phone: str) -> bool:
        until = self._frozen_until.get(phone, 0.0)
        return time.time() < until

    @staticmethod
    def _match_reply(text: str) -> Tuple[Optional[str], int]:
        """
        Возвращает (reply, block_idx) — первый сработавший блок;
        или (None, -1), если ни один паттерн не подошёл.
        """
        if not text:
            return None, -1
        low = text.lower()
        for idx, block in enumerate(AUTOREPLY_RULES):
            for p in block["patterns"]:
                if p in low:
                    reply = random.choice(block["replies"])
                    return reply, idx
        return None, -1
