# -*- coding: utf-8 -*-
"""
ldv_functions.py — Регистрация и лайкинг в @leomatchbot.

Содержит:
  • register_one_ldv      — однократная регистрация одного аккаунта.
  • ldv_attach_listener   — подписка глобального слушателя сообщений
                            от @leomatchbot, складывающего Message в
                            store.last_ldv_msg[phone].
  • _wait_ldv_msg         — ожидание свежего сообщения от leomatchbot
                            через store.last_ldv_msg[phone].
  • ldv_liking_task       — фоновая корутина-цикл лайкинга для аккаунта.
  • ldv_scheduler         — забирает pending ldv_tasks из БД и запускает.
"""

import asyncio
import os
import random
import time
import logging
from typing import Optional, Callable, Awaitable, Dict, Any, List

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.tl.types import InputMediaContact
from telethon.tl.functions.messages import SendMediaRequest

import db
import client_pool as _client_pool
from config import (
    API_ID, API_HASH, SESSIONS_DIR, LDV_BOT,
    LDV_LISTEN_LO, LDV_LISTEN_HI, LDV_RESPONSE_TIMEOUT,
)
from global_proxy import proxy_to_telethon, get_proxy_for_account

# Телефоны, у которых уже зарегистрирован NewMessage-listener
_ldv_listeners: set = set()

log = logging.getLogger("ldv")


def _find_reply_button(msg, search_text: str) -> Optional[str]:
    """
    Ищет кнопку reply-клавиатуры в сообщении Telethon, текст которой
    содержит search_text (регистронезависимо).
    Возвращает точный текст кнопки или None.
    """
    try:
        markup = getattr(msg, "reply_markup", None)
        if markup is None:
            return None
        rows = getattr(markup, "rows", None)
        if not rows:
            return None
        for row in rows:
            for btn in (getattr(row, "buttons", None) or []):
                btn_text = getattr(btn, "text", "") or ""
                if search_text.lower() in btn_text.lower():
                    return btn_text
    except Exception:
        pass
    return None


# =================================================================
# register_one_ldv — полная регистрация
# =================================================================
async def register_one_ldv(client: TelegramClient,
                           phone: str,
                           data: Dict[str, Any],
                           notify_func: Optional[Callable[[str],
                                                          Awaitable[None]]] = None,
                           owner_id: Optional[int] = None,
                           cancel_set: Optional[set] = None) -> bool:
    """
    Полная (с нуля) регистрация в @leomatchbot.

    data:
       ages: List, sex: str, target: str, cities: List, names: List,
       photos: List[str]

    cancel_set — set номеров; если phone окажется в нём, регистрация
    прерывается на ближайшем шаге.
    """
    bot = LDV_BOT

    def _is_cancel() -> bool:
        return bool(cancel_set and phone in cancel_set)

    ages   = data.get("ages") or data.get("age") or []
    if isinstance(ages, (str, int)): ages = [ages]
    sex    = data.get("sex") or "Я девушка"
    target = data.get("target") or "Парни"
    cities = data.get("cities") or data.get("city") or []
    if isinstance(cities, str): cities = [cities]
    names  = data.get("names") or data.get("name") or []
    if isinstance(names, str): names = [names]
    photos = list(data.get("photos") or [])

    age  = str(random.choice(ages))   if ages   else "20"
    city = random.choice(cities)      if cities else "Москва"
    name = random.choice(names)       if names  else "Аня"

    async def _save(step: int):
        if owner_id is not None:
            await db.db_save_reg_state(phone, bot, step, data, owner_id)

    async def _send(text: str):
        await client.send_message(bot, text)
        await asyncio.sleep(random.uniform(3, 7))

    try:
        if _is_cancel(): return False
        await client.send_message(bot, "/start")
        await asyncio.sleep(2)
        await _save(0)

        # ── Проверка: аккаунт уже зарегистрирован? ──────────────
        try:
            _pre = await client.get_messages(bot, limit=5)
            for _m in _pre:
                _t = (getattr(_m, "text", "") or
                      getattr(_m, "message", "") or "").lower()
                if "так выглядит твоя анкета" in _t:
                    if notify_func:
                        await notify_func(
                            f"⏭ {phone}: уже зарегистрирован в LDV — "
                            f"пропускаю.")
                    return False
        except Exception:
            pass
        # ─────────────────────────────────────────────────────────

        if _is_cancel(): return False
        await _send("🇷🇺 Русский");                await _save(1)
        if _is_cancel(): return False
        await _send("👌 давай начнем");            await _save(2)
        if _is_cancel(): return False
        await _send("👌 Ok");                       await _save(3)
        if _is_cancel(): return False
        await _send(age);                           await _save(4)
        if _is_cancel(): return False
        await _send(sex);                           await _save(5)
        if _is_cancel(): return False
        await _send(target);                        await _save(6)
        if _is_cancel(): return False
        await _send(city);                          await _save(7)
        if _is_cancel(): return False
        await _send(name);                          await _save(8)
        if _is_cancel(): return False
        await _send("Пропустить");                  await _save(9)

        if not photos:
            if notify_func:
                await notify_func(f"⚠️ {phone}: нет фото — отмена.")
            return False
        chosen = random.sample(photos,
                               k=min(len(photos), random.randint(1, 3)))
        for p in chosen:
            if _is_cancel(): return False
            try:
                await client.send_file(bot, p)
            except Exception as e:
                if notify_func:
                    await notify_func(f"⚠️ {phone}: фото '{p}': {e}")
            await asyncio.sleep(random.uniform(3, 7))

        if _is_cancel(): return False
        await _send("Это все, сохранить фото");     await _save(10)

        # send_contact
        try:
            bot_entity = await client.get_input_entity(bot)
            me = await client.get_me()
            await client(SendMediaRequest(
                peer=bot_entity,
                media=InputMediaContact(
                    phone_number=str(phone),
                    first_name=name, last_name="", vcard="",
                ),
                message="",
                random_id=random.randint(1, 2**62),
            ))
        except Exception as e:
            if notify_func:
                await notify_func(f"⚠️ {phone}: send_contact: {e}")
        await _save(11)
        await asyncio.sleep(random.uniform(3, 7))

        if _is_cancel(): return False
        await _send("Да");                          await _save(12)

    except Exception as e:
        if notify_func:
            await notify_func(f"❌ {phone}: ошибка регистрации LDV: {e}")
        return False

    if owner_id is not None:
        await db.db_delete_reg_state(phone, bot)
    if notify_func:
        await notify_func(f"✅ {phone}: регистрация LDV завершена.")
    return True


# =================================================================
# Глобальный слушатель @leomatchbot
# =================================================================
def ldv_attach_listener(client: TelegramClient, phone: str, store) -> None:
    """
    Регистрирует на client обработчик NewMessage от LDV_BOT.
    Идемпотентно — повторный вызов для того же phone ничего не делает
    (нужно при использовании общего client_pool-клиента).
    """
    if phone in _ldv_listeners:
        return
    _ldv_listeners.add(phone)

    @client.on(events.NewMessage(from_users=LDV_BOT))
    async def _on_msg(event):
        try:
            store.last_ldv_msg[phone] = event.message
        except Exception as e:
            log.warning("ldv listener (%s) error: %s", phone, e)


async def _wait_ldv_msg(phone: str, store, timeout: float):
    """Ждёт обновление store.last_ldv_msg[phone] (по сравнению с initial id)."""
    deadline = time.time() + timeout
    initial = store.last_ldv_msg.get(phone)
    initial_id = initial.id if initial else 0
    while time.time() < deadline:
        msg = store.last_ldv_msg.get(phone)
        if msg and msg.id > initial_id:
            return msg
        await asyncio.sleep(0.3)
    return None


# =================================================================
# ldv_liking_task — цикл лайкинга для одного аккаунта
# =================================================================
async def ldv_liking_task(phone: str, owner_id: int, store,
                          notify_func: Optional[
                              Callable[[int, str], Awaitable[None]]
                          ] = None) -> None:
    """
    Цикл лайкинга. Логика:
      1. Отправляет 2 лайка в @leomatchbot
      2. Ожидает ответное сообщение, проверяя на наличие упоминания о лимите
      3. Если лимит не превышен, обновляет задачу в БД, назначая следующий запуск через 10 часов
      4. Если лимит превышен, откладывает следующий цикл на случайное время
    """
    bot = LDV_BOT
    proxy = await get_proxy_for_account(phone, owner_id)
    tproxy = proxy_to_telethon(proxy or "")

    client = await _client_pool.get_or_connect(
        phone, API_ID, API_HASH, SESSIONS_DIR, proxy=tproxy
    )
    if client is None:
        log.warning("ldv start %s: connect failed", phone)
        await db.db_update_ldv_task(phone, status="error",
                                    next_run=time.time() + 600)
        return

    ldv_attach_listener(client, phone, store)
    store.current_liking_phones.add(phone)
    await db.db_update_ldv_task(phone, status="running")

    try:
        # Проверяем, не отменен ли процесс
        if phone in store.cancelled_phones:
            return
            
        # Отправляем 2 лайка
        try:
            await client.send_message(bot, "❤️")
            await asyncio.sleep(random.uniform(2, 4))
            await client.send_message(bot, "❤️")
        except FloodWaitError as e:
            log.warning("ldv flood %s: %s", phone, e.seconds)
            await db.db_update_ldv_task(
                phone, status="pending",
                next_run=time.time() + e.seconds + 60
            )
            return
        except Exception as e:
            log.warning("ldv send heart %s: %s", phone, e)

        # Ждем ответное сообщение от бота и проверяем на лимит
        msg = await _wait_ldv_msg(phone, store, LDV_RESPONSE_TIMEOUT)
        limit_hit = False

        if msg is not None:
            text = (msg.text or msg.message or "").lower()
            if "лимит" in text or "исчерпан" in text or "ограничен" in text:
                limit_hit = True
                pause_min = random.uniform(LDV_LISTEN_LO, LDV_LISTEN_HI)
                await db.db_update_ldv_task(
                    phone, status="pending",
                    next_run=time.time() + pause_min * 60,
                )
                if notify_func:
                    try:
                        await notify_func(
                            owner_id,
                            f"⏸ {phone}: LDV — лимит, "
                            f"следующий цикл через {pause_min:.1f} мин.",
                        )
                    except Exception:
                        pass
            elif "больше внимания" in text:
                # Бот предлагает буст — отказываемся
                btn = _find_reply_button(msg, "в другой раз")
                reply_text = btn if btn else "В другой раз"
                try:
                    await client.send_message(bot, reply_text)
                except Exception:
                    pass

        # Если лимита нет — планируем следующий запуск через 10 часов (36000 секунд)
        if not limit_hit:
            await db.db_update_ldv_task(
                phone, status="pending",
                next_run=time.time() + 36000,
            )
            if notify_func:
                try:
                    await notify_func(
                        owner_id,
                        f"💤 {phone}: отправлено 2 лайка. Следующий цикл через 10 часов."
                    )
                except Exception:
                    pass

    except Exception as e:
        log.warning("ldv_liking_task %s: %s", phone, e)
        if notify_func:
            try:
                await notify_func(owner_id, f"❌ {phone}: LDV ошибка: {e}")
            except Exception:
                pass
        await db.db_update_ldv_task(
            phone, status="pending",
            next_run=time.time() + 300,
        )
    finally:
        store.current_liking_phones.discard(phone)


# =================================================================
# ldv_scheduler — фоновый планировщик
# =================================================================
async def ldv_scheduler(store,
                        notify_func: Optional[
                            Callable[[int, str], Awaitable[None]]
                        ] = None,
                        task_queue=None) -> None:
    """
    Каждые 10с забирает все pending ldv_tasks с next_run<=now и
    стартует ldv_liking_task через asyncio.create_task.
    Если phone уже в current_liking_phones — пропускает.
    """
    while True:
        try:
            tasks = await db.db_get_pending_ldv_tasks()
            for t in tasks:
                phone = t["phone"]
                owner_id = t["owner_id"]
                if phone in store.current_liking_phones:
                    continue
                if phone in store.cancelled_phones:
                    await db.db_delete_ldv_task(phone)
                    store.cancelled_phones.discard(phone)
                    continue

                store.current_liking_phones.add(phone)

                async def _runner(p=phone, o=owner_id):
                    await ldv_liking_task(p, o, store, notify_func=notify_func)

                asyncio.create_task(_runner())
        except Exception as e:
            log.warning("ldv_scheduler error: %s", e)

        await asyncio.sleep(10)