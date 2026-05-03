# -*- coding: utf-8 -*-
"""
main.py — Точка входа бота-менеджера фермы Telegram-аккаунтов.

Вся бизнес-логика вынесена в handlers/:
  start.py        — /start, /cancel, /help, главное меню, фото-коллектор
  accounts.py     — аккаунты, TData/Session импорт, прокси
  automation.py   — массовый залив, рега LDV/XO, подписка, username, AR
  transfer.py     — передача аккаунтов по одноразовой ссылке
  share.py        — общий доступ к аккаунтам (2+ пользователей)
  manage.py       — задачи LDV/XO, пролайк, отмена регистраций
  admin.py        — whitelist, администраторы, все аккаунты
"""

import asyncio
import logging
import os

from aiogram import Router
from aiogram.types import CallbackQuery, Message

import config
import db
import client_pool as _client_pool
from bot_globals import bot, dp, store, task_queue, ar_manager
from utils import (
    is_allowed, restore_main_menu, attach_pending_router,
    cancel_pending_ask,
)
from global_proxy import (
    run_health_check_loop, set_admin_notifier, get_proxy_for_account,
)
from ldv_functions import ldv_scheduler
from xo_functions import xo_liking_scheduler
from client_pool import session_watchdog as _session_watchdog

# Импортируем роутеры хендлеров
from handlers import start, accounts, automation, transfer, manage, admin, share

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("main")


# =================================================================
# Мидлварь доступа
# =================================================================
@dp.update.outer_middleware()
async def access_middleware(handler, event, data):
    """Любой апдейт от неизвестного user_id → отказ."""
    uid = None
    msg_or_cb = None
    if event.message:
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
    update_type = (
        "message" if event.message else
        "callback_query" if event.callback_query else
        "inline_query" if event.inline_query else "other"
    )
    log.warning("access_middleware: denied uid=%s type=%s", uid, update_type)
    try:
        if isinstance(msg_or_cb, Message):
            await msg_or_cb.answer("⛔ У вас нет доступа к этому боту.")
        elif isinstance(msg_or_cb, CallbackQuery):
            await msg_or_cb.answer("⛔ Нет доступа.", show_alert=True)
    except Exception:
        pass


# =================================================================
# Pending-router (перехват текстовых ответов ask_with_cancel)
# =================================================================
pending_router = Router(name="pending")
attach_pending_router(pending_router, store)
dp.include_router(pending_router)


# =================================================================
# Guard middleware — кнопки имеют высший приоритет над pending
# =================================================================
@dp.callback_query.outer_middleware()
async def _callback_guard_mw(handler, event: CallbackQuery, data):
    if not event.data:
        return await handler(event, data)
    uid = event.from_user.id if event.from_user else 0
    if uid:
        # Отменяем висящий ask_with_cancel, чтобы кнопки имели приоритет.
        # store.set_action НЕ сбрасываем здесь: управление action —
        # ответственность каждого хендлера (особенно важно для задач
        # с интерактивным вводом кода/прокси в фоновом Task'е).
        cancel_pending_ask(uid)
    return await handler(event, data)


# =================================================================
# Регистрируем роутеры хендлеров
# =================================================================
dp.include_router(start.router)
dp.include_router(accounts.router)
dp.include_router(automation.router)
dp.include_router(transfer.router)
dp.include_router(share.router)
dp.include_router(manage.router)
dp.include_router(admin.router)


# =================================================================
# Bootstrap
# =================================================================
async def _bootstrap_autoreplies():
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
            ok = await ar_manager.start(phone, owner_id, proxy, custom_text=custom)
            if ok:
                started += 1
        except Exception as e:
            log.warning("bootstrap autoreply %s: %s", phone, e)
    log.info("Autoreplies started: %d", started)


async def _bootstrap_dirs():
    os.makedirs(config.SESSIONS_DIR, exist_ok=True)
    os.makedirs(config.TEMP_DIR, exist_ok=True)


async def _notify_admins(text: str) -> None:
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
    from bot_globals import notify_owner
    import bot_globals as _bg
    await _bootstrap_dirs()
    await db.init_db()
    try:
        me = await bot.get_me()
        _bg._bot_username = me.username or ""
    except Exception as e:
        log.warning("_on_startup: get_me() failed: %s", e)
    ar_manager.set_notifier(notify_owner)
    set_admin_notifier(_notify_admins)
    # Удаляем устаревшие токены передачи (TTL из config.TRANSFER_TOKEN_TTL)
    try:
        expired = await db.db_transfer_cleanup_expired()
        if expired:
            log.info("Cleaned up %d expired transfer token(s).", expired)
    except Exception as _e:
        log.warning("transfer cleanup: %s", _e)
    # Удаляем устаревшие токены шаринга
    try:
        expired_shr = await db.db_share_cleanup_expired()
        if expired_shr:
            log.info("Cleaned up %d expired share token(s).", expired_shr)
    except Exception as _e:
        log.warning("share cleanup: %s", _e)
    asyncio.create_task(run_health_check_loop())
    asyncio.create_task(ldv_scheduler(store, notify_func=notify_owner))
    asyncio.create_task(xo_liking_scheduler(store, notify_func=notify_owner))
    asyncio.create_task(_bootstrap_autoreplies())

    async def _watchdog_notify(phone: str) -> None:
        try:
            acc = await db.db_get_account(phone)
            uid = acc["owner_id"] if acc else None
            if uid:
                await notify_owner(uid,
                    f"⚠️ Аккаунт <code>{phone}</code> вышел из системы — сессия удалена.")
        except Exception as e:
            log.debug("watchdog_notify %s: %s", phone, e)

    asyncio.create_task(
        _session_watchdog(config.SESSIONS_DIR, interval=120, notify_func=_watchdog_notify)
    )
    log.info("Менеджер запущен.")


async def _on_shutdown():
    log.info("Останавливаю менеджер…")
    try:
        await ar_manager.stop_all()
    except Exception:
        pass
    for ph, t in list(store.xo_liking_tasks.items()):
        store.xo_liking_tasks.pop(ph, None)
        if not t.done():
            t.cancel()
    for ph in _client_pool.all_phones():
        await _client_pool.remove(ph)
    try:
        await bot.session.close()
    except Exception:
        pass


# =================================================================
# Main
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
