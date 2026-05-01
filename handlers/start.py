# -*- coding: utf-8 -*-
"""
handlers/start.py — /start, /cancel, /help, главное меню, переключатели
разделов и глобальный сборщик фото.
"""

import logging
import os
import time
from typing import Optional

from aiogram import Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery
from aiogram.exceptions import TelegramBadRequest

import db
import config
from bot_globals import (
    bot, store, task_queue,
    _grp_index_cache, _signin_sessions, _tdata_sessions,
    _trf_selection, _transfer_pending, _man_sel_ctx, _man_selection,
    kb, home_btn, notify_owner,
)
from utils import (
    restore_main_menu, main_menu_keyboard, cancel_pending_ask,
    has_pending,
)
from global_proxy import apply_global_to_unproxied

log = logging.getLogger("start")
router = Router(name="start")


# =================================================================
# /start — поддерживает deep-link передачи аккаунтов (tr_TOKEN)
# =================================================================
@router.message(CommandStart())
async def handle_start(msg: Message):
    uid = msg.from_user.id
    # Deep-link: передача аккаунтов
    parts = (msg.text or "").split(maxsplit=1)
    args = parts[1].strip() if len(parts) > 1 else ""
    if args.startswith("tr_"):
        from handlers.transfer import _handle_transfer_incoming
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


@router.message(Command("cancel"))
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


@router.message(Command("help"))
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


@router.message(F.text == "🏠 Главное меню")
async def handle_home(msg: Message):
    uid = msg.from_user.id
    store.reset_user(uid)
    _grp_index_cache.pop(uid, None)
    _signin_sessions.pop(uid, None)
    _trf_selection.pop(uid, None)
    _transfer_pending.pop(uid, None)
    _man_sel_ctx.pop(uid, None)
    _man_selection.pop(uid, None)
    await restore_main_menu(bot, msg.chat.id, uid, "Возврат в главное меню.")


@router.callback_query(F.data == "action_cancel")
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
# Переключатели верхних разделов
# =================================================================
@router.message(F.text == "⚙️ Аккаунты")
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
            [("🏷 Смена username", "auto_rtag"),
             ("🔑 Смена 2FA", "acc_2fa_bulk")],
            [("🔄 Передать аккаунты", "acc_transfer")],
            [home_btn()],
        ),
    )


@router.callback_query(F.data == "acc_apply_global")
async def cb_acc_apply_global(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer("Проверяю и применяю…")
    res = await apply_global_to_unproxied(uid, recheck=True)
    await cb.message.answer(
        "📡 <b>Применение глобал-прокси</b>\n"
        f"Живых глобалов после проверки: <b>{res['alive_globals']}</b>\n"
        f"Назначено аккаунтам: <b>{res['updated']}</b>\n"
        f"Без прокси осталось: <b>{res['skipped']}</b>"
    )


@router.message(F.text == "🤖 Автоматизация")
async def handle_section_auto(msg: Message):
    await msg.answer(
        "🤖 <b>Автоматизация</b>\n\n"
        "Массовые операции над аккаунтами: заливка профилей, "
        "регистрация в приложениях, автоответы.",
        reply_markup=kb(
            [("🚀 Массовый залив", "auto_mass")],
            [("🤖 Регистрация LDV", "auto_ldv"),
             ("💘 Регистрация XO", "auto_xo")],
            [("📺 Подписка @leoday", "auto_subdv")],
            [("💬 Автоответы", "auto_ar")],
            [home_btn()],
        ),
    )


@router.message(F.text == "📊 Управление")
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


@router.message(F.text == "📈 Прогресс")
async def handle_section_progress(msg: Message):
    uid = msg.from_user.id
    s = task_queue.status()
    accs = await db.db_get_accounts_by_owner(uid)
    ldv_tasks = await db.db_get_ldv_tasks_by_owner(uid)
    xo_tasks  = await db.db_get_xo_tasks_by_owner(uid)
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
        reply_markup=kb([(logs_btn_text, "prog_logs_toggle")], [home_btn()]),
    )


@router.callback_query(F.data == "prog_logs_toggle")
async def cb_prog_logs_toggle(cb: CallbackQuery):
    uid = cb.from_user.id
    s = await db.db_user_settings_get(uid)
    new_val = not bool(s.get("logs_enabled"))
    await db.db_user_settings_set_logs(uid, new_val)
    await cb.answer(f"Логи {'включены' if new_val else 'выключены'}.")


@router.message(F.text == "👑 Админ")
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
# Глобальный сборщик фото (для mass fill, карточек аккаунтов и т. п.)
# =================================================================
@router.message(F.photo)
async def handle_photo(msg: Message):
    """
    Если пользователь сейчас «собирает» фото (store.photo_collecting[uid]),
    скачиваем фото в temp/<uid>/ и добавляем путь в store.temp_photos[uid].
    """
    uid = msg.from_user.id
    if has_pending(uid):
        return
    if not store.photo_collecting.get(uid):
        return

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


@router.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()
