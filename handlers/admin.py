# -*- coding: utf-8 -*-
"""
handlers/admin.py — Администрирование: whitelist, список администраторов,
просмотр всех аккаунтов. Глобальные прокси делегированы accounts.py.
"""

import logging
from typing import Any, Dict, List

from aiogram import Router, F
from aiogram.types import CallbackQuery

import db
from bot_globals import bot, kb, home_btn
from utils import ask_with_retry

log = logging.getLogger("admin")
router = Router(name="admin")


@router.callback_query(F.data == "adm_back")
async def cb_adm_back(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    await cb.message.edit_text(
        "👑 <b>Администрирование</b>\n\n"
        "Управление доступом пользователей, глобальными прокси и просмотр всех аккаунтов.",
        reply_markup=kb(
            [("👥 Whitelist", "adm_wl"), ("👮 Администраторы", "adm_admins")],
            [("🌐 Глобальные прокси", "gpx_list"), ("📋 Все аккаунты", "adm_all_accs")],
            [home_btn()]))
    await cb.answer()


# =================================================================
# 👥 Whitelist
# =================================================================
@router.callback_query(F.data == "adm_wl")
async def cb_adm_wl(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    rows_db = await db.db_whitelist_get_all()
    text_lines = [f"👥 <b>Whitelist</b>\n━━━━━━━━━━━━━━━━━━━\nПользователей: <b>{len(rows_db)}</b>\n"]
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
            [("➕ Добавить", "adm_wl_add"), ("🗑 Удалить", "adm_wl_del")],
            [("‹ Назад", "adm_back"), home_btn()]))
    await cb.answer()


@router.callback_query(F.data == "adm_wl_add")
async def cb_adm_wl_add(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, cb.from_user.id,
        "👤 Пришлите user_id или @username для добавления в whitelist:",
        validator=lambda t: t.strip().startswith("@") or t.strip().lstrip("-").isdigit(),
        error_msg="❌ Введите числовой user_id или @username.")
    if not raw:
        return
    raw = raw.strip()
    user_id = None; username = ""
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
        f"✅ Добавлен: <code>{user_id}</code>" + (f" (@{username})" if username else ""))


@router.callback_query(F.data == "adm_wl_del")
async def cb_adm_wl_del(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, cb.from_user.id,
        "🗑 Пришлите user_id для удаления из whitelist:",
        validator=lambda t: t.strip().lstrip("-").isdigit(),
        error_msg="❌ user_id должен быть числом.")
    if not raw:
        return
    user_id = int(raw.strip())
    await db.db_whitelist_remove(user_id)
    await cb.message.answer(f"🗑 Удалён: <code>{user_id}</code>")


# =================================================================
# 👮 Администраторы
# =================================================================
@router.callback_query(F.data == "adm_admins")
async def cb_adm_admins(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    rows_db = await db.db_admins_get_all()
    text_lines = [f"👮 <b>Администраторы</b>\n━━━━━━━━━━━━━━━━━━━\nВсего: <b>{len(rows_db)}</b>\n"]
    if rows_db:
        for r in rows_db:
            text_lines.append(f"• <code>{r['user_id']}</code>")
    else:
        text_lines.append("— список пуст —")
    await cb.message.edit_text(
        "\n".join(text_lines),
        reply_markup=kb(
            [("➕ Добавить", "adm_admins_add"), ("🗑 Удалить", "adm_admins_del")],
            [("‹ Назад", "adm_back"), home_btn()]))
    await cb.answer()


@router.callback_query(F.data == "adm_admins_add")
async def cb_adm_admins_add(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, cb.from_user.id,
        "👮 user_id нового админа:",
        validator=lambda t: t.strip().lstrip("-").isdigit(),
        error_msg="❌ user_id должен быть числом.")
    if not raw:
        return
    user_id = int(raw.strip())
    await db.db_admins_add(user_id)
    await cb.message.answer(f"✅ Админ: <code>{user_id}</code>")


@router.callback_query(F.data == "adm_admins_del")
async def cb_adm_admins_del(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, cb.from_user.id,
        "🗑 user_id админа для удаления:",
        validator=lambda t: t.strip().lstrip("-").isdigit(),
        error_msg="❌ user_id должен быть числом.")
    if not raw:
        return
    user_id = int(raw.strip())
    # Защита от удаления последнего администратора
    all_admins = await db.db_admins_get_all()
    if len(all_admins) <= 1:
        return await cb.message.answer(
            "❌ Нельзя удалить последнего администратора. "
            "Сначала добавьте другого."
        )
    await db.db_admins_remove(user_id)
    await cb.message.answer(f"🗑 Админ удалён: <code>{user_id}</code>")


# =================================================================
# 📋 Все аккаунты
# =================================================================
@router.callback_query(F.data == "adm_all_accs")
async def cb_adm_all_accs(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("⛔", show_alert=True)
    rows_db = await db.db_get_all_accounts()
    if not rows_db:
        text = "📋 <b>Все аккаунты</b>\n━━━━━━━━━━━━━━━━━━━\nАккаунтов нет."
    else:
        by_owner: Dict[int, List[Dict[str, Any]]] = {}
        for a in rows_db:
            by_owner.setdefault(a.get("owner_id") or 0, []).append(a)
        lines = [
            f"📋 <b>Все аккаунты</b>\n━━━━━━━━━━━━━━━━━━━\n"
            f"Всего: <b>{len(rows_db)}</b>  ·  Пользователей: <b>{len(by_owner)}</b>"
        ]
        for owner_id, accs in by_owner.items():
            lines.append(f"\n👤 <code>{owner_id}</code> — {len(accs)} аккаунтов:")
            for a in accs[:15]:
                grp = f" 📁{a['grp']}" if a.get("grp") else ""
                lines.append(f"  • {a['phone']} (@{a.get('username') or '—'}){grp}")
            if len(accs) > 15:
                lines.append(f"  …ещё {len(accs) - 15}")
        text = "\n".join(lines)
    if len(text) > 3800:
        text = text[:3800] + "\n…(обрезано)…"
    await cb.message.edit_text(text, reply_markup=kb([("‹ Назад", "adm_back"), home_btn()]))
    await cb.answer()
