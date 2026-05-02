# -*- coding: utf-8 -*-
"""
handlers/manage.py — Управление: ручной пролайк LDV/XO, задачи,
отмена регистраций.
"""

import logging
import re
import time
from typing import List

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.exceptions import TelegramBadRequest

import db
from bot_globals import (
    bot, store, _grp_index_cache,
    notify_owner, kb, home_btn,
)
from utils import restore_main_menu, ask_with_retry, validate_phone

log = logging.getLogger("manage")
router = Router(name="manage")


# ── alias на XO рега из раздела Управление ──
@router.callback_query(F.data == "mng_xo")
async def cb_mng_xo(cb: CallbackQuery):
    from handlers.automation import cb_auto_xo
    await cb_auto_xo(cb)


# =================================================================
# ❤️ Ручной пролайк LDV
# =================================================================
@router.callback_query(F.data == "mng_manual_ldv")
async def cb_mng_manual_ldv(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "❤️ Пришлите номера для немедленного пролайка ДВ (через запятую или с новой строки):",
        validator=lambda t: any(validate_phone(tok) for tok in re.split(r"[,\n;\s]+", t) if tok),
        error_msg="❌ Не нашёл валидных номеров. Формат: +79991112233")
    if not raw:
        return
    phones = [p for tok in re.split(r"[,\n;\s]+", raw) if (p := validate_phone(tok))]
    n = 0; not_found = []
    for ph in phones:
        a = await db.db_get_account(ph)
        if a:
            owner = a.get("owner_id") or uid
            await db.db_schedule_ldv_task(ph, owner, time.time() + 2, step=0, status="pending")
            store.cancelled_phones.discard(ph)
            store.paused_phones.discard(ph)
            n += 1
        else:
            not_found.append(ph)
    text = f"✅ Запланировано ДВ: {n}."
    if not_found:
        text += f"\n⚠️ Не найдены в БД: {', '.join(not_found)}"
    await cb.message.answer(text)


# =================================================================
# 💘 Ручной пролайк XO
# =================================================================
@router.callback_query(F.data == "mng_manual_xo")
async def cb_mng_manual_xo(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "💘 Пришлите номера для немедленного пролайка XO (через запятую или с новой строки):",
        validator=lambda t: any(validate_phone(tok) for tok in re.split(r"[,\n;\s]+", t) if tok),
        error_msg="❌ Не нашёл валидных номеров. Формат: +79991112233")
    if not raw:
        return
    phones = [p for tok in re.split(r"[,\n;\s]+", raw) if (p := validate_phone(tok))]
    n = 0; not_found = []
    for ph in phones:
        a = await db.db_get_account(ph)
        if a:
            owner = a.get("owner_id") or uid
            await db.db_schedule_xo_task(ph, owner, time.time() + 2, status="pending")
            store.xo_liking_paused.discard(ph)
            n += 1
        else:
            not_found.append(ph)
    text = f"✅ Запланировано XO: {n}."
    if not_found:
        text += f"\n⚠️ Не найдены в БД: {', '.join(not_found)}"
    await cb.message.answer(text)


# =================================================================
# ⚙️ Задачи LDV
# =================================================================
@router.callback_query(F.data == "mng_ldv")
async def cb_mng_ldv(cb: CallbackQuery):
    await cb.message.edit_text(
        "🤖 <b>Управление лайкингом LDV</b>\n\n"
        "Просматривайте активные циклы, ставьте на паузу или удаляйте задачи.",
        reply_markup=kb(
            [("📋 Активные циклы", "mng_ldv_list:0")],
            [("🗑 Сбросить все", "mng_ldv_resetall")],
            [("📁 Сбросить по группе", "mng_ldv_resetgrp"),
             ("🎯 Сбросить выборочно", "mng_ldv_resetman")],
            [("‹ Назад", "back_manage"), home_btn()],
        ))
    await cb.answer()


@router.callback_query(F.data == "back_manage")
async def cb_back_manage(cb: CallbackQuery):
    await cb.message.edit_text(
        "📊 <b>Управление</b>\n\nРучной запуск лайкинга, управление задачами и отмена регистраций.",
        reply_markup=kb(
            [("❤️ Пролайк LDV", "mng_manual_ldv"), ("💘 Пролайк XO", "mng_manual_xo")],
            [("⚙️ Задачи LDV", "mng_ldv"), ("💘 Задачи XO", "mng_xo_panel")],
            [("🛑 Отмена регистрации", "mng_regcancel")],
            [home_btn()],
        ))
    await cb.answer()


@router.callback_query(F.data == "mng_xo_panel")
async def cb_mng_xo_panel(cb: CallbackQuery):
    await _xo_manage_render(cb.message.chat.id, cb.from_user.id, edit_msg=cb.message)
    await cb.answer()


async def _render_ldv_list(cb: CallbackQuery, page: int = 0) -> None:
    uid = cb.from_user.id
    tasks = await db.db_get_ldv_tasks_by_owner(uid)
    if not tasks:
        await cb.message.edit_text(
            "📋 <b>LDV-циклы</b>\n━━━━━━━━━━━━━━━━━━━\nАктивных циклов нет.",
            reply_markup=kb([("‹ Назад", "mng_ldv")]))
        return
    per = 8
    total = len(tasks)
    pages = max(1, (total + per - 1) // per)
    page = max(0, min(page, pages - 1))
    chunk = tasks[page * per:(page + 1) * per]
    n_run  = sum(1 for t in tasks if t["phone"] in store.current_liking_phones)
    n_paus = sum(1 for t in tasks if t["phone"] in store.paused_phones)
    lines = [f"📋 <b>LDV-циклы</b>\n━━━━━━━━━━━━━━━━━━━\n"
             f"Задач: <b>{total}</b>  ·  ▶️ активных: {n_run}  ·  ⏸ пауза: {n_paus}\n"]
    rows = []
    for t in chunk:
        ph = t["phone"]; st = t["status"]
        nxt = time.strftime("%d.%m %H:%M", time.localtime(t["next_run"] or 0))
        is_paused = ph in store.paused_phones
        running_icon = ("▶️" if ph in store.current_liking_phones
                        else ("⏸" if is_paused else "⏳"))
        lines.append(f"{running_icon} {ph} — {st}  /  next: {nxt}  /  шаг {t['step']}")
        rows.append([
            (("▶️ Resume" if is_paused else "⏸ Pause"), f"mng_ldv_pp:{ph}"),
            ("🗑 Удалить", f"mng_ldv_del:{ph}"),
        ])
    nav = []
    if page > 0:   nav.append(("◀️", f"mng_ldv_list:{page-1}"))
    nav.append((f"{page+1}/{pages}", "noop"))
    if page < pages - 1: nav.append(("▶️", f"mng_ldv_list:{page+1}"))
    if nav: rows.append(nav)
    rows.append([("‹ Назад", "mng_ldv")])
    await cb.message.edit_text("\n".join(lines), reply_markup=kb(*rows))


@router.callback_query(F.data.startswith("mng_ldv_list:"))
async def cb_mng_ldv_list(cb: CallbackQuery):
    try:
        page = int(cb.data.split(":", 1)[1])
    except Exception:
        page = 0
    await _render_ldv_list(cb, page)
    await cb.answer()


@router.callback_query(F.data.startswith("mng_ldv_pp:"))
async def cb_mng_ldv_pp(cb: CallbackQuery):
    ph = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    tasks = await db.db_get_ldv_tasks_by_owner(uid)
    if not any(t["phone"] == ph for t in tasks):
        return await cb.answer("Задача не найдена.", show_alert=True)
    if ph in store.paused_phones:
        store.paused_phones.discard(ph)
        await cb.answer("▶️ Возобновлён.")
    else:
        store.paused_phones.add(ph)
        await cb.answer("⏸ На паузе.")
    await _render_ldv_list(cb, page=0)


@router.callback_query(F.data.startswith("mng_ldv_del:"))
async def cb_mng_ldv_del(cb: CallbackQuery):
    ph = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    tasks = await db.db_get_ldv_tasks_by_owner(uid)
    if not any(t["phone"] == ph for t in tasks):
        return await cb.answer("Задача не найдена.", show_alert=True)
    store.cancelled_phones.add(ph)
    await db.db_delete_ldv_task(ph)
    await cb.answer("🗑 Задача удалена.")
    await _render_ldv_list(cb, page=0)


@router.callback_query(F.data == "mng_ldv_resetall")
async def cb_mng_ldv_resetall(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    confirm = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "⚠️ <b>Удалить ВСЕ LDV-задачи?</b>\n\n"
        "Все циклы лайкинга будут остановлены и удалены из базы.\n"
        "Для подтверждения напишите <b>ДА</b>:",
        validator=lambda t: t.strip().lower() == "да",
        error_msg='❌ Напишите именно "ДА" для подтверждения.',
        parse_mode="HTML")
    if not confirm:
        return await cb.message.answer("✅ Отменено.")
    tasks = await db.db_get_ldv_tasks_by_owner(uid)
    for t in tasks:
        store.cancelled_phones.add(t["phone"])
    n = await db.db_delete_ldv_tasks_by_owner(uid)
    await cb.message.answer(f"🗑 Удалено: {n}")


@router.callback_query(F.data == "mng_ldv_resetgrp")
async def cb_mng_ldv_resetgrp(cb: CallbackQuery):
    uid = cb.from_user.id
    groups = await db.db_get_groups_by_owner(uid)
    if not groups:
        return await cb.answer("Групп нет.", show_alert=True)
    _grp_index_cache[uid] = groups
    rows = [[(f"📁 {g}", f"mng_ldv_grpdel:{i}")] for i, g in enumerate(groups[:30])]
    rows.append([("‹ Назад", "mng_ldv")])
    await cb.message.edit_text("📁 Выберите группу:", reply_markup=kb(*rows))
    await cb.answer()


@router.callback_query(F.data.startswith("mng_ldv_grpdel:"))
async def cb_mng_ldv_grpdel(cb: CallbackQuery):
    uid = cb.from_user.id
    gi = int(cb.data.split(":", 1)[1])
    groups = _grp_index_cache.get(uid, [])
    if not (0 <= gi < len(groups)):
        return await cb.answer("Bad", show_alert=True)
    accs = await db.db_get_accounts_by_group(uid, groups[gi])
    for a in accs:
        store.cancelled_phones.add(a["phone"])
    n = await db.db_delete_ldv_tasks_by_group(uid, groups[gi])
    await cb.answer(f"🗑 Удалено: {n}", show_alert=True)


@router.callback_query(F.data == "mng_ldv_resetman")
async def cb_mng_ldv_resetman(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "🎯 Пришлите номера для удаления LDV-задач:",
        validator=lambda t: any(validate_phone(tok) for tok in re.split(r"[,\n;\s]+", t) if tok),
        error_msg="❌ Не нашёл валидных номеров. Формат: +79991112233")
    if not raw:
        return
    n = 0
    for tok in re.split(r"[,\n;\s]+", raw):
        p = validate_phone(tok)
        if p:
            store.cancelled_phones.add(p)
            await db.db_delete_ldv_task(p)
            n += 1
    await cb.message.answer(f"🗑 Удалено: {n}")


# =================================================================
# 💘 Управление XO
# =================================================================
@router.message(Command("xo_manage"))
async def cmd_xo_manage(msg: Message):
    await _xo_manage_render(msg.chat.id, msg.from_user.id)


async def _xo_manage_render(chat_id: int, uid: int, edit_msg=None):
    tasks = await db.db_get_xo_tasks_by_owner(uid)
    if not tasks:
        text = "💘 <b>Управление XO</b>\n━━━━━━━━━━━━━━━━━━━\nАктивных задач нет."
        rows = [[("‹ Назад", "back_manage"), home_btn()]]
    else:
        n_run  = sum(1 for t in tasks if t["status"] == "running")
        n_paus = sum(1 for t in tasks if t["status"] == "paused")
        text_lines = [
            f"💘 <b>Управление XO</b>\n━━━━━━━━━━━━━━━━━━━\n"
            f"Задач: <b>{len(tasks)}</b>  ·  ▶️ активных: {n_run}  ·  ⏸ пауза: {n_paus}\n"
        ]
        rows = []
        for t in tasks[:30]:
            ph = t["phone"]; st = t["status"]
            nxt = time.strftime("%d.%m %H:%M", time.localtime(t["next_run"] or 0))
            is_paused = ph in store.xo_liking_paused
            paused_icon = "⏸" if is_paused else "▶️"
            text_lines.append(f"{paused_icon} {ph} — {st}  /  next: {nxt}")
            rows.append([
                (("▶️ Resume" if is_paused else "⏸ Pause"), f"mng_xo_pp:{ph}"),
                ("🛑 Стоп", f"mng_xo_stop:{ph}"),
                ("🗑 Удалить", f"mng_xo_del:{ph}"),
            ])
        rows.append([("‹ Назад", "back_manage"), home_btn()])
        text = "\n".join(text_lines)
    if edit_msg:
        try:
            await edit_msg.edit_text(text, reply_markup=kb(*rows))
            return
        except TelegramBadRequest:
            pass
    await bot.send_message(chat_id, text, reply_markup=kb(*rows))


@router.callback_query(F.data.startswith("mng_xo_pp:"))
async def cb_mng_xo_pp(cb: CallbackQuery):
    ph = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    tasks = await db.db_get_xo_tasks_by_owner(uid)
    if not any(t["phone"] == ph for t in tasks):
        return await cb.answer("Задача не найдена.", show_alert=True)
    if ph in store.xo_liking_paused:
        store.xo_liking_paused.discard(ph)
        await db.db_update_xo_task(ph, status="running")
        await cb.answer("▶️ Возобновлён.")
    else:
        store.xo_liking_paused.add(ph)
        await db.db_update_xo_task(ph, status="paused")
        await cb.answer("⏸ На паузе.")
    await _xo_manage_render(cb.message.chat.id, cb.from_user.id, edit_msg=cb.message)


@router.callback_query(F.data.startswith("mng_xo_stop:"))
async def cb_mng_xo_stop(cb: CallbackQuery):
    ph = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    tasks = await db.db_get_xo_tasks_by_owner(uid)
    if not any(t["phone"] == ph for t in tasks):
        return await cb.answer("Задача не найдена.", show_alert=True)
    t = store.xo_liking_tasks.pop(ph, None)
    if t and not t.done():
        t.cancel()
    await db.db_update_xo_task(ph, status="stopped")
    await cb.answer("🛑 Остановлен.")
    await _xo_manage_render(cb.message.chat.id, cb.from_user.id, edit_msg=cb.message)


@router.callback_query(F.data.startswith("mng_xo_del:"))
async def cb_mng_xo_del(cb: CallbackQuery):
    ph = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    tasks = await db.db_get_xo_tasks_by_owner(uid)
    if not any(t["phone"] == ph for t in tasks):
        return await cb.answer("Задача не найдена.", show_alert=True)
    t = store.xo_liking_tasks.pop(ph, None)
    if t and not t.done():
        t.cancel()
    await db.db_delete_xo_task(ph)
    await cb.answer("🗑 Удалена.")
    await _xo_manage_render(cb.message.chat.id, cb.from_user.id, edit_msg=cb.message)


# =================================================================
# 🛑 Отмена регистраций
# =================================================================
@router.callback_query(F.data == "mng_regcancel")
async def cb_mng_regcancel(cb: CallbackQuery):
    uid = cb.from_user.id
    pending_ldv = len(store.ldv_reg_cancel)
    pending_xo = len(store.xo_reg_cancel)
    active_ldv_targets = (store.ldv_data.get(uid) or {}).get("targets") or []
    active_xo_targets  = (store.xo_data.get(uid) or {}).get("targets") or []
    text = (
        "🛑 <b>Отмена регистрации</b>\n━━━━━━━━━━━━━━━━━━━\n"
        f"🤖  LDV — активная партия:  <b>{len(active_ldv_targets)}</b>\n"
        f"💘  XO  — активная партия:  <b>{len(active_xo_targets)}</b>\n\n"
        f"В очереди на отмену:\n    ▸ LDV: <b>{pending_ldv}</b>\n    ▸ XO:  <b>{pending_xo}</b>"
    )
    await cb.message.edit_text(text, reply_markup=kb(
        [("🤖 Отменить LDV", "rc_ldv"), ("💘 Отменить XO", "rc_xo")],
        [("♻️ Очистить стоп-лист", "rc_clear")],
        [("‹ Назад", "back_manage"), home_btn()]))
    await cb.answer()


@router.callback_query(F.data == "rc_clear")
async def cb_rc_clear(cb: CallbackQuery):
    n = len(store.ldv_reg_cancel) + len(store.xo_reg_cancel)
    store.ldv_reg_cancel.clear()
    store.xo_reg_cancel.clear()
    await cb.answer(f"Очищено: {n}", show_alert=True)
    await cb_mng_regcancel(cb)


@router.callback_query(F.data == "rc_ldv")
async def cb_rc_ldv(cb: CallbackQuery):
    await cb.message.edit_text(
        "🛑 <b>Отмена регистрации LDV</b>\n\nВыберите диапазон аккаунтов для отмены:",
        reply_markup=kb(
            [("📋 Все аккаунты", "rc_ldv_all")],
            [("📁 По группе", "rc_ldv_grp"), ("✏️ По номерам", "rc_ldv_man")],
            [("‹ Назад", "mng_regcancel"), home_btn()]))
    await cb.answer()


@router.callback_query(F.data == "rc_ldv_all")
async def cb_rc_ldv_all(cb: CallbackQuery):
    uid = cb.from_user.id
    targets = (store.ldv_data.get(uid) or {}).get("targets") or []
    accs = await db.db_get_accounts_by_owner(uid)
    n = 0
    for ph in targets:
        store.ldv_reg_cancel.add(ph); n += 1
    for a in accs:
        store.ldv_reg_cancel.add(a["phone"])
    await cb.answer(f"Отмена принята: {n} активных, {len(accs)} аккаунтов в стоп-листе.", show_alert=True)


@router.callback_query(F.data == "rc_ldv_grp")
async def cb_rc_ldv_grp(cb: CallbackQuery):
    uid = cb.from_user.id
    groups = await db.db_get_groups_by_owner(uid)
    if not groups:
        return await cb.answer("Групп нет.", show_alert=True)
    _grp_index_cache[uid] = groups
    rows = [[(f"📁 {g}", f"rc_ldv_gi:{i}")] for i, g in enumerate(groups[:30])]
    rows.append([("‹ Назад", "rc_ldv")])
    await cb.message.edit_text("📁 Выберите группу для отмены ЛДВ:", reply_markup=kb(*rows))
    await cb.answer()


@router.callback_query(F.data.startswith("rc_ldv_gi:"))
async def cb_rc_ldv_gi(cb: CallbackQuery):
    uid = cb.from_user.id
    gi = int(cb.data.split(":", 1)[1])
    groups = _grp_index_cache.get(uid, [])
    if not (0 <= gi < len(groups)):
        return await cb.answer("Bad", show_alert=True)
    accs = await db.db_get_accounts_by_group(uid, groups[gi])
    n = 0
    for a in accs:
        store.ldv_reg_cancel.add(a["phone"]); n += 1
    await cb.answer(f"🛑 В группе «{groups[gi]}» отмечено: {n}", show_alert=True)


@router.callback_query(F.data == "rc_ldv_man")
async def cb_rc_ldv_man(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "🛑 Пришлите номера для отмены ЛДВ-регистрации (через запятую или с новой строки):",
        validator=lambda t: any(validate_phone(tok) for tok in re.split(r"[,\n;\s]+", t) if tok),
        error_msg="❌ Не нашёл валидных номеров. Формат: +79991112233")
    if not raw:
        return
    n = 0
    for tok in re.split(r"[,\n;\s]+", raw):
        p = validate_phone(tok)
        if p:
            store.ldv_reg_cancel.add(p); n += 1
    await cb.message.answer(f"🛑 В стоп-лист ЛДВ добавлено: <b>{n}</b>")


@router.callback_query(F.data == "rc_xo")
async def cb_rc_xo(cb: CallbackQuery):
    await cb.message.edit_text(
        "🛑 <b>Отмена регистрации XO</b>\n\nВыберите диапазон аккаунтов для отмены:",
        reply_markup=kb(
            [("📋 Все аккаунты", "rc_xo_all")],
            [("📁 По группе", "rc_xo_grp"), ("✏️ По номерам", "rc_xo_man")],
            [("‹ Назад", "mng_regcancel"), home_btn()]))
    await cb.answer()


@router.callback_query(F.data == "rc_xo_all")
async def cb_rc_xo_all(cb: CallbackQuery):
    uid = cb.from_user.id
    targets = (store.xo_data.get(uid) or {}).get("targets") or []
    accs = await db.db_get_accounts_by_owner(uid)
    n = 0
    for ph in targets:
        store.xo_reg_cancel.add(ph); n += 1
    for a in accs:
        store.xo_reg_cancel.add(a["phone"])
    await cb.answer(f"Отмена принята: {n} активных, {len(accs)} аккаунтов в стоп-листе.", show_alert=True)


@router.callback_query(F.data == "rc_xo_grp")
async def cb_rc_xo_grp(cb: CallbackQuery):
    uid = cb.from_user.id
    groups = await db.db_get_groups_by_owner(uid)
    if not groups:
        return await cb.answer("Групп нет.", show_alert=True)
    _grp_index_cache[uid] = groups
    rows = [[(f"📁 {g}", f"rc_xo_gi:{i}")] for i, g in enumerate(groups[:30])]
    rows.append([("‹ Назад", "rc_xo")])
    await cb.message.edit_text("📁 Выберите группу для отмены XO:", reply_markup=kb(*rows))
    await cb.answer()


@router.callback_query(F.data.startswith("rc_xo_gi:"))
async def cb_rc_xo_gi(cb: CallbackQuery):
    uid = cb.from_user.id
    gi = int(cb.data.split(":", 1)[1])
    groups = _grp_index_cache.get(uid, [])
    if not (0 <= gi < len(groups)):
        return await cb.answer("Bad", show_alert=True)
    accs = await db.db_get_accounts_by_group(uid, groups[gi])
    n = 0
    for a in accs:
        store.xo_reg_cancel.add(a["phone"]); n += 1
    await cb.answer(f"🛑 В группе «{groups[gi]}» отмечено: {n}", show_alert=True)


@router.callback_query(F.data == "rc_xo_man")
async def cb_rc_xo_man(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "🛑 Пришлите номера для отмены XO-регистрации (через запятую или с новой строки):",
        validator=lambda t: any(validate_phone(tok) for tok in re.split(r"[,\n;\s]+", t) if tok),
        error_msg="❌ Не нашёл валидных номеров. Формат: +79991112233")
    if not raw:
        return
    n = 0
    for tok in re.split(r"[,\n;\s]+", raw):
        p = validate_phone(tok)
        if p:
            store.xo_reg_cancel.add(p); n += 1
    await cb.message.answer(f"🛑 В стоп-лист XO добавлено: <b>{n}</b>")
