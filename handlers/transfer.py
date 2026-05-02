# -*- coding: utf-8 -*-
"""
handlers/transfer.py — Передача аккаунтов между пользователями
через одноразовые токен-ссылки.
"""

import logging
import secrets
from typing import List

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.exceptions import TelegramBadRequest

import db
import bot_globals as _bg
from bot_globals import (
    bot, store, ar_manager,
    _trf_selection, _transfer_pending, _TRF_SEL_PER_PAGE,
    _trf_grp_selection, _grp_index_cache,
    notify_owner, kb, home_btn,
)
from utils import restore_main_menu
from handlers.helpers import (
    _send_target_picker,
    _resolve_targets_all, _resolve_targets_manual,
)

log = logging.getLogger("transfer")
router = Router(name="transfer")


@router.callback_query(F.data == "acc_transfer")
async def cb_acc_transfer(cb: CallbackQuery):
    uid = cb.from_user.id
    accs = await db.db_get_accounts_by_owner(uid)
    if not accs:
        return await cb.answer("У вас нет аккаунтов для передачи.", show_alert=True)
    await cb.answer()
    await _send_target_picker(
        cb.message.chat.id, "trf_t",
        "🔄 <b>Передача аккаунтов</b>\n\n"
        "Выберите аккаунты, которые хотите передать другому пользователю.\n\n"
        "⚠️ При передаче:\n"
        "  • Группа и личный прокси очистятся\n"
        "  • Все задачи LDV/XO/автоответы сбросятся\n"
        "  • Вы получите уведомление когда получатель примет",
    )


async def _trf_show_preview(cb: CallbackQuery, uid: int, phones: List[str]) -> None:
    _transfer_pending[uid] = phones
    preview = "\n".join(f"  • <code>{ph}</code>" for ph in phones[:20])
    if len(phones) > 20:
        preview += f"\n  …и ещё {len(phones) - 20}"
    await bot.send_message(
        cb.message.chat.id,
        f"🔄 <b>Передача аккаунтов</b>\n━━━━━━━━━━━━━━━━━━━\n"
        f"Выбрано: <b>{len(phones)}</b>\n\n{preview}\n\nСоздать одноразовую ссылку передачи?",
        reply_markup=kb([("🔗 Создать ссылку", "trf_create")], [("❌ Отмена", "action_cancel")]),
    )


async def _render_trf_selector(cb: CallbackQuery, uid: int, page: int) -> None:
    accs = await db.db_get_accounts_by_owner(uid)
    if not accs:
        await cb.message.answer("❌ У вас нет аккаунтов.")
        return
    selected = _trf_selection.setdefault(uid, set())
    total = len(accs)
    pages = max(1, (total + _TRF_SEL_PER_PAGE - 1) // _TRF_SEL_PER_PAGE)
    page = max(0, min(page, pages - 1))
    chunk = accs[page * _TRF_SEL_PER_PAGE:(page + 1) * _TRF_SEL_PER_PAGE]
    rows = []
    for a in chunk:
        ph = a["phone"]
        un = f" (@{a['username']})" if a.get("username") else ""
        grp = f" 📁{a['grp']}" if a.get("grp") else ""
        icon = "✅" if ph in selected else "⬜"
        rows.append([(f"{icon} {ph}{un}{grp}", f"trf_tog:{ph}:{page}")])
    nav = []
    if page > 0:
        nav.append(("◀️", f"trf_sel:{page - 1}"))
    nav.append((f"{page + 1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append(("▶️", f"trf_sel:{page + 1}"))
    if nav:
        rows.append(nav)
    n_sel = len(selected)
    if n_sel > 0:
        rows.append([(f"✅ Подтвердить ({n_sel} выбрано)", "trf_sel_confirm")])
    rows.append([("❌ Отмена", "action_cancel")])
    text = (
        f"📋 <b>Выбор аккаунтов</b>\n━━━━━━━━━━━━━━━━━━━\n"
        f"Всего: <b>{total}</b>  ·  Выбрано: <b>{n_sel}</b>  ·  "
        f"Стр. {page + 1}/{pages}\n\nНажмите на аккаунт чтобы отметить/снять:"
    )
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))


async def _render_trf_grp_selector(cb: CallbackQuery, uid: int) -> None:
    """Отображает список групп с чекбоксами для мультивыбора (передача аккаунтов)."""
    groups = await db.db_get_groups_by_owner(uid)
    if not groups:
        text = "📁 <b>У вас нет групп.</b>\nСначала создайте группы для аккаунтов."
        try:
            await cb.message.edit_text(text, reply_markup=kb([home_btn()]))
        except TelegramBadRequest:
            await bot.send_message(cb.message.chat.id, text,
                                   reply_markup=kb([home_btn()]))
        return

    # Обновляем кэш групп
    _grp_index_cache[uid] = groups
    selected = _trf_grp_selection.setdefault(uid, set())
    n_grp = len(groups)
    n_sel = len(selected)
    rows = []

    # Кнопка «Выбрать все» / «Снять все»
    all_selected = n_sel == n_grp
    rows.append([(
        "☑️ Снять все группы" if all_selected else "✅ Выбрать все группы",
        "trf_grp_all"
    )])

    # Строка на каждую группу
    for i, g in enumerate(groups):
        icon = "✅" if g in selected else "⬜"
        rows.append([(f"{icon} 📁 {g}", f"trf_grp_tog:{i}")])

    # Кнопка подтверждения (только если что-то выбрано)
    if n_sel > 0:
        rows.append([(f"🔄 Передать ({n_sel} гр.)", "trf_grp_confirm")])
    rows.append([("❌ Отмена", "action_cancel")])

    text = (
        f"📁 <b>Выбор групп для передачи</b>\n━━━━━━━━━━━━━━━━━━━\n"
        f"Групп всего: <b>{n_grp}</b>  ·  Выбрано: <b>{n_sel}</b>\n\n"
        f"Отметьте группы — все аккаунты из них войдут в одну ссылку передачи:"
    )
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await bot.send_message(cb.message.chat.id, text, reply_markup=kb(*rows))


@router.callback_query(F.data.startswith("trf_t:"))
async def cb_trf_t(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    mode = parts[1]
    if mode == "man":
        _trf_selection.pop(uid, None)
        await cb.answer()
        try:
            await cb.message.edit_text(
                "✏️ <b>Ручной выбор аккаунтов</b>\n\nКак хотите выбрать?",
                reply_markup=kb([("✏️ Ввести номера", "trf_man_type")],
                                [("📋 Выбрать из списка", "trf_sel:0")],
                                [("❌ Отмена", "action_cancel")]))
        except TelegramBadRequest:
            await cb.message.answer(
                "✏️ <b>Ручной выбор аккаунтов</b>\n\nКак хотите выбрать?",
                reply_markup=kb([("✏️ Ввести номера", "trf_man_type")],
                                [("📋 Выбрать из списка", "trf_sel:0")],
                                [("❌ Отмена", "action_cancel")]))
        return
    if mode == "all":
        await cb.answer()
        targets = await _resolve_targets_all(uid)
        if not targets:
            return await bot.send_message(cb.message.chat.id,
                                          "❌ Аккаунтов для передачи нет.")
        await _trf_show_preview(cb, uid, [a["phone"] for a in targets])
    elif mode == "grp":
        await cb.answer()
        _trf_grp_selection.pop(uid, None)   # сброс предыдущего выбора групп
        await _render_trf_grp_selector(cb, uid)
    else:
        await cb.answer("Bad", show_alert=True)


@router.callback_query(F.data.startswith("trf_grp_tog:"))
async def cb_trf_grp_tog(cb: CallbackQuery):
    """Переключает выбор одной группы (чекбокс)."""
    uid = cb.from_user.id
    try:
        gi = int(cb.data.split(":", 1)[1])
    except Exception:
        return await cb.answer("Bad", show_alert=True)
    groups = _grp_index_cache.get(uid, [])
    if not (0 <= gi < len(groups)):
        return await cb.answer("Группа не найдена.", show_alert=True)
    grp = groups[gi]
    selected = _trf_grp_selection.setdefault(uid, set())
    if grp in selected:
        selected.discard(grp)
    else:
        selected.add(grp)
    await cb.answer()
    await _render_trf_grp_selector(cb, uid)


@router.callback_query(F.data == "trf_grp_all")
async def cb_trf_grp_all(cb: CallbackQuery):
    """Выбирает все группы / снимает все."""
    uid = cb.from_user.id
    groups = _grp_index_cache.get(uid, [])
    if not groups:
        return await cb.answer("Нет групп.", show_alert=True)
    selected = _trf_grp_selection.setdefault(uid, set())
    if len(selected) == len(groups):
        selected.clear()          # снять все
    else:
        selected.update(groups)   # выбрать все
    await cb.answer()
    await _render_trf_grp_selector(cb, uid)


@router.callback_query(F.data == "trf_grp_confirm")
async def cb_trf_grp_confirm(cb: CallbackQuery):
    """Подтверждает выбор групп — собирает все аккаунты и показывает превью."""
    uid = cb.from_user.id
    selected_groups = _trf_grp_selection.pop(uid, set())
    if not selected_groups:
        return await cb.answer("Не выбрано ни одной группы.", show_alert=True)
    await cb.answer()

    # Собираем аккаунты из всех выбранных групп, убираем дубли
    phones_seen: set = set()
    targets = []
    for grp in sorted(selected_groups):
        for acc in await db.db_get_accounts_by_group(uid, grp):
            if acc["phone"] not in phones_seen:
                phones_seen.add(acc["phone"])
                targets.append(acc)

    if not targets:
        return await bot.send_message(
            cb.message.chat.id,
            "❌ В выбранных группах нет аккаунтов."
        )
    targets.sort(key=lambda a: a["phone"])
    await _trf_show_preview(cb, uid, [a["phone"] for a in targets])


@router.callback_query(F.data == "trf_man_type")
async def cb_trf_man_type(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    targets = await _resolve_targets_manual(uid, cb.message.chat.id)
    if not targets:
        return await bot.send_message(uid, "❌ Аккаунтов не найдено.")
    await _trf_show_preview(cb, uid, [a["phone"] for a in targets])


@router.callback_query(F.data.startswith("trf_sel:"))
async def cb_trf_sel(cb: CallbackQuery):
    uid = cb.from_user.id
    try:
        page = int(cb.data.split(":", 1)[1])
    except Exception:
        page = 0
    await cb.answer()
    await _render_trf_selector(cb, uid, page)


@router.callback_query(F.data.startswith("trf_tog:"))
async def cb_trf_tog(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    phone = parts[1]
    try:
        page = int(parts[2])
    except Exception:
        page = 0
    selected = _trf_selection.setdefault(uid, set())
    if phone in selected:
        selected.discard(phone)
    else:
        selected.add(phone)
    await cb.answer()
    await _render_trf_selector(cb, uid, page)


@router.callback_query(F.data == "trf_sel_confirm")
async def cb_trf_sel_confirm(cb: CallbackQuery):
    uid = cb.from_user.id
    selected = _trf_selection.pop(uid, set())
    if not selected:
        return await cb.answer("Не выбрано ни одного аккаунта.", show_alert=True)
    await cb.answer()
    await _trf_show_preview(cb, uid, sorted(selected))


@router.callback_query(F.data == "trf_create")
async def cb_trf_create(cb: CallbackQuery):
    uid = cb.from_user.id
    phones = _transfer_pending.pop(uid, None)
    if not phones:
        return await cb.answer("Сессия передачи устарела — начните заново.", show_alert=True)
    token = secrets.token_urlsafe(16)
    await db.db_transfer_create(token, uid, phones)
    bot_un = _bg._bot_username or "бот"
    link = f"https://t.me/{bot_un}?start=tr_{token}"
    await cb.answer()
    try:
        await cb.message.edit_text(
            f"🔗 <b>Ссылка передачи создана</b>\n━━━━━━━━━━━━━━━━━━━\n"
            f"Аккаунтов: <b>{len(phones)}</b>\n\nОтправьте получателю эту ссылку:\n"
            f"<code>{link}</code>\n\n"
            f"⚠️ Ссылка <b>одноразовая</b> — после принятия сгорает.\n"
            f"Если получатель откажется — аккаунты остаются у вас.",
            reply_markup=kb([home_btn()]))
    except Exception:
        await cb.message.answer(f"🔗 <b>Ссылка передачи:</b>\n<code>{link}</code>",
                                reply_markup=kb([home_btn()]))


async def _handle_transfer_incoming(msg: Message, uid: int, token: str) -> None:
    rec = await db.db_transfer_get(token)
    if not rec:
        await msg.answer("❌ Ссылка передачи недействительна или уже использована.")
        await restore_main_menu(bot, msg.chat.id, uid)
        return
    from_uid = rec["from_uid"]
    phones: List[str] = rec["phones"]
    if from_uid == uid:
        await msg.answer("⚠️ Нельзя передать аккаунты самому себе.")
        await restore_main_menu(bot, msg.chat.id, uid)
        return
    valid = []
    for ph in phones:
        a = await db.db_get_account(ph)
        if a and a.get("owner_id") == from_uid:
            valid.append(ph)
    if not valid:
        await db.db_transfer_delete(token)
        await msg.answer("❌ Все аккаунты из этой ссылки уже недоступны\n(удалены или ранее переданы).")
        await restore_main_menu(bot, msg.chat.id, uid)
        return
    preview = "\n".join(f"  • <code>{ph}</code>" for ph in valid[:20])
    if len(valid) > 20:
        preview += f"\n  …и ещё {len(valid) - 20}"
    await msg.answer(
        f"📦 <b>Вам предлагают аккаунты</b>\n━━━━━━━━━━━━━━━━━━━\n"
        f"Количество: <b>{len(valid)}</b>\n\n{preview}\n\n"
        f"⚠️ Все задачи LDV/XO/автоответы будут сброшены.\nПринять передачу?",
        reply_markup=kb([("✅ Принять", f"trf_accept:{token}")],
                        [("❌ Отклонить", f"trf_decline:{token}")]))


@router.callback_query(F.data.startswith("trf_accept:"))
async def cb_trf_accept(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    rec = await db.db_transfer_get(token)
    if not rec:
        await cb.answer("❌ Ссылка уже использована.", show_alert=True)
        try:
            await cb.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return
    from_uid: int = rec["from_uid"]
    phones: List[str] = rec["phones"]
    await db.db_transfer_delete(token)
    await cb.answer("⏳ Принимаю аккаунты…")
    ok = []; skipped = []
    for ph in phones:
        a = await db.db_get_account(ph)
        if not a or a.get("owner_id") != from_uid:
            skipped.append(ph)
            continue
        try:
            await ar_manager.stop(ph)
        except Exception:
            pass
        xo_task = store.xo_liking_tasks.pop(ph, None)
        if xo_task and not xo_task.done():
            xo_task.cancel()
        store.cancelled_phones.add(ph)
        store.paused_phones.discard(ph)
        await db.db_transfer_account(ph, uid)
        ok.append(ph)
    for ph in ok:
        if ph not in store.current_liking_phones:
            store.cancelled_phones.discard(ph)
    ok_text = "\n".join(f"  ✅ <code>{ph}</code>" for ph in ok)
    skip_text = "\n".join(f"  ❌ <code>{ph}</code>" for ph in skipped)
    result_text = f"📦 <b>Передача завершена</b>\n━━━━━━━━━━━━━━━━━━━\nПринято: <b>{len(ok)}</b>"
    if ok_text:
        result_text += f"\n{ok_text}"
    if skipped:
        result_text += f"\n\nНедоступных (удалены/уже переданы): <b>{len(skipped)}</b>\n{skip_text}"
    try:
        await cb.message.edit_text(result_text, reply_markup=None)
    except Exception:
        await cb.message.answer(result_text)
    await restore_main_menu(bot, cb.message.chat.id, uid)
    try:
        notif = (f"📦 <b>Передача принята</b>\nПользователь <code>{uid}</code> принял "
                 f"<b>{len(ok)}</b> аккаунт(ов).")
        if skipped:
            notif += f"\n⚠️ Недоступных: {len(skipped)}"
        await notify_owner(from_uid, notif)
    except Exception:
        pass


@router.callback_query(F.data.startswith("trf_decline:"))
async def cb_trf_decline(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    # Читаем до удаления, чтобы уведомить отправителя
    rec = await db.db_transfer_get(token)
    await db.db_transfer_delete(token)
    await cb.answer("Передача отклонена.")
    try:
        await cb.message.edit_text("❌ Передача аккаунтов отклонена.")
    except Exception:
        pass
    await restore_main_menu(bot, cb.message.chat.id, cb.from_user.id)
    # Уведомляем отправителя
    if rec:
        try:
            n = len(rec.get("phones") or [])
            await notify_owner(
                rec["from_uid"],
                f"❌ Пользователь <code>{cb.from_user.id}</code> отклонил передачу "
                f"<b>{n}</b> аккаунт(ов). Аккаунты остаются у вас."
            )
        except Exception:
            pass
