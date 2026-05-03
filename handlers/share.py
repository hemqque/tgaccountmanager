# -*- coding: utf-8 -*-
"""
handlers/share.py - Общий доступ к аккаунтам между пользователями.

Механизм: владелец создаёт одноразовую ссылку (sh_TOKEN); получатель
переходит по ней и получает совместный доступ к аккаунтам.
Владелец может отозвать доступ в любой момент. Получатель может
самостоятельно отказаться от доступа.

Отличия от transfer:
  - owner_id аккаунта НЕ меняется.
  - Можно поделиться с несколькими пользователями.
  - Ссылка одноразовая, но можно создавать сколько угодно.
  - Shared-пользователь НЕ может удалять/передавать аккаунты.
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
    bot,
    _share_pending, _shr_selection, _shr_grp_selection,
    _SHR_SEL_PER_PAGE, _grp_index_cache,
    notify_owner, kb, home_btn,
)
from utils import restore_main_menu
from handlers.helpers import (
    _send_target_picker,
    _resolve_targets_all, _resolve_targets_manual,
)

log = logging.getLogger("share")
router = Router(name="share")

_SHR_REVOKE_PAGE_SIZE = 10


# =================================================================
# Главное меню общего доступа
# =================================================================
@router.callback_query(F.data == "acc_share")
async def cb_acc_share(cb: CallbackQuery):
    await cb.answer()
    markup = kb(
        [("📤 Поделиться аккаунтами", "shr_create_link")],
        [("📋 Мне поделились",         "shr_mine"),
         ("👥 Я поделился",            "shr_by_me")],
        [("‹ Назад", "action_cancel"), home_btn()],
    )
    text = (
        "🤝 <b>Общий доступ к аккаунтам</b>\n━━━━━━━━━━━━━━━━━━━\n\n"
        "Поделитесь аккаунтами с другим пользователем. "
        "Он получит доступ к управлению, но <b>не сможет</b> "
        "удалять или передавать аккаунты.\n\n"
        "Вы в любой момент можете отозвать доступ."
    )
    try:
        await cb.message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest:
        await bot.send_message(cb.message.chat.id, text, reply_markup=markup)


# =================================================================
# Создание ссылки — выбор аккаунтов
# =================================================================
@router.callback_query(F.data == "shr_create_link")
async def cb_shr_create_link(cb: CallbackQuery):
    uid = cb.from_user.id
    accs = await db.db_get_accounts_by_owner(uid)
    if not accs:
        return await cb.answer("У вас нет аккаунтов для шаринга.", show_alert=True)
    await cb.answer()
    await _send_target_picker(
        cb.message.chat.id, "shr_t",
        "🤝 <b>Общий доступ — выбор аккаунтов</b>\n\n"
        "Выберите аккаунты для совместного доступа.\n"
        "Получатель сможет управлять ими, но <b>не сможет</b> удалить или передать.",
    )


async def _shr_show_preview(cb: CallbackQuery, uid: int,
                            phones: List[str]) -> None:
    _share_pending[uid] = phones
    lines = [f"  <code>{ph}</code>" for ph in phones[:20]]
    preview = "\n".join(lines)
    if len(phones) > 20:
        preview += f"\n  ...и ещё {len(phones) - 20}"
    await bot.send_message(
        cb.message.chat.id,
        f"🤝 <b>Общий доступ — подтверждение</b>\n━━━━━━━━━━━━━━━━━━━\n"
        f"Аккаунтов: <b>{len(phones)}</b>\n\n{preview}\n\n"
        f"Создать одноразовую ссылку-приглашение?",
        reply_markup=kb(
            [("🔗 Создать ссылку", "shr_create")],
            [("❌ Отмена", "action_cancel")],
        ),
    )


async def _render_shr_selector(cb: CallbackQuery, uid: int, page: int) -> None:
    accs = await db.db_get_accounts_by_owner(uid)
    if not accs:
        await cb.message.answer("❌ У вас нет аккаунтов.")
        return
    selected = _shr_selection.setdefault(uid, set())
    total = len(accs)
    pages = max(1, (total + _SHR_SEL_PER_PAGE - 1) // _SHR_SEL_PER_PAGE)
    page = max(0, min(page, pages - 1))
    chunk = accs[page * _SHR_SEL_PER_PAGE:(page + 1) * _SHR_SEL_PER_PAGE]
    rows = []
    for a in chunk:
        ph = a["phone"]
        un = f" (@{a['username']})" if a.get("username") else ""
        grp = f" 📁{a['grp']}" if a.get("grp") else ""
        icon = "✅" if ph in selected else "⬜"
        rows.append([(f"{icon} {ph}{un}{grp}", f"shr_tog:{ph}:{page}")])
    nav = []
    if page > 0:
        nav.append(("◀️", f"shr_sel:{page - 1}"))
    nav.append((f"{page + 1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append(("▶️", f"shr_sel:{page + 1}"))
    if nav:
        rows.append(nav)
    n_sel = len(selected)
    if n_sel > 0:
        rows.append([(f"✅ Подтвердить ({n_sel} выбрано)", "shr_sel_confirm")])
    rows.append([("❌ Отмена", "action_cancel")])
    text = (
        f"📋 <b>Выбор аккаунтов для шаринга</b>\n━━━━━━━━━━━━━━━━━━━\n"
        f"Всего: <b>{total}</b>  ·  Выбрано: <b>{n_sel}</b>  ·  "
        f"Стр. {page + 1}/{pages}\n\nНажмите аккаунт чтобы отметить/снять:"
    )
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))


async def _render_shr_grp_selector(cb: CallbackQuery, uid: int) -> None:
    groups = await db.db_get_groups_by_owner(uid)
    if not groups:
        text = "📁 <b>У вас нет групп.</b>"
        try:
            await cb.message.edit_text(text, reply_markup=kb([home_btn()]))
        except TelegramBadRequest:
            await bot.send_message(cb.message.chat.id, text,
                                   reply_markup=kb([home_btn()]))
        return
    _grp_index_cache[uid] = groups
    selected = _shr_grp_selection.setdefault(uid, set())
    n_grp = len(groups)
    n_sel = len(selected)
    rows = []
    all_sel = n_sel == n_grp
    rows.append([(
        "☑️ Снять все группы" if all_sel else "✅ Выбрать все группы",
        "shr_grp_all",
    )])
    for i, g in enumerate(groups):
        icon = "✅" if g in selected else "⬜"
        rows.append([(f"{icon} 📁 {g}", f"shr_grp_tog:{i}")])
    if n_sel > 0:
        rows.append([(f"🤝 Расшарить ({n_sel} гр.)", "shr_grp_confirm")])
    rows.append([("❌ Отмена", "action_cancel")])
    text = (
        f"📁 <b>Выбор групп для шаринга</b>\n━━━━━━━━━━━━━━━━━━━\n"
        f"Групп: <b>{n_grp}</b>  ·  Выбрано: <b>{n_sel}</b>\n\n"
        f"Отметьте группы — все их аккаунты войдут в одну ссылку:"
    )
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await bot.send_message(cb.message.chat.id, text, reply_markup=kb(*rows))


# ─── Picker-режимы ───────────────────────────────────────────────

@router.callback_query(F.data.startswith("shr_t:"))
async def cb_shr_t(cb: CallbackQuery):
    uid = cb.from_user.id
    mode = cb.data.split(":")[1]
    if mode == "man":
        _shr_selection.pop(uid, None)
        await cb.answer()
        markup = kb(
            [("✏️ Ввести номера",     "shr_man_type")],
            [("📋 Выбрать из списка", "shr_sel:0")],
            [("❌ Отмена",            "action_cancel")],
        )
        try:
            await cb.message.edit_text(
                "✏️ <b>Ручной выбор аккаунтов</b>\n\nКак хотите выбрать?",
                reply_markup=markup)
        except TelegramBadRequest:
            await cb.message.answer(
                "✏️ <b>Ручной выбор аккаунтов</b>\n\nКак хотите выбрать?",
                reply_markup=markup)
    elif mode == "all":
        await cb.answer()
        targets = await _resolve_targets_all(uid)
        if not targets:
            return await bot.send_message(
                cb.message.chat.id, "❌ Аккаунтов для шаринга нет.")
        await _shr_show_preview(cb, uid, [a["phone"] for a in targets])
    elif mode == "grp":
        await cb.answer()
        _shr_grp_selection.pop(uid, None)
        await _render_shr_grp_selector(cb, uid)
    else:
        await cb.answer("Bad", show_alert=True)


@router.callback_query(F.data.startswith("shr_grp_tog:"))
async def cb_shr_grp_tog(cb: CallbackQuery):
    uid = cb.from_user.id
    try:
        gi = int(cb.data.split(":", 1)[1])
    except Exception:
        return await cb.answer("Bad", show_alert=True)
    groups = _grp_index_cache.get(uid, [])
    if not (0 <= gi < len(groups)):
        return await cb.answer("Группа не найдена.", show_alert=True)
    grp = groups[gi]
    selected = _shr_grp_selection.setdefault(uid, set())
    selected.discard(grp) if grp in selected else selected.add(grp)
    await cb.answer()
    await _render_shr_grp_selector(cb, uid)


@router.callback_query(F.data == "shr_grp_all")
async def cb_shr_grp_all(cb: CallbackQuery):
    uid = cb.from_user.id
    groups = _grp_index_cache.get(uid, [])
    if not groups:
        return await cb.answer("Нет групп.", show_alert=True)
    selected = _shr_grp_selection.setdefault(uid, set())
    selected.clear() if len(selected) == len(groups) else selected.update(groups)
    await cb.answer()
    await _render_shr_grp_selector(cb, uid)


@router.callback_query(F.data == "shr_grp_confirm")
async def cb_shr_grp_confirm(cb: CallbackQuery):
    uid = cb.from_user.id
    selected_groups = _shr_grp_selection.pop(uid, set())
    if not selected_groups:
        return await cb.answer("Не выбрано ни одной группы.", show_alert=True)
    await cb.answer()
    phones_seen: set = set()
    targets = []
    for grp in sorted(selected_groups):
        for acc in await db.db_get_accounts_by_group(uid, grp):
            if acc["phone"] not in phones_seen:
                phones_seen.add(acc["phone"])
                targets.append(acc)
    if not targets:
        return await bot.send_message(
            cb.message.chat.id, "❌ В выбранных группах нет аккаунтов.")
    targets.sort(key=lambda a: a["phone"])
    await _shr_show_preview(cb, uid, [a["phone"] for a in targets])


@router.callback_query(F.data == "shr_man_type")
async def cb_shr_man_type(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    targets = await _resolve_targets_manual(uid, cb.message.chat.id)
    if not targets:
        return await bot.send_message(
            cb.message.chat.id, "❌ Аккаунтов не найдено.")
    await _shr_show_preview(cb, uid, [a["phone"] for a in targets])


@router.callback_query(F.data.startswith("shr_sel:"))
async def cb_shr_sel(cb: CallbackQuery):
    uid = cb.from_user.id
    try:
        page = int(cb.data.split(":", 1)[1])
    except Exception:
        page = 0
    await cb.answer()
    await _render_shr_selector(cb, uid, page)


@router.callback_query(F.data.startswith("shr_tog:"))
async def cb_shr_tog(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    phone = parts[1]
    page = int(parts[2]) if len(parts) > 2 else 0
    selected = _shr_selection.setdefault(uid, set())
    selected.discard(phone) if phone in selected else selected.add(phone)
    await cb.answer()
    await _render_shr_selector(cb, uid, page)


@router.callback_query(F.data == "shr_sel_confirm")
async def cb_shr_sel_confirm(cb: CallbackQuery):
    uid = cb.from_user.id
    selected = _shr_selection.pop(uid, set())
    if not selected:
        return await cb.answer("Не выбрано ни одного аккаунта.", show_alert=True)
    await cb.answer()
    await _shr_show_preview(cb, uid, sorted(selected))


# =================================================================
# Создание токена и отправка ссылки
# =================================================================
@router.callback_query(F.data == "shr_create")
async def cb_shr_create(cb: CallbackQuery):
    uid = cb.from_user.id
    phones = _share_pending.pop(uid, None)
    if not phones:
        return await cb.answer(
            "Сессия шаринга устарела — начните заново.", show_alert=True)
    token = secrets.token_urlsafe(16)
    await db.db_share_create(token, uid, phones)
    bot_un = _bg._bot_username or "бот"
    link = f"https://t.me/{bot_un}?start=sh_{token}"
    await cb.answer()
    try:
        await cb.message.edit_text(
            f"🔗 <b>Ссылка общего доступа создана</b>\n━━━━━━━━━━━━━━━━━━━\n"
            f"Аккаунтов: <b>{len(phones)}</b>\n\n"
            f"Отправьте получателю эту ссылку:\n<code>{link}</code>\n\n"
            f"Ссылка <b>одноразовая</b> — один получатель.\n"
            f"Владелец аккаунтов не меняется.\n"
            f"Отозвать доступ можно в разделе «👥 Я поделился».",
            reply_markup=kb([home_btn()]),
        )
    except Exception:
        await cb.message.answer(
            f"🔗 <b>Ссылка шаринга:</b>\n<code>{link}</code>",
            reply_markup=kb([home_btn()]),
        )


# =================================================================
# Входящий share-link: /start sh_TOKEN
# =================================================================
async def _handle_share_incoming(msg: Message, uid: int, token: str) -> None:
    rec = await db.db_share_get(token)
    if not rec:
        await msg.answer(
            "❌ Ссылка общего доступа недействительна или уже использована.")
        await restore_main_menu(bot, msg.chat.id, uid)
        return
    from_uid: int = rec["from_uid"]
    phones: List[str] = rec["phones"]

    if from_uid == uid:
        await msg.answer("Нельзя дать общий доступ самому себе.")
        await restore_main_menu(bot, msg.chat.id, uid)
        return

    valid = []
    for ph in phones:
        a = await db.db_get_account(ph)
        if a and a.get("owner_id") == from_uid:
            valid.append(ph)

    if not valid:
        await db.db_share_delete(token)
        await msg.answer("❌ Все аккаунты из этой ссылки уже недоступны.")
        await restore_main_menu(bot, msg.chat.id, uid)
        return

    already = [ph for ph in valid if await db.db_shared_check(ph, uid)]
    preview = "\n".join(f"  <code>{ph}</code>" for ph in valid[:20])
    if len(valid) > 20:
        preview += f"\n  ...и ещё {len(valid) - 20}"
    extra = (f"\n\nИз них уже доступны вам: <b>{len(already)}</b>"
             if already else "")

    await msg.answer(
        f"🤝 <b>Вам предлагают совместный доступ</b>\n━━━━━━━━━━━━━━━━━━━\n"
        f"Аккаунтов: <b>{len(valid)}</b>{extra}\n\n{preview}\n\n"
        f"Вы получите доступ к управлению этими аккаунтами.\n"
        f"Удалять или передавать их нельзя.\n\nПринять?",
        reply_markup=kb(
            [("✅ Принять",   f"shr_accept:{token}")],
            [("❌ Отклонить", f"shr_decline:{token}")],
        ),
    )


@router.callback_query(F.data.startswith("shr_accept:"))
async def cb_shr_accept(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    rec = await db.db_share_get(token)
    if not rec:
        await cb.answer("❌ Ссылка уже использована.", show_alert=True)
        try:
            await cb.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return
    from_uid: int = rec["from_uid"]
    phones: List[str] = rec["phones"]
    await db.db_share_delete(token)
    await cb.answer("Добавляю доступ...")
    ok = []
    skipped = []
    for ph in phones:
        a = await db.db_get_account(ph)
        if not a or a.get("owner_id") != from_uid:
            skipped.append(ph)
            continue
        await db.db_shared_add(ph, uid, from_uid)
        ok.append(ph)
    ok_text   = "\n".join(f"  ✅ <code>{ph}</code>" for ph in ok)
    skip_text = "\n".join(f"  ❌ <code>{ph}</code>" for ph in skipped)
    result_text = (
        f"🤝 <b>Общий доступ предоставлен</b>\n━━━━━━━━━━━━━━━━━━━\n"
        f"Добавлено: <b>{len(ok)}</b>"
    )
    if ok_text:
        result_text += f"\n{ok_text}"
    if skipped:
        result_text += f"\n\nНедоступных: <b>{len(skipped)}</b>\n{skip_text}"
    result_text += (
        "\n\nАккаунты теперь видны в вашем списке с пометкой 🤝.\n"
        "Раздел: <b>Аккаунты</b>"
    )
    try:
        await cb.message.edit_text(result_text, reply_markup=None)
    except Exception:
        await cb.message.answer(result_text)
    await restore_main_menu(bot, cb.message.chat.id, uid)
    try:
        await notify_owner(
            from_uid,
            f"🤝 <b>Общий доступ принят</b>\n"
            f"Пользователь <code>{uid}</code> получил доступ "
            f"к <b>{len(ok)}</b> аккаунту(ам).",
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("shr_decline:"))
async def cb_shr_decline(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    rec = await db.db_share_get(token)
    await db.db_share_delete(token)
    await cb.answer("Доступ отклонён.")
    try:
        await cb.message.edit_text("❌ Общий доступ отклонён.")
    except Exception:
        pass
    await restore_main_menu(bot, cb.message.chat.id, cb.from_user.id)
    if rec:
        try:
            n = len(rec.get("phones") or [])
            await notify_owner(
                rec["from_uid"],
                f"❌ Пользователь <code>{cb.from_user.id}</code> отклонил "
                f"запрос общего доступа к <b>{n}</b> аккаунту(ам).",
            )
        except Exception:
            pass


# =================================================================
# Мне поделились
# =================================================================
@router.callback_query(F.data == "shr_mine")
async def cb_shr_mine(cb: CallbackQuery):
    uid = cb.from_user.id
    shares = await db.db_shared_get_by_user(uid)
    await cb.answer()
    back_mk = kb([("‹ Назад", "acc_share"), home_btn()])
    if not shares:
        text = (
            "📋 <b>Мне поделились</b>\n━━━━━━━━━━━━━━━━━━━\n"
            "Никто не давал вам совместный доступ."
        )
        try:
            await cb.message.edit_text(text, reply_markup=back_mk)
        except TelegramBadRequest:
            await cb.message.answer(text, reply_markup=back_mk)
        return
    lines = [
        f"📋 <b>Мне поделились</b>\n━━━━━━━━━━━━━━━━━━━\n"
        f"Всего: <b>{len(shares)}</b>\n"
    ]
    for s in shares[:30]:
        un = f" (@{s['username']})" if s.get("username") else ""
        grp = f" 📁{s['grp']}" if s.get("grp") else ""
        lines.append(
            f"  🤝 <code>{s['phone']}</code>{un}{grp}"
            f"  (от uid <code>{s['shared_by_uid']}</code>)"
        )
    if len(shares) > 30:
        lines.append(f"...ещё {len(shares) - 30}")
    rows = []
    for s in shares[:8]:
        rows.append([(f"🗑 Убрать: {s['phone']}", f"shr_leave:{s['phone']}")])
    if len(shares) > 8:
        rows.append([("...ещё аккаунты есть (см. список)", "noop")])
    rows.append([("‹ Назад", "acc_share"), home_btn()])
    try:
        await cb.message.edit_text("\n".join(lines), reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer("\n".join(lines), reply_markup=kb(*rows))


@router.callback_query(F.data.startswith("shr_leave:"))
async def cb_shr_leave(cb: CallbackQuery):
    """Пользователь сам убирает свой shared-доступ к аккаунту."""
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    await db.db_shared_remove(phone, uid)
    await cb.answer(f"Доступ к {phone} убран.")
    await cb_shr_mine(cb)


# =================================================================
# Я поделился
# =================================================================
@router.callback_query(F.data == "shr_by_me")
async def cb_shr_by_me(cb: CallbackQuery):
    uid = cb.from_user.id
    shares = await db.db_shared_get_by_sharer(uid)
    await cb.answer()
    back_mk = kb([("‹ Назад", "acc_share"), home_btn()])
    if not shares:
        text = (
            "👥 <b>Я поделился</b>\n━━━━━━━━━━━━━━━━━━━\n"
            "Вы ещё ни с кем не делились аккаунтами."
        )
        try:
            await cb.message.edit_text(text, reply_markup=back_mk)
        except TelegramBadRequest:
            await cb.message.answer(text, reply_markup=back_mk)
        return
    lines = [
        f"👥 <b>Я поделился</b>\n━━━━━━━━━━━━━━━━━━━\n"
        f"Записей: <b>{len(shares)}</b>\n"
    ]
    rows = []
    shown = 0
    for s in shares:
        if shown >= _SHR_REVOKE_PAGE_SIZE:
            break
        un = f" (@{s['username']})" if s.get("username") else ""
        lines.append(
            f"  🤝 <code>{s['phone']}</code>{un}"
            f"  → uid <code>{s['shared_with_uid']}</code>"
        )
        rows.append([(
            f"🗑 {s['phone']} (uid {s['shared_with_uid']})",
            f"shr_revoke:{s['phone']}:{s['shared_with_uid']}",
        )])
        shown += 1
    if len(shares) > _SHR_REVOKE_PAGE_SIZE:
        lines.append(f"...ещё {len(shares) - _SHR_REVOKE_PAGE_SIZE}")
    rows.append([("‹ Назад", "acc_share"), home_btn()])
    try:
        await cb.message.edit_text("\n".join(lines), reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer("\n".join(lines), reply_markup=kb(*rows))


@router.callback_query(F.data.startswith("shr_revoke:"))
async def cb_shr_revoke(cb: CallbackQuery):
    """Владелец отзывает доступ конкретного пользователя к аккаунту."""
    uid = cb.from_user.id
    parts = cb.data.split(":")
    if len(parts) < 3:
        return await cb.answer("Bad data", show_alert=True)
    phone = parts[1]
    try:
        target_uid = int(parts[2])
    except Exception:
        return await cb.answer("Bad uid", show_alert=True)
    acc = await db.db_get_account(phone)
    if not acc or acc.get("owner_id") != uid:
        return await cb.answer(
            "Аккаунт не найден или вы не владелец.", show_alert=True)
    await db.db_shared_remove(phone, target_uid)
    await cb.answer(f"Доступ uid {target_uid} к {phone} отозван.")
    try:
        await notify_owner(
            target_uid,
            f"Доступ к аккаунту <code>{phone}</code> отозван владельцем.",
        )
    except Exception:
        pass
    await cb_shr_by_me(cb)
