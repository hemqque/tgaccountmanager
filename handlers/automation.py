# -*- coding: utf-8 -*-
"""
handlers/automation.py — Автоматизация: массовый залив, рега LDV/XO,
подписка, смена username, автоответы, универсальный ручной выборщик.
"""

import asyncio
import logging
import os
import random
import time
from typing import Any, Dict, List, Optional

from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.exceptions import TelegramBadRequest
from telethon.tl.functions.account import UpdateProfileRequest, UpdateUsernameRequest
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.photos import (
    UploadProfilePhotoRequest, DeletePhotosRequest, GetUserPhotosRequest,
)
from telethon.tl.types import InputPhoto

import db
import config
from bot_globals import (
    bot, store, task_queue, ar_manager,
    _grp_index_cache, _man_sel_ctx, _man_selection, _MAN_SEL_PER_PAGE,
    notify_owner, get_or_create_account_client, kb, home_btn,
    _gen_username,
)
from global_proxy import get_proxy_for_account
from progress import _start_progress, _update_progress, _finish_progress
from utils import restore_main_menu, ask_with_cancel, ask_with_retry
from handlers.helpers import (
    _send_target_picker, _send_groups_picker,
    _resolve_targets_all, _resolve_targets_group, _resolve_targets_manual,
)
from reg_resume import register_ldv_resumable, register_xo_resumable
from ldv_functions import register_one_ldv
from xo_functions import register_one_xo
from profile_music import set_birthday, set_profile_music, cleanup_user_music

log = logging.getLogger("automation")
router = Router(name="automation")

# =================================================================
# Универсальный ручной выборщик аккаунтов
# =================================================================
async def _show_man_submenu(cb):
    text = "✏️ <b>Ручной выбор аккаунтов</b>\n\nКак хотите выбрать?"
    markup = kb(
        [("✏️ Ввести номера", "man_type")],
        [("📋 Выбрать из списка", "man_sel:0")],
        [("❌ Отмена", "action_cancel")],
    )
    try:
        await cb.message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=markup)


@router.callback_query(F.data == "man_type")
async def cb_man_type(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    targets = await _resolve_targets_manual(uid, cb.message.chat.id)
    if not targets:
        return await bot.send_message(uid, "❌ Аккаунтов не найдено.")
    await _man_after_confirm(cb, uid, [a["phone"] for a in targets])


async def _render_man_selector(cb, uid: int, page: int) -> None:
    accs = await db.db_get_accounts_by_owner(uid)
    if not accs:
        await cb.message.answer("❌ У вас нет аккаунтов.")
        return
    selected = _man_selection.setdefault(uid, set())
    total = len(accs)
    pages = max(1, (total + _MAN_SEL_PER_PAGE - 1) // _MAN_SEL_PER_PAGE)
    page = max(0, min(page, pages - 1))
    chunk = accs[page * _MAN_SEL_PER_PAGE:(page + 1) * _MAN_SEL_PER_PAGE]
    rows = []
    for a in chunk:
        ph = a["phone"]
        un = f" (@{a['username']})" if a.get("username") else ""
        grp = f" 📁{a['grp']}" if a.get("grp") else ""
        icon = "✅" if ph in selected else "⬜"
        rows.append([(f"{icon} {ph}{un}{grp}", f"man_tog:{ph}:{page}")])
    nav = []
    if page > 0:
        nav.append(("◀️", f"man_sel:{page - 1}"))
    nav.append((f"{page + 1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append(("▶️", f"man_sel:{page + 1}"))
    if nav:
        rows.append(nav)
    n_sel = len(selected)
    if n_sel > 0:
        rows.append([(f"✅ Подтвердить ({n_sel} выбрано)", "man_confirm")])
    rows.append([("❌ Отмена", "action_cancel")])
    text = (
        f"📋 <b>Выбор аккаунтов</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Всего: <b>{total}</b>  ·  Выбрано: <b>{n_sel}</b>  ·  "
        f"Стр. {page + 1}/{pages}\n\nНажмите на аккаунт чтобы отметить/снять:"
    )
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))


@router.callback_query(F.data.startswith("man_sel:"))
async def cb_man_sel(cb: CallbackQuery):
    uid = cb.from_user.id
    try:
        page = int(cb.data.split(":", 1)[1])
    except Exception:
        page = 0
    await cb.answer()
    await _render_man_selector(cb, uid, page)


@router.callback_query(F.data.startswith("man_tog:"))
async def cb_man_tog(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    phone = parts[1]
    try:
        page = int(parts[2])
    except Exception:
        page = 0
    selected = _man_selection.setdefault(uid, set())
    if phone in selected:
        selected.discard(phone)
    else:
        selected.add(phone)
    await cb.answer()
    await _render_man_selector(cb, uid, page)


@router.callback_query(F.data == "man_confirm")
async def cb_man_confirm(cb: CallbackQuery):
    uid = cb.from_user.id
    selected = _man_selection.pop(uid, set())
    if not selected:
        return await cb.answer("Не выбрано ни одного аккаунта.", show_alert=True)
    await cb.answer()
    await _man_after_confirm(cb, uid, sorted(selected))


async def _man_after_confirm(cb, uid: int, phones) -> None:
    ctx = _man_sel_ctx.pop(uid, "")
    targets = []
    for ph in phones:
        a = await db.db_get_account(ph)
        if a and a.get("owner_id") == uid:
            targets.append(a)
    if not targets:
        return await bot.send_message(uid, "❌ Аккаунтов не найдено.")
    if ctx == "mass_t":
        await _mass_after_targets(cb, uid, targets)
    elif ctx == "ldvr_t":
        await _ldvr_after_targets(cb, uid, targets)
    elif ctx == "xor_t":
        await _xor_after_targets(cb, uid, targets)
    elif ctx == "subdv_t":
        await _subdv_after_targets(cb, uid, targets)
    elif ctx == "rtag_t":
        await _rtag_after_targets(cb, uid, targets)
    elif ctx == "fa2_t":
        from handlers.accounts import _fa2_after_targets
        await _fa2_after_targets(cb, uid, targets)
    else:
        await bot.send_message(uid, "❌ Контекст выбора потерян.")


# =================================================================
# 🚀 Массовый залив
# =================================================================
@router.callback_query(F.data == "auto_mass")
async def cb_auto_mass(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Завершите текущее действие.", show_alert=True)
    store.mass_data[uid] = {}
    await cb.answer()
    await _send_target_picker(
        cb.message.chat.id, "mass_t",
        "🚀 <b>Массовый залив</b>\n\n"
        "Выберите аккаунты — затем отметите что именно менять:\n"
        "✏️ Имена  ·  📝 Био  ·  📷 Фото"
    )


@router.callback_query(F.data.startswith("mass_t:"))
async def cb_mass_t(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    mode = parts[1]
    if mode == "man":
        _man_sel_ctx[uid] = "mass_t"
        _man_selection.pop(uid, None)
        await cb.answer()
        await _show_man_submenu(cb)
        return
    targets = []
    if mode == "all":
        targets = await _resolve_targets_all(uid)
        await cb.answer()
    elif mode == "grp" and len(parts) == 2:
        await cb.answer()
        return await _send_groups_picker(uid, cb.message.chat.id, "mass_t")
    elif mode == "gi" and len(parts) == 3:
        await cb.answer()
        targets = await _resolve_targets_group(uid, int(parts[2]))
    else:
        return await cb.answer("Bad", show_alert=True)
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    await _mass_after_targets(cb, uid, targets)


async def _mass_after_targets(cb, uid: int, targets) -> None:
    store.mass_data[uid] = {"targets": [a["phone"] for a in targets], "what_sel": set()}
    await _mass_render_what(cb, uid, send_new=True, header=f"✅ Целей: <b>{len(targets)}</b>.\n\n")


async def _mass_render_what(cb, uid: int, send_new: bool = False, header: str = "") -> None:
    what = store.mass_data.get(uid, {}).get("what_sel", set())
    rows = [
        [(f"{'✅' if 'name'  in what else '⬜'} ✏️ Имена",  "mass_what_tog:name")],
        [(f"{'✅' if 'bio'   in what else '⬜'} 📝 Био",    "mass_what_tog:bio")],
        [(f"{'✅' if 'photo' in what else '⬜'} 📷 Фото",   "mass_what_tog:photo")],
        [(f"{'✅' if 'music' in what else '⬜'} 🎵 Музыка + 🎂 д/р", "mass_what_tog:music")],
    ]
    if what:
        rows.append([("✅ Продолжить", "mass_what_confirm")])
    rows.append([("❌ Отмена", "action_cancel")])
    text = header + "🚀 <b>Что изменить?</b>\n━━━━━━━━━━━━━━━━━━━\nВыберите один или несколько пунктов:"
    if send_new:
        await bot.send_message(cb.message.chat.id, text, reply_markup=kb(*rows))
    else:
        try:
            await cb.message.edit_text(text, reply_markup=kb(*rows))
        except TelegramBadRequest:
            pass


@router.callback_query(F.data.startswith("mass_what_tog:"))
async def cb_mass_what_tog(cb: CallbackQuery):
    uid = cb.from_user.id
    item = cb.data.split(":", 1)[1]
    what = store.mass_data.setdefault(uid, {}).setdefault("what_sel", set())
    if item in what:
        what.discard(item)
    else:
        what.add(item)
    await cb.answer()
    await _mass_render_what(cb, uid)


@router.callback_query(F.data == "mass_what_confirm")
async def cb_mass_what_confirm(cb: CallbackQuery):
    uid = cb.from_user.id
    what = store.mass_data.get(uid, {}).get("what_sel", set())
    if not what:
        return await cb.answer("Выберите хотя бы один пункт.", show_alert=True)
    await cb.answer()
    chat_id = cb.message.chat.id
    md = store.mass_data.get(uid, {})
    if "name" in what:
        txt = await ask_with_cancel(bot, chat_id, uid,
            "✏️ Пришлите ИМЕНА (каждое с новой строки).\nРандомно раздаются по аккаунтам:")
        if not txt:
            return await restore_main_menu(bot, chat_id, uid, "Отменено.")
        md["names"] = [s.strip() for s in txt.splitlines() if s.strip()]
    if "bio" in what:
        txt = await ask_with_cancel(bot, chat_id, uid,
            "📝 Пришлите БИО (каждое с новой строки).\nРандомно раздаются по аккаунтам:")
        if not txt:
            return await restore_main_menu(bot, chat_id, uid, "Отменено.")
        md["bios"] = [s.strip() for s in txt.splitlines() if s.strip()]
    if "photo" in what:
        store.photo_collecting[uid] = True
        store.clear_temp_photos(uid)
        await bot.send_message(chat_id,
            "📸 Пришлите ФОТО (несколько). Затем нажмите «📸 Готово».",
            reply_markup=kb([("📸 Готово", "mass_photodone")], [("❌ Отмена", "action_cancel")]))
    elif "music" in what:
        await _mass_start_music_collect(chat_id, uid)
    else:
        await _mass_run(chat_id, uid, md)


@router.callback_query(F.data == "mass_photodone")
async def cb_mass_photodone(cb: CallbackQuery):
    uid = cb.from_user.id
    photos = store.get_temp_photos(uid)
    store.photo_collecting[uid] = False
    md = store.mass_data.get(uid) or {}
    md["photos"] = photos
    await cb.answer()
    if not md.get("targets"):
        store.mass_data.pop(uid, None)
        await restore_main_menu(bot, cb.message.chat.id, uid, "❌ Цели потеряны.")
        return
    if "music" in md.get("what_sel", set()):
        await _mass_start_music_collect(cb.message.chat.id, uid)
    else:
        await _mass_run(cb.message.chat.id, uid, md)


async def _mass_start_music_collect(chat_id: int, uid: int) -> None:
    store.music_collecting[uid] = True
    store.clear_temp_music(uid)
    await bot.send_message(
        chat_id,
        "🎵 Пришлите MP3 файлы (аудио). Затем нажмите «🎵 Готово».\n"
        "Если музыка не нужна — нажмите «⏭ Пропустить».\n\n"
        "Будет установлено: 🎂 день рождения 11.02 + 🎵 музыка (только Premium).",
        reply_markup=kb(
            [("🎵 Готово", "mass_musicdone")],
            [("⏭ Пропустить музыку", "mass_musicskip")],
            [("❌ Отмена", "action_cancel")],
        ),
    )


@router.callback_query(F.data == "mass_musicdone")
async def cb_mass_musicdone(cb: CallbackQuery):
    uid = cb.from_user.id
    music_files = store.get_temp_music(uid)
    store.music_collecting[uid] = False
    md = store.mass_data.get(uid) or {}
    md["music_files"] = music_files
    await cb.answer()
    if not md.get("targets"):
        store.mass_data.pop(uid, None)
        store.clear_temp_music(uid)
        await restore_main_menu(bot, cb.message.chat.id, uid, "❌ Цели потеряны.")
        return
    await _mass_run(cb.message.chat.id, uid, md)


@router.callback_query(F.data == "mass_musicskip")
async def cb_mass_musicskip(cb: CallbackQuery):
    uid = cb.from_user.id
    store.music_collecting[uid] = False
    store.clear_temp_music(uid)
    md = store.mass_data.get(uid) or {}
    md["music_files"] = []
    await cb.answer()
    if not md.get("targets"):
        store.mass_data.pop(uid, None)
        await restore_main_menu(bot, cb.message.chat.id, uid, "❌ Цели потеряны.")
        return
    await _mass_run(cb.message.chat.id, uid, md)


async def _mass_run(chat_id: int, uid: int, md: dict) -> None:
    targets = md.get("targets") or []
    what = md.get("what_sel", set())
    if not targets:
        await bot.send_message(uid, "❌ Цели потеряны.")
        return

    async def _runner():
        await _start_progress(bot, chat_id, uid, total=len(targets), store=store, title="🚀 Массовый залив")
        ok = 0
        for ph in targets:
            await _update_progress(bot, uid, store, current=ph)
            try:
                cli = await get_or_create_account_client(ph, uid)
                if not cli:
                    await _update_progress(bot, uid, store, done_inc=1, current=None, error=f"{ph}: не подключился")
                    continue
                kwargs = {}
                if "name" in what and md.get("names"):
                    kwargs["first_name"] = random.choice(md["names"])[:64]
                if "bio" in what and md.get("bios"):
                    kwargs["about"] = random.choice(md["bios"])[:70]
                if kwargs:
                    await cli(UpdateProfileRequest(**kwargs))
                if "photo" in what and md.get("photos"):
                    photo_path = random.choice(md["photos"])
                    try:
                        existing = await cli(GetUserPhotosRequest(user_id="me", offset=0, max_id=0, limit=10))
                        ips = [InputPhoto(id=p.id, access_hash=p.access_hash, file_reference=p.file_reference)
                               for p in existing.photos]
                        if ips:
                            await cli(DeletePhotosRequest(id=ips))
                    except Exception:
                        pass
                    await cli(UploadProfilePhotoRequest(file=await cli.upload_file(photo_path)))
                if "music" in what:
                    await set_birthday(cli)
                    music_files = md.get("music_files") or []
                    if music_files:
                        mp3_path = random.choice(music_files)
                        await set_profile_music(cli, mp3_path)
                ok += 1
                await _update_progress(bot, uid, store, done_inc=1, current=None)
            except Exception as e:
                await _update_progress(bot, uid, store, done_inc=1, current=None, error=f"{ph}: {e}")
            await asyncio.sleep(random.uniform(7, 20))
        changed = []
        if "name" in what:  changed.append("имена")
        if "bio" in what:   changed.append("био")
        if "photo" in what: changed.append("фото")
        if "music" in what: changed.append("🎵 музыка + 🎂 д/р")
        await _finish_progress(bot, uid, store,
            summary_extra=f"Обновлено: {ok}/{len(targets)}\nИзменено: {', '.join(changed)}")
        store.clear_temp_photos(uid)
        store.clear_temp_music(uid)
        if "music" in what:
            cleanup_user_music(uid)
        store.mass_data.pop(uid, None)
        await restore_main_menu(bot, chat_id, uid)

    await task_queue.submit(_runner, owner_id=uid, notify=notify_owner, title=f"Массовый залив {len(targets)}")


# =================================================================
# Helper: user_log
# =================================================================
async def _user_log(uid: int, text: str) -> None:
    try:
        s = await db.db_user_settings_get(uid)
        if s.get("logs_enabled"):
            await bot.send_message(uid, f"📋 {text}")
    except Exception:
        pass


# =================================================================
# 🤖 Рега ЛДВ
# =================================================================
@router.callback_query(F.data == "auto_ldv")
async def cb_auto_ldv(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Занято.", show_alert=True)
    store.ldv_data[uid] = {}
    await cb.answer()
    await _send_target_picker(cb.message.chat.id, "ldvr_t",
        "🤖 <b>Регистрация LDV</b>\n\nЗарегистрирует аккаунты в @leomatchbot и запланирует автолайкинг.")


@router.callback_query(F.data.startswith("ldvr_t:"))
async def cb_ldvr_t(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    mode = parts[1]
    if mode == "man":
        _man_sel_ctx[uid] = "ldvr_t"
        _man_selection.pop(uid, None)
        await cb.answer()
        await _show_man_submenu(cb)
        return
    targets = []
    if mode == "all":
        targets = await _resolve_targets_all(uid)
        await cb.answer()
    elif mode == "grp" and len(parts) == 2:
        await cb.answer()
        return await _send_groups_picker(uid, cb.message.chat.id, "ldvr_t")
    elif mode == "gi":
        await cb.answer()
        targets = await _resolve_targets_group(uid, int(parts[2]))
    else:
        return await cb.answer("Bad", show_alert=True)
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    await _ldvr_after_targets(cb, uid, targets)


async def _ldvr_after_targets(cb, uid: int, targets) -> None:
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    store.ldv_data.setdefault(uid, {})["targets"] = [a["phone"] for a in targets]
    raw = await ask_with_retry(bot, cb.message.chat.id, uid,
        "📋 Пришлите данные (6 строк через Enter):\n"
        "1. Возраст (можно несколько через запятую)\n"
        "2. Пол: «Я девушка» или «Я парень»\n"
        "3. Кого показывать: «Парни» или «Девушки»\n"
        "4. Город (можно несколько через запятую)\n"
        "5. Имя (можно несколько через запятую)\n"
        "6. Задержка в минутах перед стартом",
        validator=lambda t: len([s.strip() for s in t.splitlines() if s.strip()]) >= 6,
        error_msg="❌ Нужно 6 непустых строк.")
    if not raw:
        return await restore_main_menu(bot, cb.message.chat.id, uid)
    lines = [s.strip() for s in raw.splitlines() if s.strip()]
    ages   = [s.strip() for s in lines[0].split(",") if s.strip()]
    sex    = lines[1]
    target = lines[2]
    cities = [s.strip() for s in lines[3].split(",") if s.strip()]
    names  = [s.strip() for s in lines[4].split(",") if s.strip()]
    try:
        delay_min = float(lines[5])
    except Exception:
        delay_min = 0.0
    store.ldv_data[uid].update({"ages": ages, "sex": sex, "target": target,
                                 "cities": cities, "names": names, "delay_min": delay_min})
    store.photo_collecting[uid] = True
    store.clear_temp_photos(uid)
    await bot.send_message(cb.message.chat.id,
        "📸 Пришлите ФОТО (несколько). Затем нажмите «📸 Готово».",
        reply_markup=kb([("📸 Готово", "ldvr_photodone")],
                        [("🛑 Отменить регистрацию ЛДВ", "ldvr_cancel_all")],
                        [("❌ Отмена", "action_cancel")]))


@router.callback_query(F.data == "ldvr_cancel_all")
async def cb_ldvr_cancel_all(cb: CallbackQuery):
    uid = cb.from_user.id
    d = store.ldv_data.get(uid) or {}
    n = 0
    for ph in d.get("targets") or []:
        store.ldv_reg_cancel.add(ph)
        n += 1
    await cb.answer(f"Отменяю партию ЛДВ ({n}).", show_alert=True)


@router.callback_query(F.data == "ldvr_photodone")
async def cb_ldvr_photodone(cb: CallbackQuery):
    uid = cb.from_user.id
    photos = store.get_temp_photos(uid)
    store.photo_collecting[uid] = False
    d = store.ldv_data.get(uid) or {}
    d["photos"] = photos
    targets = d.get("targets") or []
    await cb.answer()
    if not targets or not photos:
        store.ldv_data.pop(uid, None)
        await restore_main_menu(bot, cb.message.chat.id, uid, "❌ Целей или фото нет.")
        return

    async def _runner():
        if d.get("delay_min", 0) > 0:
            await bot.send_message(uid, f"⏱ Старт через {d['delay_min']:.1f} мин.")
            await asyncio.sleep(d["delay_min"] * 60)
        await _start_progress(bot, cb.message.chat.id, uid, total=len(targets), store=store, title="🤖 Рега ЛДВ")
        success = []
        ages_list   = list(d.get("ages")   or ["20"])
        cities_list = list(d.get("cities") or ["Москва"])
        names_list  = list(d.get("names")  or ["Аня"])
        random.shuffle(ages_list)
        random.shuffle(cities_list)
        random.shuffle(names_list)
        for i, ph in enumerate(targets):
            if ph in store.ldv_reg_cancel:
                store.ldv_reg_cancel.discard(ph)
                await _update_progress(bot, uid, store, done_inc=1, error=f"{ph}: отменено")
                continue
            await _update_progress(bot, uid, store, current=f"{ph} — подключение…")
            try:
                cli = await get_or_create_account_client(ph, uid)
                if not cli:
                    raise RuntimeError("connect failed")
                per_acc = dict(d)
                per_acc.pop("ages", None); per_acc.pop("cities", None); per_acc.pop("names", None)
                per_acc["age"]  = str(ages_list[i % len(ages_list)])
                per_acc["city"] = cities_list[i % len(cities_list)]
                per_acc["name"] = names_list[i % len(names_list)]
                await _update_progress(bot, uid, store, current=f"{ph} — регистрация…")
                state = await db.db_get_reg_state(ph, config.LDV_BOT)
                if state:
                    ok = await register_ldv_resumable(cli, ph, per_acc, uid,
                        notify_func=lambda t, _u=uid: _user_log(_u, t),
                        photos_request_func=None, cancel_set=store.ldv_reg_cancel)
                else:
                    ok = await register_one_ldv(cli, ph, per_acc,
                        notify_func=lambda t, _u=uid: _user_log(_u, t),
                        owner_id=uid, cancel_set=store.ldv_reg_cancel)
                if ok:
                    success.append(ph)
                    # Отправляем город в Saved Messages аккаунта
                    try:
                        await cli.send_message("me", per_acc["city"])
                    except Exception as _e:
                        log.debug("saved_messages city %s: %s", ph, _e)
                await _update_progress(bot, uid, store, done_inc=1, current=None,
                                       error=None if ok else f"{ph}: не зарегистрирован")
            except Exception as e:
                await _update_progress(bot, uid, store, done_inc=1, error=f"{ph}: {e}")
        nxt = time.time() + config.LDV_INITIAL_DELAY_HOURS * 3600
        for ph in success:
            await db.db_schedule_ldv_task(ph, uid, nxt, step=0, status="pending")
        await _finish_progress(bot, uid, store,
            summary_extra=(f"Зарегистрировано: {len(success)}/{len(targets)}\n"
                           f"📅 Лайкинг запланирован через {config.LDV_INITIAL_DELAY_HOURS}ч "
                           f"для {len(success)} аккаунтов."))
        store.clear_temp_photos(uid)
        store.ldv_data.pop(uid, None)
        await restore_main_menu(bot, cb.message.chat.id, uid)

    await task_queue.submit(_runner, owner_id=uid, notify=notify_owner, title=f"Рега ЛДВ {len(targets)}")


# =================================================================
# 💘 Рега XO
# =================================================================
@router.callback_query(F.data == "auto_xo")
async def cb_auto_xo(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Занято.", show_alert=True)
    store.xo_data[uid] = {}
    await cb.answer()
    await _send_target_picker(cb.message.chat.id, "xor_t",
        "💘 <b>Регистрация XO</b>\n\nЗарегистрирует аккаунты в XO-боте и запустит автолайкинг.")


@router.callback_query(F.data.startswith("xor_t:"))
async def cb_xor_t(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    mode = parts[1]
    if mode == "man":
        _man_sel_ctx[uid] = "xor_t"
        _man_selection.pop(uid, None)
        await cb.answer()
        await _show_man_submenu(cb)
        return
    targets = []
    if mode == "all":
        targets = await _resolve_targets_all(uid)
        await cb.answer()
    elif mode == "grp" and len(parts) == 2:
        await cb.answer()
        return await _send_groups_picker(uid, cb.message.chat.id, "xor_t")
    elif mode == "gi":
        await cb.answer()
        targets = await _resolve_targets_group(uid, int(parts[2]))
    else:
        return await cb.answer("Bad", show_alert=True)
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    await _xor_after_targets(cb, uid, targets)


async def _xor_after_targets(cb, uid: int, targets) -> None:
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    store.xo_data.setdefault(uid, {})["targets"] = [a["phone"] for a in targets]
    raw = await ask_with_retry(bot, cb.message.chat.id, uid,
        "📋 Пришлите данные (4 строки через Enter):\n"
        "1. Пол: «💁‍♀️ я девушка» или «🙋‍♂️ я парень»\n"
        "2. Дата рождения (дд.мм.гггг)\n"
        "3. Город\n"
        "4. Имя",
        validator=lambda t: len([s.strip() for s in t.splitlines() if s.strip()]) >= 4,
        error_msg="❌ Нужно 4 непустых строк.")
    if not raw:
        return await restore_main_menu(bot, cb.message.chat.id, uid)
    lines = [s.strip() for s in raw.splitlines() if s.strip()]
    store.xo_data[uid].update({
        "sex":      lines[0],
        "birthday": lines[1],
        "cities":   [c.strip() for c in lines[2].split(",") if c.strip()],
        "names":    [n.strip() for n in lines[3].split(",") if n.strip()],
    })
    store.photo_collecting[uid] = True
    store.clear_temp_photos(uid)
    await bot.send_message(cb.message.chat.id,
        "📸 Пришлите ФОТО для XO. Затем «📸 Готово».",
        reply_markup=kb([("📸 Готово", "xor_photodone")],
                        [("🛑 Отменить регистрацию XO", "xor_cancel_all")],
                        [("❌ Отмена", "action_cancel")]))


@router.callback_query(F.data == "xor_cancel_all")
async def cb_xor_cancel_all(cb: CallbackQuery):
    uid = cb.from_user.id
    d = store.xo_data.get(uid) or {}
    for ph in d.get("targets") or []:
        store.xo_reg_cancel.add(ph)
    await cb.answer("Отменяю партию XO.", show_alert=True)


@router.callback_query(F.data == "xor_photodone")
async def cb_xor_photodone(cb: CallbackQuery):
    uid = cb.from_user.id
    photos = store.get_temp_photos(uid)
    store.photo_collecting[uid] = False
    d = store.xo_data.get(uid) or {}
    d["photos"] = photos
    targets = d.get("targets") or []
    await cb.answer()
    if not targets or not photos:
        store.xo_data.pop(uid, None)
        await restore_main_menu(bot, cb.message.chat.id, uid, "❌ Целей или фото нет.")
        return

    async def _runner():
        await _start_progress(bot, cb.message.chat.id, uid, total=len(targets), store=store, title="💘 Рега XO")
        success = []
        cities_list = list(d.get("cities") or [d.get("city") or "Москва"])
        names_list  = list(d.get("names")  or [d.get("name")  or "Аня"])
        random.shuffle(cities_list); random.shuffle(names_list)
        for i, ph in enumerate(targets):
            if ph in store.xo_reg_cancel:
                store.xo_reg_cancel.discard(ph)
                await _update_progress(bot, uid, store, done_inc=1, error=f"{ph}: отменено")
                continue
            await _update_progress(bot, uid, store, current=f"{ph} — подключение…")
            try:
                cli = await get_or_create_account_client(ph, uid)
                if not cli:
                    raise RuntimeError("connect failed")
                per_acc = dict(d)
                per_acc.pop("cities", None); per_acc.pop("names", None)
                per_acc["city"] = cities_list[i % len(cities_list)]
                per_acc["name"] = names_list[i % len(names_list)]
                await _update_progress(bot, uid, store, current=f"{ph} — регистрация…")
                state = await db.db_get_reg_state(ph, config.XO_BOT)
                if state:
                    ok = await register_xo_resumable(cli, ph, per_acc, uid,
                        notify_func=lambda t, _u=uid: _user_log(_u, t),
                        cancel_set=store.xo_reg_cancel)
                else:
                    ok = await register_one_xo(cli, ph, per_acc,
                        notify_func=lambda t, _u=uid: _user_log(_u, t),
                        cancel_set=store.xo_reg_cancel, owner_id=uid)
                if ok:
                    success.append(ph)
                await _update_progress(bot, uid, store, done_inc=1, current=None,
                                       error=None if ok else f"{ph}: не зарегистрирован")
            except Exception as e:
                await _update_progress(bot, uid, store, done_inc=1, error=f"{ph}: {e}")
        for ph in success:
            await db.db_schedule_xo_task(ph, uid, time.time() + 5, status="pending")
        await _finish_progress(bot, uid, store,
            summary_extra=f"Зарегистрировано: {len(success)}/{len(targets)}\n💘 XO-лайкинг запланирован.")
        store.clear_temp_photos(uid)
        store.xo_data.pop(uid, None)
        for ph in targets:
            store.xo_reg_cancel.discard(ph)
        await restore_main_menu(bot, cb.message.chat.id, uid)

    await task_queue.submit(_runner, owner_id=uid, notify=notify_owner, title=f"Рега XO {len(targets)}")


# =================================================================
# 📺 Подписка на @leoday
# =================================================================
@router.callback_query(F.data == "auto_subdv")
async def cb_auto_subdv(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Занято.", show_alert=True)
    await cb.answer()
    await _send_target_picker(cb.message.chat.id, "subdv_t",
        "📺 <b>Подписка на @leoday</b>\n\nПодпишет выбранные аккаунты на канал @leoday.")


@router.callback_query(F.data.startswith("subdv_t:"))
async def cb_subdv_t(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    mode = parts[1]
    if mode == "man":
        _man_sel_ctx[uid] = "subdv_t"
        _man_selection.pop(uid, None)
        await cb.answer()
        await _show_man_submenu(cb)
        return
    targets = []
    if mode == "all":
        targets = await _resolve_targets_all(uid)
        await cb.answer()
    elif mode == "grp" and len(parts) == 2:
        await cb.answer()
        return await _send_groups_picker(uid, cb.message.chat.id, "subdv_t")
    elif mode == "gi":
        await cb.answer()
        targets = await _resolve_targets_group(uid, int(parts[2]))
    else:
        return await cb.answer("Bad", show_alert=True)
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    await _subdv_after_targets(cb, uid, targets)


async def _subdv_after_targets(cb, uid: int, targets) -> None:
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    async def _runner():
        await _start_progress(bot, cb.message.chat.id, uid, total=len(targets), store=store, title="📺 Подписка @leoday")
        ok = 0
        for a in targets:
            ph = a["phone"]
            await _update_progress(bot, uid, store, current=ph)
            try:
                cli = await get_or_create_account_client(ph, uid)
                if not cli:
                    raise RuntimeError("connect failed")
                await asyncio.wait_for(cli(JoinChannelRequest("leoday")), timeout=5)
                ok += 1
                await _update_progress(bot, uid, store, done_inc=1, current=None)
            except Exception as e:
                await _update_progress(bot, uid, store, done_inc=1, error=f"{ph}: {e}")
            await asyncio.sleep(random.uniform(5, 15))
        await _finish_progress(bot, uid, store, summary_extra=f"Подписано: {ok}/{len(targets)}")
        await restore_main_menu(bot, cb.message.chat.id, uid)
    await task_queue.submit(_runner, owner_id=uid, notify=notify_owner, title=f"Подписка @leoday {len(targets)}")


# =================================================================
# 🏷 Смена username
# =================================================================
@router.callback_query(F.data == "auto_rtag")
async def cb_auto_rtag(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Занято.", show_alert=True)
    await cb.answer()
    await _send_target_picker(cb.message.chat.id, "rtag_t",
        "🏷 <b>Смена тега (username)</b>\n\n"
        "Сгенерирует случайный username из 3 слов + число 1–100 для выбранных аккаунтов.")


@router.callback_query(F.data.startswith("rtag_t:"))
async def cb_rtag_t(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    mode = parts[1]
    if mode == "man":
        _man_sel_ctx[uid] = "rtag_t"
        _man_selection.pop(uid, None)
        await cb.answer()
        await _show_man_submenu(cb)
        return
    targets = []
    if mode == "all":
        targets = await _resolve_targets_all(uid)
        await cb.answer()
    elif mode == "grp" and len(parts) == 2:
        await cb.answer()
        return await _send_groups_picker(uid, cb.message.chat.id, "rtag_t")
    elif mode == "gi":
        await cb.answer()
        targets = await _resolve_targets_group(uid, int(parts[2]))
    else:
        return await cb.answer("Bad", show_alert=True)
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    await _rtag_after_targets(cb, uid, targets)


async def _rtag_after_targets(cb, uid: int, targets) -> None:
    if not targets:
        return await bot.send_message(uid, "❌ Целей нет.")
    phones = [a["phone"] for a in targets]
    async def _runner():
        await _start_progress(bot, cb.message.chat.id, uid, total=len(phones), store=store, title="🏷 Смена username")
        ok = 0
        for ph in phones:
            await _update_progress(bot, uid, store, current=ph)
            try:
                cli = await get_or_create_account_client(ph, uid)
                if not cli:
                    raise RuntimeError("connect failed")
                new_un = _gen_username()
                await cli(UpdateUsernameRequest(username=new_un))
                await db.db_update_account_field(ph, "username", new_un)
                ok += 1
                await _update_progress(bot, uid, store, done_inc=1, current=None)
            except Exception as e:
                await _update_progress(bot, uid, store, done_inc=1, error=f"{ph}: {e}")
            await asyncio.sleep(random.uniform(3, 8))
        await _finish_progress(bot, uid, store, summary_extra=f"Обновлено: {ok}/{len(phones)}")
        await restore_main_menu(bot, cb.message.chat.id, uid)
    await task_queue.submit(_runner, owner_id=uid, notify=notify_owner, title=f"Смена username {len(phones)}")


# =================================================================
# 💬 Автоответы
# =================================================================
@router.callback_query(F.data == "auto_ar")
async def cb_auto_ar(cb: CallbackQuery):
    uid = cb.from_user.id
    accs = await db.db_get_accounts_by_owner(uid)
    ar_bulk = await db.db_ar_get_settings_bulk(uid)
    n_on = 0; n_run = 0; rows = []
    for a in accs[:30]:
        ph = a["phone"]
        on = bool((ar_bulk.get(ph) or {}).get("enabled"))
        running = ar_manager.is_running(ph)
        if on: n_on += 1
        if running: n_run += 1
        mark = "✅" if (on and running) else ("🟡" if on else "❌")
        rows.append([(f"{mark} {ph}", f"ar_view:{ph}")])
    rows.append([("✅ Включить все", "ar_enable_all"), ("❌ Выключить все", "ar_disable_all")])
    rows.append([("📁 По группе", "ar_by_group"), ("✏️ Текст всем", "ar_text_all")])
    rows.append([home_btn()])
    text = (
        "💬 <b>Автоответы</b>\n━━━━━━━━━━━━━━━━━━━\n"
        f"Аккаунтов: <b>{len(accs)}</b>  ·  Вкл: <b>{n_on}</b>  ·  Работает: <b>{n_run}</b>\n\n"
        "✅ включён и работает\n🟡 включён, клиент не запущен\n❌ выключен"
    )
    await cb.message.edit_text(text, reply_markup=kb(*rows))
    await cb.answer()


@router.callback_query(F.data.startswith("ar_view:"))
async def cb_ar_view(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    s = await db.db_ar_get_settings(uid, phone)
    on = bool(s.get("enabled"))
    running = ar_manager.is_running(phone)
    custom = s.get("custom_text") or "—"
    silenced = ar_manager.silenced_count(phone)
    if on and running:   status_str = "✅ Включён и работает"
    elif on:             status_str = "🟡 Включён (клиент не запущен)"
    else:                status_str = "❌ Выключен"
    text = (
        f"💬 <b>Автоответ</b>  <code>{phone}</code>\n━━━━━━━━━━━━━━━━━━━\n"
        f"📊  Статус:          {status_str}\n🔇  Замолчано чатов: <b>{silenced}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n✏️  Текст ответа:\n"
        f"<i>{(custom[:200] if custom else '— (используется стандартный) —')}</i>"
    )
    toggle_text = "❌ Выключить" if on else "✅ Включить"
    rows = [
        [(toggle_text, f"ar_toggle:{phone}"), ("✏️ Текст", f"ar_text:{phone}")],
        [("🔇 Сбросить молчание", f"ar_reset:{phone}")],
        [("‹ Назад", "auto_ar")],
    ]
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))
    await cb.answer()


@router.callback_query(F.data.startswith("ar_toggle:"))
async def cb_ar_toggle(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    on = await db.db_ar_is_enabled(uid, phone)
    new = not on
    await db.db_ar_set_enabled(uid, phone, new)
    if new:
        proxy = await get_proxy_for_account(phone, uid)
        s = await db.db_ar_get_settings(uid, phone)
        ok = await ar_manager.start(phone, uid, proxy, custom_text=s.get("custom_text"))
        await cb.answer("Включён." if ok else "Не запустился.", show_alert=not ok)
    else:
        await ar_manager.stop(phone)
        await cb.answer("Выключен.")
    await cb_ar_view(cb)


@router.callback_query(F.data.startswith("ar_text:"))
async def cb_ar_text(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    await cb.answer()
    txt = await ask_with_cancel(bot, cb.message.chat.id, uid,
        f"✏️ Свой текст для <code>{phone}</code> (или «-» чтобы сбросить):")
    if txt is None:
        return
    new = None if txt.strip() == "-" else txt.strip()[:200]
    await db.db_ar_set_custom_text(uid, phone, new)
    ar_manager.set_custom_text(phone, new)
    await cb.message.answer("✅ Текст обновлён.")


@router.callback_query(F.data.startswith("ar_reset:"))
async def cb_ar_reset(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    n = ar_manager.reset_silenced(phone)
    await cb.answer(f"Сброшено: {n}")


async def _ar_set_all(uid: int, value: bool, group: Optional[str] = None):
    if group:
        accs = await db.db_get_accounts_by_group(uid, group)
    else:
        accs = await db.db_get_accounts_by_owner(uid)
    ar_bulk = await db.db_ar_get_settings_bulk(uid)

    async def _set_one(a):
        ph = a["phone"]
        await db.db_ar_set_enabled(uid, ph, value)
        if value:
            proxy = await get_proxy_for_account(ph, uid)
            custom = (ar_bulk.get(ph) or {}).get("custom_text")
            await ar_manager.start(ph, uid, proxy, custom_text=custom)
        else:
            await ar_manager.stop(ph)

    sem = asyncio.Semaphore(10)
    async def _limited(a):
        async with sem:
            await _set_one(a)

    await asyncio.gather(*[_limited(a) for a in accs])
    return len(accs)


@router.callback_query(F.data == "ar_enable_all")
async def cb_ar_enable_all(cb: CallbackQuery):
    n = await _ar_set_all(cb.from_user.id, True)
    await cb.answer(f"Включено: {n}", show_alert=True)


@router.callback_query(F.data == "ar_disable_all")
async def cb_ar_disable_all(cb: CallbackQuery):
    n = await _ar_set_all(cb.from_user.id, False)
    await cb.answer(f"Выключено: {n}", show_alert=True)


@router.callback_query(F.data == "ar_by_group")
async def cb_ar_by_group(cb: CallbackQuery):
    uid = cb.from_user.id
    groups = await db.db_get_groups_by_owner(uid)
    if not groups:
        return await cb.answer("Групп нет.", show_alert=True)
    _grp_index_cache[uid] = groups
    rows = []
    for i, g in enumerate(groups[:20]):
        rows.append([(f"✅ {g}", f"ar_grp:on:{i}"), (f"❌ {g}", f"ar_grp:off:{i}")])
    rows.append([("‹ Назад", "auto_ar")])
    await cb.message.edit_text(
        "📁 <b>Автоответ по группе</b>\n\n✅ — включить группу  |  ❌ — выключить группу",
        reply_markup=kb(*rows))
    await cb.answer()


@router.callback_query(F.data.startswith("ar_grp:"))
async def cb_ar_grp(cb: CallbackQuery):
    parts = cb.data.split(":")
    mode = parts[1]; gi = int(parts[2])
    uid = cb.from_user.id
    groups = _grp_index_cache.get(uid, [])
    if not (0 <= gi < len(groups)):
        return await cb.answer("Bad", show_alert=True)
    n = await _ar_set_all(uid, mode == "on", group=groups[gi])
    await cb.answer(f"Готово: {n} аккаунтов.", show_alert=True)


@router.callback_query(F.data == "ar_text_all")
async def cb_ar_text_all(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    txt = await ask_with_cancel(bot, cb.message.chat.id, uid,
        "✏️ Свой текст для ВСЕХ аккаунтов (или «-» чтобы сбросить):")
    if txt is None:
        return
    new = None if txt.strip() == "-" else txt.strip()[:200]
    accs = await db.db_get_accounts_by_owner(uid)
    n = 0
    for a in accs:
        await db.db_ar_set_custom_text(uid, a["phone"], new)
        ar_manager.set_custom_text(a["phone"], new)
        n += 1
    await cb.message.answer(f"✅ Применено к {n} аккаунтам.")
