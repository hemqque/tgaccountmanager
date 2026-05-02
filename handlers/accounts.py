# -*- coding: utf-8 -*-
"""
handlers/accounts.py — Раздел Аккаунты:
  добавление, импорт TData/Session, список, карточка,
  личные прокси и глобальные прокси.
"""

import asyncio
import glob as _glob
import logging
import os
import random
import re
import shutil
import time
from typing import Any, Dict, List, Optional, Tuple

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.exceptions import TelegramBadRequest

from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
)
from telethon.tl.functions.account import (
    UpdateProfileRequest, UpdateUsernameRequest, GetPasswordRequest,
)
from telethon.tl.functions.photos import (
    UploadProfilePhotoRequest, DeletePhotosRequest, GetUserPhotosRequest,
)
from telethon.tl.types import (
    InputPhoto,
    InputPrivacyValueAllowAll, InputPrivacyValueDisallowAll,
    InputPrivacyKeyStatusTimestamp, InputPrivacyKeyProfilePhoto,
    InputPrivacyKeyForwards, InputPrivacyKeyPhoneCall,
    InputPrivacyKeyVoiceMessages, InputPrivacyKeyPhoneNumber,
    InputPrivacyKeyChatInvite,
)
from telethon.tl.functions.account import SetPrivacyRequest

import config
import db
from bot_globals import (
    bot, store, task_queue, ar_manager,
    _signin_sessions, _tdata_sessions,
    _man_sel_ctx, _man_selection,
    kb, home_btn, notify_owner, get_or_create_account_client, user_log,
)
from utils import (
    restore_main_menu, ask_with_cancel, ask_with_retry,
    validate_phone, validate_proxy, auto_join_channels,
    cancel_pending_ask,
)
from global_proxy import (
    proxy_to_telethon, get_proxy_for_account,
    check_proxy_connection, reassign_phones,
    get_sticky_global_proxy, mask_proxy, proxy_host,
    parse_proxy_string,
)
import client_pool as _client_pool
from account_setup import setup_account
from progress import _start_progress, _update_progress, _finish_progress
from handlers.helpers import (
    _send_target_picker, _send_groups_picker,
    _resolve_targets_all, _resolve_targets_group, _resolve_targets_manual,
)

log = logging.getLogger("accounts")
router = Router(name="accounts")

# Флаги отмены пакетного добавления: uid -> bool
_batch_cancel: Dict[int, bool] = {}

# Данные массовой смены 2FA: uid -> {targets, cur_pwd, new_pwd}
_2fa_pending: Dict[int, Dict] = {}


async def _own(phone: str, uid: int) -> bool:
    """True если аккаунт phone принадлежит uid. Используется для проверки прав."""
    a = await db.db_get_account(phone)
    return bool(a and a.get("owner_id") == uid)


# =================================================================
# ДОБАВЛЕНИЕ АККАУНТОВ
# =================================================================
@router.callback_query(F.data == "acc_add")
async def cb_acc_add(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("Уже идёт другое действие.", show_alert=True)
    store.set_action(uid, "acc_add")
    await cb.answer()

    def _has_phones(text: str) -> bool:
        return any(validate_phone(t.strip())
                   for t in re.split(r"[,\n;]+", text) if t.strip())

    task_submitted = False
    try:
        raw = await ask_with_retry(
            bot, cb.message.chat.id, uid,
            "Пришлите номера телефонов (через запятую или с новой строки).\n"
            "Пример:\n+79991112233\n+79994445566",
            validator=_has_phones,
            error_msg="Не нашёл валидных номеров. Формат: +79991112233",
        )
        if raw is None:
            await restore_main_menu(bot, cb.message.chat.id, uid, "Отменено.")
            return
        phones = []
        for tok in re.split(r"[,\n;]+", raw):
            tok = tok.strip()
            if tok:
                p = validate_phone(tok)
                if p:
                    phones.append(p)
        phones = list(dict.fromkeys(phones))

        _batch_cancel[uid] = False
        cancel_msg = await bot.send_message(
            cb.message.chat.id,
            f"Добавление аккаунтов. В очереди: {len(phones)} номеров",
            reply_markup=kb([("Прервать добавление", "acc_add_cancel")]),
        )

        async def _run_add():
            try:
                await _start_progress(bot, cb.message.chat.id, uid,
                                      total=len(phones), store=store,
                                      title="Добавление аккаунтов")
                ok_count = 0
                for ph in phones:
                    if _batch_cancel.get(uid):
                        break
                    await _update_progress(bot, uid, store, current=ph)
                    try:
                        res = await _add_one_account(uid, cb.message.chat.id, ph)
                        if res:
                            ok_count += 1
                        await _update_progress(bot, uid, store, done_inc=1,
                                               current=None,
                                               error=None if res else f"{ph}: не добавлен")
                    except Exception as e:
                        await _update_progress(bot, uid, store, done_inc=1,
                                               current=None, error=f"{ph}: {e}")
                was_cancelled = bool(_batch_cancel.get(uid))
                _batch_cancel.pop(uid, None)
                try:
                    await cancel_msg.delete()
                except Exception:
                    pass
                cancelled_note = " (отменено)" if was_cancelled else ""
                await _finish_progress(bot, uid, store,
                                       summary_extra=f"Добавлено: {ok_count}/{len(phones)}{cancelled_note}")
                await restore_main_menu(bot, cb.message.chat.id, uid)
            finally:
                # Сбрасываем action только когда задача реально завершилась
                store.set_action(uid, None)

        await task_queue.submit(
            _run_add, owner_id=uid, notify=notify_owner,
            title=f"Добавление {len(phones)} аккаунтов",
        )
        task_submitted = True
    finally:
        # Сбрасываем action только если задача не была отправлена в очередь
        # (т.е. произошла ошибка до submit). Иначе action сбросит сам _run_add.
        if not task_submitted:
            store.set_action(uid, None)


async def _add_one_account(uid: int, chat_id: int, phone: str) -> bool:
    """Полный сценарий добавления одного аккаунта. Возвращает True/False."""
    proxy_raw = await ask_with_retry(
        bot, chat_id, uid,
        f"Прокси для {phone} (host:port:user:pass или Нет):",
        validator=validate_proxy,
        error_msg="Некорректный формат. Введите host:port:user:pass или Нет.",
        parse_mode="HTML",
    )
    if proxy_raw is None:
        await bot.send_message(chat_id, f"Пропущено: {phone}")
        return False
    if proxy_raw.strip().lower() in ("нет", "no", "none", "-", "без прокси"):
        g = await get_sticky_global_proxy(phone)
        proxy_str = g["proxy_str"] if g else ""
    else:
        proxy_str = proxy_raw.strip()

    os.makedirs(config.SESSIONS_DIR, exist_ok=True)
    session_path = os.path.join(config.SESSIONS_DIR, phone)
    tproxy = proxy_to_telethon(proxy_str)
    await _client_pool.remove(phone)
    client = TelegramClient(session_path, config.API_ID, config.API_HASH, proxy=tproxy)
    try:
        await client.connect()
        if await client.is_user_authorized():
            existing = await db.db_get_account(phone)
            if existing:
                if proxy_str and existing.get("proxy") != proxy_str:
                    await db.db_update_account_field(phone, "proxy", proxy_str)
                await bot.send_message(uid, f"{phone} уже есть в базе — пропускаю.")
                await client.disconnect()
                return True
            await bot.send_message(uid, f"{phone}: уже авторизован, добавляю в базу...")
        else:
            sent = await client.send_code_request(phone)
            signed_in = False
            for code_attempt in range(5):
                code = await ask_with_cancel(
                    bot, chat_id, uid,
                    f"Введите SMS-код для {phone}:",
                    parse_mode="HTML",
                )
                if not code:
                    await bot.send_message(uid, f"Отменено: {phone}")
                    await client.disconnect()
                    return False
                try:
                    await client.sign_in(phone=phone, code=code.strip(),
                                         phone_code_hash=sent.phone_code_hash)
                    signed_in = True
                    break
                except SessionPasswordNeededError:
                    for pwd_attempt in range(5):
                        pwd = await ask_with_cancel(
                            bot, chat_id, uid,
                            f"2FA-пароль для {phone}:",
                            parse_mode="HTML",
                        )
                        if not pwd:
                            await bot.send_message(uid, f"Отменено: {phone}")
                            await client.disconnect()
                            return False
                        try:
                            await client.sign_in(password=pwd.strip())
                            signed_in = True
                            break
                        except Exception:
                            remaining = 4 - pwd_attempt
                            if remaining <= 0:
                                await bot.send_message(uid, f"{phone}: 2FA попытки исчерпаны.")
                                await client.disconnect()
                                return False
                            await bot.send_message(uid, f"Неверный 2FA-пароль. Осталось: {remaining}.")
                    break
                except (PhoneCodeInvalidError, PhoneCodeExpiredError):
                    remaining = 4 - code_attempt
                    if remaining <= 0:
                        await bot.send_message(uid, f"{phone}: код попытки исчерпаны.")
                        await client.disconnect()
                        return False
                    await bot.send_message(uid, f"Неверный код. Осталось: {remaining}.")
            if not signed_in:
                await client.disconnect()
                return False

        async def _logf(t: str):
            await user_log(uid, f"{phone}: {t}")

        setup_res = await setup_account(client, uid, _logf)
        username = setup_res.get("username_set") or ""
        await auto_join_channels(client, _logf)
        await db.db_add_account(phone, proxy_str, "", "", username, uid)

        groups = await db.db_get_groups_by_owner(uid)
        rows = []
        for g in groups[:8]:
            rows.append([(f"{g}", f"acc_grpset:{phone}:{g}")])
        rows.append([("Новая группа", f"acc_grpnew:{phone}")])
        rows.append([("Без группы", f"acc_grpset:{phone}:")])
        _signin_sessions[uid] = {"phone": phone, "chat_id": chat_id}
        await bot.send_message(
            chat_id,
            f"{phone} добавлен (@{username or '-'}).\nВыберите группу:",
            reply_markup=kb(*rows),
        )
        await client.disconnect()
        return True
    except Exception as e:
        log.warning("add account %s: %s", phone, e)
        try:
            await client.disconnect()
        except Exception:
            pass
        await bot.send_message(uid, f"{phone}: {e}")
        return False


@router.callback_query(F.data == "acc_add_cancel")
async def cb_acc_add_cancel(cb: CallbackQuery):
    uid = cb.from_user.id
    _batch_cancel[uid] = True
    cancel_pending_ask(uid)
    await cb.answer("Добавление отменено.", show_alert=True)
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass


@router.callback_query(F.data.startswith("acc_grpset:"))
async def cb_acc_grpset(cb: CallbackQuery):
    parts = cb.data.split(":", 2)
    if len(parts) < 3:
        return await cb.answer("Bad data", show_alert=True)
    phone = parts[1]
    grp = parts[2]
    uid = cb.from_user.id
    await db.db_update_account_field(phone, "grp", grp)
    _signin_sessions.pop(uid, None)
    await cb.message.edit_text(f"{phone} - группа: {grp or 'без группы'}")
    await cb.answer()


@router.callback_query(F.data.startswith("acc_grpnew:"))
async def cb_acc_grpnew(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    await cb.answer()
    name = await ask_with_cancel(bot, cb.message.chat.id, uid, "Название новой группы:")
    if not name:
        return await cb.message.answer("Отменено.")
    name = name.strip()[:32]
    await db.db_update_account_field(phone, "grp", name)
    _signin_sessions.pop(uid, None)
    await cb.message.answer(f"{phone} - группа: {name}")


# =================================================================
# ИМПОРТ TData
# =================================================================
@router.callback_query(F.data == "acc_tdata")
async def cb_acc_tdata(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("Уже идёт другое действие.", show_alert=True)
    store.set_action(uid, "acc_tdata_wait")
    _tdata_sessions[uid] = {"chat_id": cb.message.chat.id, "group": ""}
    await cb.answer()
    await bot.send_message(
        cb.message.chat.id,
        "Импорт TData\n\nПришлите ZIP-архив с папками tdata от Telegram Desktop.",
        reply_markup=kb([("Отмена", "action_cancel")]),
    )


@router.message(F.document)
async def handle_document(msg: Message):
    uid = msg.from_user.id
    mode = store.active_action.get(uid)
    if mode not in ("acc_tdata_wait", "acc_session_wait"):
        return
    sess = _tdata_sessions.get(uid) or {}
    chat_id = sess.get("chat_id") or msg.chat.id

    doc = msg.document
    file_name = (doc.file_name or "archive.zip")
    if not file_name.lower().endswith(".zip"):
        return await msg.answer("Нужен файл с расширением .zip")

    work_dir = os.path.join(
        config.TEMP_DIR,
        f"{'tdata' if mode == 'acc_tdata_wait' else 'session'}_{uid}_{int(time.time())}",
    )
    os.makedirs(work_dir, exist_ok=True)
    zip_path = os.path.join(work_dir, file_name)
    try:
        f = await bot.get_file(doc.file_id)
        await bot.download_file(f.file_path, destination=zip_path)
    except Exception as e:
        store.set_action(uid, None)
        _tdata_sessions.pop(uid, None)
        return await msg.answer(f"Не получилось скачать: {e}")

    if mode == "acc_session_wait":
        await msg.answer("Архив получен, ищу .session файлы...")

        async def _session_runner():
            from tdata_import import import_sessions_from_archive
            try:
                results = await import_sessions_from_archive(
                    archive_path=zip_path, work_dir=work_dir,
                    sessions_dir=config.SESSIONS_DIR,
                    api_id=config.API_ID, api_hash=config.API_HASH,
                    proxy=None,
                )
            except Exception as e:
                await bot.send_message(uid, f"Импорт сессий упал: {e}")
                store.set_action(uid, None)
                _tdata_sessions.pop(uid, None)
                await restore_main_menu(bot, chat_id, uid)
                return
            await _write_session_report(uid, results, "Импорт сессий завершён")
            try:
                shutil.rmtree(work_dir, ignore_errors=True)
            except Exception:
                pass
            store.set_action(uid, None)
            _tdata_sessions.pop(uid, None)
            await restore_main_menu(bot, chat_id, uid)

        await task_queue.submit(_session_runner, owner_id=uid, notify=notify_owner, title="Импорт сессий")
        return

    await msg.answer("Архив получен, распаковываю и импортирую...")

    async def _runner():
        from tdata_import import import_from_archive
        try:
            results = await import_from_archive(
                archive_path=zip_path, work_dir=work_dir,
                sessions_dir=config.SESSIONS_DIR, proxy=None,
            )
        except Exception as e:
            await bot.send_message(uid, f"Импорт TData упал: {e}")
            store.set_action(uid, None)
            _tdata_sessions.pop(uid, None)
            await restore_main_menu(bot, chat_id, uid)
            return

        ok_count = 0
        err_count = 0
        lines: List[str] = []
        for tdata_folder, phone, err in results:
            short = os.path.basename(tdata_folder.rstrip(os.sep))
            if phone and not err:
                try:
                    existing = await db.db_get_account(phone)
                    if existing:
                        await db.db_add_account(
                            phone, existing.get("proxy") or "",
                            existing.get("note") or "",
                            existing.get("grp") or "",
                            existing.get("username") or "", uid,
                        )
                        lines.append(f"{phone} - обновлён (был в БД)")
                    else:
                        await db.db_add_account(phone, "", "tdata-import", "", "", uid)
                        lines.append(f"{phone} - импортирован")
                    ok_count += 1
                except Exception as e:
                    err_count += 1
                    lines.append(f"{short}: db error {e}")
            else:
                err_count += 1
                lines.append(f"{short}: {err or 'unknown'}")

        text = (
            f"Импорт TData завершён\n"
            f"Найдено TData: {len(results)}\n"
            f"Успешно: {ok_count}\n"
            f"Ошибок: {err_count}\n\n"
            + "\n".join(lines[:25])
        )
        if len(lines) > 25:
            text += f"\n...ещё {len(lines) - 25} строк"
        try:
            await bot.send_message(uid, text)
        except Exception:
            pass
        try:
            shutil.rmtree(work_dir, ignore_errors=True)
        except Exception:
            pass
        store.set_action(uid, None)
        _tdata_sessions.pop(uid, None)
        await restore_main_menu(bot, chat_id, uid)

    await task_queue.submit(_runner, owner_id=uid, notify=notify_owner, title="Импорт TData")


@router.callback_query(F.data == "acc_tdata_local")
async def cb_acc_tdata_local(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("Уже идёт другое действие.", show_alert=True)
    store.set_action(uid, "acc_tdata_local")
    await cb.answer()
    _confirmed = False
    try:
        path_raw = await ask_with_retry(
            bot, cb.message.chat.id, uid,
            "Импорт TData из локальной папки\n\n"
            "Пришлите абсолютный путь к папке с tdata.",
            validator=lambda p: os.path.isdir(p.strip().strip('"').strip("'")),
            error_msg="Папка не найдена. Укажите корректный абсолютный путь.",
            parse_mode="HTML",
        )
        if not path_raw:
            await restore_main_menu(bot, cb.message.chat.id, uid, "Отменено.")
            return
        local_path = path_raw.strip().strip('"').strip("'")

        from tdata_import import find_tdata_folders, validate_tdata_structure
        found = find_tdata_folders(local_path)
        if not found:
            await bot.send_message(uid, f"В {local_path} не нашлось ни одной tdata.")
            await restore_main_menu(bot, cb.message.chat.id, uid)
            return

        valid: List[str] = []
        broken: List[Tuple[str, str]] = []
        for p in found:
            v_err = validate_tdata_structure(p)
            if v_err:
                broken.append((p, v_err))
            else:
                valid.append(p)

        preview = (
            f"Найдено TData: {len(found)}\n"
            f"Годных: {len(valid)}\n"
            f"Битых: {len(broken)}\n\n"
        )
        if valid:
            preview += "Годные (первые 20):\n"
            for p in valid[:20]:
                short = os.path.relpath(p, local_path) or os.path.basename(p)
                preview += f"  {short}\n"
            if len(valid) > 20:
                preview += f"  ...и ещё {len(valid) - 20}\n"

        _tdata_sessions[uid] = {"chat_id": cb.message.chat.id, "local_path": local_path}
        _confirmed = True
        await bot.send_message(
            cb.message.chat.id,
            preview + "\nИмпортировать всё это?",
            reply_markup=kb(
                [("Импортировать", "acc_tdata_local_run")],
                [("Отмена", "action_cancel")],
            ),
        )
    finally:
        if not _confirmed:
            store.set_action(uid, None)


@router.callback_query(F.data == "acc_tdata_local_run")
async def cb_acc_tdata_local_run(cb: CallbackQuery):
    uid = cb.from_user.id
    sess = _tdata_sessions.get(uid) or {}
    local_path = sess.get("local_path")
    chat_id = sess.get("chat_id") or cb.message.chat.id
    if not local_path:
        store.set_action(uid, None)
        return await cb.answer("Сессия импорта пропала, начни заново.", show_alert=True)
    await cb.answer("Запускаю импорт...")

    async def _runner():
        from tdata_import import import_from_local_folder
        try:
            results = await import_from_local_folder(
                root=local_path, sessions_dir=config.SESSIONS_DIR, proxy=None,
            )
        except Exception as e:
            await bot.send_message(uid, f"Импорт TData упал: {e}")
            store.set_action(uid, None)
            _tdata_sessions.pop(uid, None)
            await restore_main_menu(bot, chat_id, uid)
            return

        ok_count = 0
        err_count = 0
        lines: List[str] = []
        for tdata_folder, phone, err in results:
            short = os.path.basename(tdata_folder.rstrip(os.sep))
            if phone and not err:
                try:
                    existing = await db.db_get_account(phone)
                    if existing:
                        await db.db_add_account(
                            phone, existing.get("proxy") or "",
                            existing.get("note") or "",
                            existing.get("grp") or "",
                            existing.get("username") or "", uid,
                        )
                        lines.append(f"{phone} - обновлён")
                    else:
                        await db.db_add_account(phone, "", "tdata-import", "", "", uid)
                        lines.append(f"{phone} - импортирован")
                    ok_count += 1
                except Exception as e:
                    err_count += 1
                    lines.append(f"{short}: db error {e}")
            else:
                err_count += 1
                lines.append(f"{short}: {err or 'unknown'}")

        text = (
            f"Локальный импорт TData завершён\n"
            f"Найдено: {len(results)}\nУспешно: {ok_count}\nОшибок: {err_count}\n\n"
            + "\n".join(lines[:25])
        )
        if len(lines) > 25:
            text += f"\n...ещё {len(lines) - 25} строк"
        try:
            await bot.send_message(uid, text)
        except Exception:
            pass
        store.set_action(uid, None)
        _tdata_sessions.pop(uid, None)
        await restore_main_menu(bot, chat_id, uid)

    await task_queue.submit(_runner, owner_id=uid, notify=notify_owner, title="Локальный импорт TData")


# =================================================================
# ИМПОРТ .SESSION
# =================================================================
@router.callback_query(F.data == "acc_session_zip")
async def cb_acc_session_zip(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("Уже идёт другое действие.", show_alert=True)
    store.set_action(uid, "acc_session_wait")
    _tdata_sessions[uid] = {"chat_id": cb.message.chat.id}
    await cb.answer()
    await bot.send_message(
        cb.message.chat.id,
        "Импорт .session файлов (ZIP)\n\nПришлите ZIP-архив с файлами *.session от Telethon.",
        reply_markup=kb([("Отмена", "action_cancel")]),
    )


@router.callback_query(F.data == "acc_session_local")
async def cb_acc_session_local(cb: CallbackQuery):
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("Уже идёт другое действие.", show_alert=True)
    store.set_action(uid, "acc_session_local")
    await cb.answer()
    _confirmed = False
    try:
        path_raw = await ask_with_retry(
            bot, cb.message.chat.id, uid,
            "Импорт .session файлов (локальная папка)\n\n"
            "Пришлите абсолютный путь к папке со .session файлами.",
            validator=lambda p: os.path.isdir(p.strip().strip('"').strip("'")),
            error_msg="Папка не найдена. Укажите корректный абсолютный путь.",
            parse_mode="HTML",
        )
        if not path_raw:
            await restore_main_menu(bot, cb.message.chat.id, uid, "Отменено.")
            return
        local_path = path_raw.strip().strip('"').strip("'")

        from tdata_import import find_session_files
        files = find_session_files(local_path)
        if not files:
            await bot.send_message(uid, f"В {local_path} не нашлось *.session файлов.")
            await restore_main_menu(bot, cb.message.chat.id, uid)
            return

        preview = f"Найдено .session: {len(files)}\n"
        for p in files[:30]:
            short = os.path.relpath(p, local_path) or os.path.basename(p)
            preview += f"  {short}\n"
        if len(files) > 30:
            preview += f"  ...и ещё {len(files) - 30}\n"

        _tdata_sessions[uid] = {"chat_id": cb.message.chat.id, "local_path": local_path}
        _confirmed = True
        await bot.send_message(
            cb.message.chat.id,
            preview + "\nИмпортировать?",
            reply_markup=kb(
                [("Импортировать", "acc_session_local_run")],
                [("Отмена", "action_cancel")],
            ),
        )
    finally:
        if not _confirmed:
            store.set_action(uid, None)


@router.callback_query(F.data == "acc_session_local_run")
async def cb_acc_session_local_run(cb: CallbackQuery):
    uid = cb.from_user.id
    sess = _tdata_sessions.get(uid) or {}
    local_path = sess.get("local_path")
    chat_id = sess.get("chat_id") or cb.message.chat.id
    if not local_path:
        store.set_action(uid, None)
        return await cb.answer("Сессия импорта пропала.", show_alert=True)
    await cb.answer("Запускаю...")

    async def _runner():
        from tdata_import import import_sessions_from_folder
        try:
            results = await import_sessions_from_folder(
                root=local_path, sessions_dir=config.SESSIONS_DIR,
                api_id=config.API_ID, api_hash=config.API_HASH, proxy=None,
            )
        except Exception as e:
            await bot.send_message(uid, f"Импорт упал: {e}")
            store.set_action(uid, None)
            _tdata_sessions.pop(uid, None)
            await restore_main_menu(bot, chat_id, uid)
            return
        await _write_session_report(uid, results, "Локальный импорт сессий")
        store.set_action(uid, None)
        _tdata_sessions.pop(uid, None)
        await restore_main_menu(bot, chat_id, uid)

    await task_queue.submit(_runner, owner_id=uid, notify=notify_owner, title="Локальный импорт сессий")


async def _write_session_report(uid: int, results, title: str) -> None:
    ok_count = 0
    err_count = 0
    lines: List[str] = []
    for src, phone, err in results:
        short = os.path.basename(src)
        if phone and not err:
            try:
                existing = await db.db_get_account(phone)
                if existing:
                    await db.db_add_account(
                        phone, existing.get("proxy") or "",
                        existing.get("note") or "",
                        existing.get("grp") or "",
                        existing.get("username") or "", uid,
                    )
                    lines.append(f"{phone} - обновлён")
                else:
                    await db.db_add_account(phone, "", "session-import", "", "", uid)
                    lines.append(f"{phone} - импортирован")
                ok_count += 1
            except Exception as e:
                err_count += 1
                lines.append(f"{short}: db error {e}")
        else:
            err_count += 1
            lines.append(f"{short}: {err or 'unknown'}")

    text = (
        f"{title}\nНайдено сессий: {len(results)}\n"
        f"Успешно: {ok_count}\nОшибок: {err_count}\n\n"
        + "\n".join(lines[:25])
    )
    if len(lines) > 25:
        text += f"\n...ещё {len(lines) - 25} строк"
    try:
        await bot.send_message(uid, text)
    except Exception:
        pass


# =================================================================
# СПИСОК АККАУНТОВ
# =================================================================
@router.callback_query(F.data.startswith("acc_list:"))
async def cb_acc_list(cb: CallbackQuery):
    uid = cb.from_user.id
    try:
        page = int(cb.data.split(":", 1)[1])
    except Exception:
        page = 0
    accs = await db.db_get_accounts_by_owner(uid)
    per = config.ACCOUNTS_PER_PAGE
    total = len(accs)
    pages = max(1, (total + per - 1) // per)
    page = max(0, min(page, pages - 1))
    chunk = accs[page * per:(page + 1) * per]

    rows = []
    for a in chunk:
        ph = a["phone"]
        un = a.get("username") or "-"
        nt = (a.get("note") or "")[:24]
        rows.append([
            (f"{ph} (@{un})" + (f" - {nt}" if nt else ""), f"acc_card:{ph}")
        ])
    nav = []
    if page > 0:
        nav.append(("<<", f"acc_list:{page-1}"))
    nav.append((f"{page+1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append((">>", f"acc_list:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([("Сбросить все сессии", "acc_reset_all")])
    rows.append([home_btn()])
    text = f"Мои аккаунты\nВсего: {total}  Стр. {page+1}/{pages}"
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))
    await cb.answer()


@router.callback_query(F.data == "acc_reset_all")
async def cb_acc_reset_all(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()
    confirm = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "Удалить ВСЕ аккаунты?\nДля подтверждения напишите ДА:",
        validator=lambda t: t.strip().lower() == "да",
        error_msg='Напишите именно "ДА" для подтверждения.',
        parse_mode="HTML",
    )
    if not confirm:
        return await cb.message.answer("Отменено - аккаунты не удалены.")
    accs = await db.db_get_accounts_by_owner(uid)
    n = 0
    for a in accs:
        try:
            await db.db_delete_account(a["phone"])
            base = os.path.join(config.SESSIONS_DIR, a["phone"])
            for _sp in _glob.glob(base + "*.session*"):
                try:
                    os.remove(_sp)
                except Exception:
                    pass
            n += 1
        except Exception:
            pass
    await cb.message.answer(f"Удалено: {n} аккаунтов.")


# =================================================================
# КАРТОЧКА АККАУНТА
# =================================================================
async def _render_account_card(phone: str, owner_id: int):
    a = await db.db_get_account(phone)
    if not a or a.get("owner_id") != owner_id:
        return None, None
    ar_settings = await db.db_ar_get_settings(owner_id, phone)
    ar_on = bool(ar_settings.get("enabled"))

    own_proxy = (a.get("proxy") or "").strip()
    if own_proxy:
        proxy_line = own_proxy
    else:
        gp = await get_sticky_global_proxy(phone)
        if gp:
            host = proxy_host(gp["proxy_str"]) or "?"
            proxy_line = f"(через глобал) {host}"
        else:
            proxy_line = "без прокси"

    un = a.get("username") or "-"
    text = (
        f"Аккаунт: {a['phone']} (@{un})\n"
        f"Группа: {a.get('grp') or '-'}\n"
        f"Заметка: {a.get('note') or '-'}\n"
        f"Прокси: {proxy_line}\n"
        f"Автоответ: {'включён' if ar_on else 'выключен'}"
    )
    rows = [
        [("Имя", f"acc_name:{phone}"), ("Био", f"acc_bio:{phone}"), ("Username", f"acc_uname:{phone}")],
        [("Фото", f"acc_photo:{phone}"), ("Заметка", f"acc_note:{phone}"), ("Группа", f"acc_grp:{phone}")],
        [("🔑 2FA", f"acc_2fa:{phone}"), ("Приватность", f"acc_priv:{phone}"), ("Получить код", f"acc_code:{phone}")],
        [("Удалить аккаунт", f"acc_del:{phone}")],
        [("< Назад", "acc_list:0"), home_btn()],
    ]
    return text, kb(*rows)


@router.callback_query(F.data.startswith("acc_card:"))
async def cb_acc_card(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    text, mk = await _render_account_card(phone, cb.from_user.id)
    if not text:
        return await cb.answer("Аккаунт не найден.", show_alert=True)
    try:
        await cb.message.edit_text(text, reply_markup=mk)
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=mk)
    await cb.answer()


@router.callback_query(F.data.startswith("acc_note:"))
async def cb_acc_note(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    if not await _own(phone, uid):
        return await cb.answer("Аккаунт не найден.", show_alert=True)
    await cb.answer()
    text = await ask_with_cancel(bot, cb.message.chat.id, uid, f"Новая заметка для {phone}:")
    if text is None:
        return
    await db.db_update_account_field(phone, "note", text.strip()[:256])
    await cb.message.answer("Заметка обновлена.")


@router.callback_query(F.data.startswith("acc_grp:"))
async def cb_acc_grp(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    if not await _own(phone, uid):
        return await cb.answer("Аккаунт не найден.", show_alert=True)
    await cb.answer()
    groups = (await db.db_get_groups_by_owner(uid))[:8]
    rows = [[(f"{g}", f"acc_grpset2:{phone}:{g}")] for g in groups]
    rows.append([("Новая группа", f"acc_grpnew2:{phone}")])
    rows.append([("Без группы", f"acc_grpset2:{phone}:")])
    rows.append([("< Назад", f"acc_card:{phone}")])
    await cb.message.answer(f"Выберите группу для {phone}:", reply_markup=kb(*rows))


@router.callback_query(F.data.startswith("acc_grpset2:"))
async def cb_acc_grpset2(cb: CallbackQuery):
    parts = cb.data.split(":", 2)
    phone = parts[1]
    grp = parts[2] if len(parts) > 2 else ""
    await db.db_update_account_field(phone, "grp", grp)
    await cb.answer("Группа обновлена.")
    text, mk = await _render_account_card(phone, cb.from_user.id)
    if text:
        try:
            await cb.message.edit_text(text, reply_markup=mk)
        except TelegramBadRequest:
            await cb.message.answer(text, reply_markup=mk)


@router.callback_query(F.data.startswith("acc_grpnew2:"))
async def cb_acc_grpnew2(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    await cb.answer()
    name = await ask_with_cancel(bot, cb.message.chat.id, uid, "Название новой группы:")
    if not name:
        return
    await db.db_update_account_field(phone, "grp", name.strip()[:32])
    text, mk = await _render_account_card(phone, uid)
    if text:
        await cb.message.answer(text, reply_markup=mk)


@router.callback_query(F.data.startswith("acc_name:"))
async def cb_acc_name(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    if not await _own(phone, uid):
        return await cb.answer("Аккаунт не найден.", show_alert=True)
    await cb.answer()
    new_name = await ask_with_cancel(bot, cb.message.chat.id, uid, f"Новое имя для {phone}:")
    if not new_name:
        return
    cli = await get_or_create_account_client(phone, uid)
    if not cli:
        return await cb.message.answer("Не удалось подключиться.")
    try:
        await cli(UpdateProfileRequest(first_name=new_name.strip()[:64]))
        await cb.message.answer("Имя обновлено.")
    except Exception as e:
        await cb.message.answer(f"Ошибка: {e}")


@router.callback_query(F.data.startswith("acc_bio:"))
async def cb_acc_bio(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    if not await _own(phone, uid):
        return await cb.answer("Аккаунт не найден.", show_alert=True)
    await cb.answer()
    bio = await ask_with_cancel(bot, cb.message.chat.id, uid, f"Новое био для {phone} (до 70 симв.):")
    if bio is None:
        return
    cli = await get_or_create_account_client(phone, uid)
    if not cli:
        return await cb.message.answer("Не удалось подключиться.")
    try:
        await cli(UpdateProfileRequest(about=bio.strip()[:70]))
        await cb.message.answer("Био обновлено.")
    except Exception as e:
        await cb.message.answer(f"Ошибка: {e}")


@router.callback_query(F.data.startswith("acc_photo:"))
async def cb_acc_photo(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    if not await _own(phone, uid):
        return await cb.answer("Аккаунт не найден.", show_alert=True)
    await cb.answer()
    store.photo_collecting[uid] = True
    store.clear_temp_photos(uid)
    await bot.send_message(
        cb.message.chat.id,
        f"Пришлите ОДНО фото для {phone}. После - нажмите Готово.",
        reply_markup=kb([("Готово", f"acc_photodone:{phone}")], [("Отмена", "action_cancel")]),
    )


@router.callback_query(F.data.startswith("acc_photodone:"))
async def cb_acc_photodone(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    photos = store.get_temp_photos(uid)
    store.photo_collecting[uid] = False
    store.clear_temp_photos(uid)
    await cb.answer()
    if not photos:
        return await cb.message.answer("Фото не получено.")
    cli = await get_or_create_account_client(phone, uid)
    if not cli:
        return await cb.message.answer("Не удалось подключиться.")
    try:
        try:
            existing = await cli(GetUserPhotosRequest(user_id="me", offset=0, max_id=0, limit=10))
            input_photos = [
                InputPhoto(id=p.id, access_hash=p.access_hash, file_reference=p.file_reference)
                for p in existing.photos
            ]
            if input_photos:
                await cli(DeletePhotosRequest(id=input_photos))
        except Exception:
            pass
        await cli(UploadProfilePhotoRequest(file=await cli.upload_file(photos[0])))
        await cb.message.answer("Фото профиля обновлено.")
    except Exception as e:
        await cb.message.answer(f"Ошибка: {e}")


@router.callback_query(F.data.startswith("acc_uname:"))
async def cb_acc_uname(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    if not await _own(phone, uid):
        return await cb.answer("Аккаунт не найден.", show_alert=True)
    await cb.answer()
    new_un = await ask_with_cancel(bot, cb.message.chat.id, uid, f"Новый username для {phone} (без @):")
    if not new_un:
        return
    new_un = new_un.strip().lstrip("@")
    cli = await get_or_create_account_client(phone, uid)
    if not cli:
        return await cb.message.answer("Не удалось подключиться.")
    try:
        await cli(UpdateUsernameRequest(username=new_un))
        await db.db_update_account_field(phone, "username", new_un)
        await cb.message.answer(f"Username @{new_un} установлен.")
    except Exception as e:
        await cb.message.answer(f"Ошибка: {e}")


@router.callback_query(F.data.startswith("acc_priv:"))
async def cb_acc_priv(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    if not await _own(phone, cb.from_user.id):
        return await cb.answer("Аккаунт не найден.", show_alert=True)
    await cb.answer()
    await cb.message.answer(
        f"Приватность для {phone}\nНастройте видимость профиля.",
        reply_markup=kb(
            [("Закрыть всё", f"acc_privset:{phone}:close"),
             ("Открыть всё", f"acc_privset:{phone}:open")],
            [("< Назад", f"acc_card:{phone}")],
        ),
    )


@router.callback_query(F.data.startswith("acc_privset:"))
async def cb_acc_privset(cb: CallbackQuery):
    parts = cb.data.split(":", 2)
    phone, mode = parts[1], parts[2]
    uid = cb.from_user.id
    if not await _own(phone, uid):
        return await cb.answer("Аккаунт не найден.", show_alert=True)
    cli = await get_or_create_account_client(phone, uid)
    if not cli:
        return await cb.answer("Не подключились.", show_alert=True)
    keys = [
        InputPrivacyKeyStatusTimestamp(), InputPrivacyKeyProfilePhoto(),
        InputPrivacyKeyForwards(), InputPrivacyKeyPhoneCall(),
        InputPrivacyKeyVoiceMessages(), InputPrivacyKeyPhoneNumber(),
        InputPrivacyKeyChatInvite(),
    ]
    rule = (InputPrivacyValueDisallowAll() if mode == "close" else InputPrivacyValueAllowAll())
    errs = 0
    for k in keys:
        try:
            await cli(SetPrivacyRequest(key=k, rules=[rule]))
        except Exception:
            errs += 1
    await cb.answer(("Закрыто" if mode == "close" else "Открыто") + (f" ({errs} ошибок)" if errs else ""))


@router.callback_query(F.data.startswith("acc_code:"))
async def cb_acc_code(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    if not await _own(phone, uid):
        return await cb.answer("Аккаунт не найден.", show_alert=True)
    cli = await get_or_create_account_client(phone, uid)
    if not cli:
        return await cb.answer("Не подключились.", show_alert=True)
    found = None
    try:
        for sender in (777000, 42777):
            try:
                msgs = await cli.get_messages(sender, limit=1)
                if msgs:
                    found = msgs[0]
                    break
            except Exception:
                continue
    except Exception:
        pass
    await cb.answer()
    if not found:
        return await cb.message.answer("Сообщений от Telegram не найдено.")
    text = found.text or found.message or ""
    await cb.message.answer(f"Последнее от Telegram:\n{text[:1000]}")


@router.callback_query(F.data.startswith("acc_del:"))
async def cb_acc_del(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    if not await _own(phone, cb.from_user.id):
        return await cb.answer("Аккаунт не найден.", show_alert=True)
    await cb.answer()
    await cb.message.answer(
        f"Удалить аккаунт {phone}?\n\nБудут удалены: .session-файл, задачи LDV и XO, настройки автоответа",
        reply_markup=kb(
            [("Да, удалить", f"acc_del2:{phone}")],
            [("<- Отмена", f"acc_card:{phone}")],
        ),
    )


@router.callback_query(F.data.startswith("acc_del2:"))
async def cb_acc_del2(cb: CallbackQuery):
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    if not await _own(phone, uid):
        return await cb.answer("Аккаунт не найден.", show_alert=True)
    try:
        await ar_manager.stop(phone)
    except Exception:
        pass
    t = store.xo_liking_tasks.pop(phone, None)
    if t and not t.done():
        t.cancel()
    store.cancelled_phones.add(phone)
    await _client_pool.remove(phone)
    await db.db_delete_account(phone)
    base = os.path.join(config.SESSIONS_DIR, phone)
    for _sp in _glob.glob(base + "*.session*"):
        try:
            os.remove(_sp)
        except Exception:
            pass
    await cb.answer("Удалён.")
    await cb.message.edit_text(f"{phone} удалён.")


# =================================================================
# СМЕНА 2FA
# =================================================================

@router.callback_query(F.data.startswith("acc_2fa:"))
async def cb_acc_2fa(cb: CallbackQuery):
    """Смена/установка 2FA для одного аккаунта (из карточки)."""
    phone = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    if not await _own(phone, uid):
        return await cb.answer("Аккаунт не найден.", show_alert=True)
    await cb.answer()

    cli = await get_or_create_account_client(phone, uid)
    if not cli:
        return await cb.message.answer("❌ Не удалось подключиться к аккаунту.")

    try:
        pwd_info = await cli(GetPasswordRequest())
        has_pwd = pwd_info.has_password
    except Exception as e:
        return await cb.message.answer(f"❌ Ошибка получения статуса 2FA: {e}")

    status_text = "🔒 <b>Защищён</b> (2FA включена)" if has_pwd else "🔓 <b>Не защищён</b> (2FA отсутствует)"
    await cb.message.answer(
        f"🔑 <b>Смена 2FA</b>  <code>{phone}</code>\n"
        f"Статус: {status_text}"
    )

    cur_pwd = None
    if has_pwd:
        cur_pwd = await ask_with_cancel(
            bot, cb.message.chat.id, uid,
            f"Введите <b>текущий</b> пароль 2FA для <code>{phone}</code>:",
            parse_mode="HTML",
        )
        if cur_pwd is None:
            return await cb.message.answer("Отменено.")

    new_pwd = await ask_with_cancel(
        bot, cb.message.chat.id, uid,
        f"Введите <b>новый</b> пароль 2FA для <code>{phone}</code>:",
        parse_mode="HTML",
    )
    if not new_pwd or not new_pwd.strip():
        return await cb.message.answer("Отменено.")

    try:
        kwargs: Dict = {"new_password": new_pwd.strip()}
        if cur_pwd:
            kwargs["current_password"] = cur_pwd.strip()
        await cli.edit_2fa(**kwargs)
        action = "изменён" if has_pwd else "установлен"
        await cb.message.answer(f"✅ 2FA для <code>{phone}</code> успешно {action}.")
    except Exception as e:
        await cb.message.answer(f"❌ Ошибка: {e}")


@router.callback_query(F.data == "acc_2fa_bulk")
async def cb_acc_2fa_bulk(cb: CallbackQuery):
    """Массовая смена 2FA — выбор аккаунтов через пикер."""
    uid = cb.from_user.id
    if store.is_busy(uid):
        return await cb.answer("⏳ Завершите текущее действие.", show_alert=True)
    await cb.answer()
    await _send_target_picker(
        cb.message.chat.id, "fa2_t",
        "🔑 <b>Смена 2FA</b>\n\n"
        "Выберите аккаунты для установки / смены пароля\n"
        "двухфакторной аутентификации.\n\n"
        "⚠️ Для аккаунтов с уже установленной 2FA потребуется текущий пароль."
    )


@router.callback_query(F.data.startswith("fa2_t:"))
async def cb_fa2_t(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    mode = parts[1]
    if mode == "man":
        _man_sel_ctx[uid] = "fa2_t"
        _man_selection.pop(uid, None)
        await cb.answer()
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
        return
    targets = []
    if mode == "all":
        targets = await _resolve_targets_all(uid)
        await cb.answer()
    elif mode == "grp" and len(parts) == 2:
        await cb.answer()
        return await _send_groups_picker(uid, cb.message.chat.id, "fa2_t")
    elif mode == "gi":
        await cb.answer()
        targets = await _resolve_targets_group(uid, int(parts[2]))
    else:
        return await cb.answer("Bad", show_alert=True)
    if not targets:
        return await bot.send_message(uid, "❌ Аккаунтов нет.")
    await _fa2_after_targets(cb, uid, targets)


async def _fa2_after_targets(cb, uid: int, targets) -> None:
    """Запрашивает пароли и запускает массовую смену 2FA."""
    if not targets:
        return await bot.send_message(uid, "❌ Аккаунтов нет.")

    phones = [a["phone"] for a in targets]
    chat_id = cb.message.chat.id

    cur_raw = await ask_with_cancel(
        bot, chat_id, uid,
        f"🔑 <b>Смена 2FA</b>  ·  Целей: <b>{len(phones)}</b>\n\n"
        "Введите <b>текущий</b> пароль 2FA.\n"
        "Если 2FA не установлена — введите <code>нет</code>.\n"
        "(Аккаунты без 2FA пропустят этот шаг автоматически)",
        parse_mode="HTML",
    )
    if cur_raw is None:
        return await restore_main_menu(bot, chat_id, uid, "Отменено.")
    cur_pwd: Optional[str] = (
        None if cur_raw.strip().lower() in ("нет", "no", "none", "-", "")
        else cur_raw.strip()
    )

    new_pwd = await ask_with_cancel(
        bot, chat_id, uid,
        "🔑 Введите <b>новый</b> пароль 2FA\n"
        "(будет установлен на все выбранные аккаунты):",
        parse_mode="HTML",
    )
    if not new_pwd or not new_pwd.strip():
        return await restore_main_menu(bot, chat_id, uid, "Отменено.")
    new_pwd = new_pwd.strip()

    async def _runner():
        await _start_progress(bot, chat_id, uid, total=len(phones),
                               store=store, title="🔑 Смена 2FA")
        ok = 0
        for ph in phones:
            await _update_progress(bot, uid, store, current=ph)
            try:
                cli = await get_or_create_account_client(ph, uid)
                if not cli:
                    raise RuntimeError("connect failed")
                try:
                    pwd_info = await cli(GetPasswordRequest())
                    has_pwd = pwd_info.has_password
                except Exception:
                    has_pwd = False
                kwargs: Dict = {"new_password": new_pwd}
                if has_pwd and cur_pwd:
                    kwargs["current_password"] = cur_pwd
                await cli.edit_2fa(**kwargs)
                ok += 1
                await _update_progress(bot, uid, store, done_inc=1, current=None)
            except Exception as e:
                await _update_progress(bot, uid, store, done_inc=1,
                                       current=None, error=f"{ph}: {e}")
            await asyncio.sleep(random.uniform(3, 8))
        await _finish_progress(bot, uid, store,
                               summary_extra=f"2FA обновлена: {ok}/{len(phones)}")
        await restore_main_menu(bot, chat_id, uid)

    await task_queue.submit(_runner, owner_id=uid, notify=notify_owner,
                            title=f"Смена 2FA {len(phones)}")


# =================================================================
# ПРОКСИ
# =================================================================
@router.callback_query(F.data == "px_list")
async def cb_px_list(cb: CallbackQuery):
    uid = cb.from_user.id
    proxies = await db.db_proxy_get_by_owner(uid)
    alive = sum(1 for p in proxies if p.get("status") == "alive")
    dead  = sum(1 for p in proxies if p.get("status") == "dead")
    if not proxies:
        text = "Мои прокси\nПрокси не добавлены."
    else:
        text = f"Мои прокси\nВсего: {len(proxies)}  Живых: {alive}  Мёртвых: {dead}\n"
        for p in proxies[:20]:
            mark = "OK" if p.get("status") == "alive" else "X" if p.get("status") == "dead" else "?"
            note = f" - {p['note']}" if p.get("note") else ""
            text += f"\n{mark} #{p['id']}  {p['proxy_str']}{note}"
        if len(proxies) > 20:
            text += f"\n...ещё {len(proxies) - 20}"

    rows = []
    for p in proxies[:20]:
        mark = "OK" if p.get("status") == "alive" else "X" if p.get("status") == "dead" else "?"
        rows.append([(f"{mark} #{p['id']}", f"px_view:{p['id']}")])
    rows.append([("Добавить прокси", "px_add"), ("Проверить все", "px_checkall")])
    rows.append([("Назначить на аккаунты", "px_reassign")])
    rows.append([("Глобальные прокси", "gpx_list")])
    rows.append([home_btn()])
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))
    await cb.answer()


@router.callback_query(F.data == "px_add")
async def cb_px_add(cb: CallbackQuery):
    uid = cb.from_user.id
    await cb.answer()

    def _has_valid_proxy(text: str) -> bool:
        return any(parse_proxy_string(l.strip()) for l in text.splitlines() if l.strip())

    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "Пришлите прокси (host:port:user:pass). Можно несколько строк.",
        validator=_has_valid_proxy,
        error_msg="Не нашёл валидных прокси. Формат: host:port:user:pass",
    )
    if raw is None:
        return
    added = 0
    for line in raw.splitlines():
        line = line.strip()
        if not line or not parse_proxy_string(line):
            continue
        await db.db_proxy_add(uid, line, "")
        added += 1
    await cb.message.answer(f"Добавлено: {added}")


@router.callback_query(F.data == "px_checkall")
async def cb_px_checkall(cb: CallbackQuery):
    uid = cb.from_user.id
    proxies = await db.db_proxy_get_by_owner(uid)
    await cb.answer(f"Проверяю {len(proxies)} прокси...")
    _sem = asyncio.Semaphore(10)

    async def _check(p):
        async with _sem:
            ok = await check_proxy_connection(p["proxy_str"])
            await db.db_proxy_update_status(p["id"], "alive" if ok else "dead")
            return ok

    results = await asyncio.gather(*[_check(p) for p in proxies])
    alive = sum(results)
    await cb.message.answer(
        f"Проверка завершена\nПроверено: {len(proxies)}\nЖивых: {alive}\nМёртвых: {len(proxies) - alive}"
    )


@router.callback_query(F.data == "px_reassign")
async def cb_px_reassign(cb: CallbackQuery):
    uid = cb.from_user.id
    res = await reassign_phones(uid)
    await cb.answer()
    await cb.message.answer(
        f"Назначение прокси завершено\nОбновлено аккаунтов: {res['updated']}\nБез прокси осталось: {res['skipped']}"
    )


@router.callback_query(F.data.startswith("px_view:"))
async def cb_px_view(cb: CallbackQuery):
    pid = int(cb.data.split(":", 1)[1])
    p = await db.db_proxy_get_by_id(pid)
    if not p or p["owner_id"] != cb.from_user.id:
        return await cb.answer("Не найдено.", show_alert=True)
    mark = "OK" if p["status"] == "alive" else "X" if p["status"] == "dead" else "?"
    text = (
        f"Прокси #{pid}\n"
        f"Адрес: {p['proxy_str']}\n"
        f"Статус: {mark} {p['status']}\n"
        f"Заметка: {p.get('note') or '-'}"
    )
    await cb.message.edit_text(
        text,
        reply_markup=kb(
            [("Проверить", f"px_check:{pid}"), ("Заметка", f"px_note:{pid}")],
            [("Удалить прокси", f"px_del:{pid}")],
            [("< Назад", "px_list")],
        ),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("px_check:"))
async def cb_px_check(cb: CallbackQuery):
    pid = int(cb.data.split(":", 1)[1])
    p = await db.db_proxy_get_by_id(pid)
    if not p:
        return await cb.answer("Не найдено.", show_alert=True)
    ok = await check_proxy_connection(p["proxy_str"])
    await db.db_proxy_update_status(pid, "alive" if ok else "dead")
    await cb.answer("Жив" if ok else "Мёртв")
    await cb_px_view(cb)


@router.callback_query(F.data.startswith("px_note:"))
async def cb_px_note(cb: CallbackQuery):
    pid = int(cb.data.split(":", 1)[1])
    uid = cb.from_user.id
    await cb.answer()
    txt = await ask_with_cancel(bot, cb.message.chat.id, uid, f"Заметка для прокси #{pid}:")
    if txt is None:
        return
    await db.db_proxy_update_note(pid, txt.strip()[:128])
    await cb.message.answer("Заметка обновлена.")


@router.callback_query(F.data.startswith("px_del:"))
async def cb_px_del(cb: CallbackQuery):
    pid = int(cb.data.split(":", 1)[1])
    await db.db_proxy_delete(pid)
    await cb.answer("Удалён.")
    await cb_px_list(cb)


# =================================================================
# ГЛОБАЛЬНЫЕ ПРОКСИ
# =================================================================
def _gpx_render_row(g: Dict[str, Any], is_admin: bool) -> str:
    mark = "OK" if g.get("status") == "alive" else "X" if g.get("status") == "dead" else "?"
    body = g['proxy_str'] if is_admin else mask_proxy(g['proxy_str'])
    note = f" - {g['note']}" if g.get("note") else ""
    return f"{mark} #{g['id']} - {body}{note}"


@router.callback_query(F.data == "gpx_list")
async def cb_gpx_list(cb: CallbackQuery):
    uid = cb.from_user.id
    is_admin = await db.db_admins_check(uid)
    globs = await db.db_gproxy_get_all()
    alive = sum(1 for g in globs if g.get("status") == "alive")
    dead  = sum(1 for g in globs if g.get("status") == "dead")
    if not globs:
        text = "Глобальные прокси\nГлобальные прокси не добавлены."
    else:
        text = f"Глобальные прокси\nВсего: {len(globs)}  Живых: {alive}  Мёртвых: {dead}\n"
        for g in globs[:20]:
            text += "\n" + _gpx_render_row(g, is_admin)
        if len(globs) > 20:
            text += f"\n...ещё {len(globs) - 20}"
        if not is_admin:
            text += "\n\nТолько для чтения - управление у админов"

    rows = []
    for g in globs[:20]:
        mark = "OK" if g.get("status") == "alive" else "X" if g.get("status") == "dead" else "?"
        rows.append([(f"{mark} #{g['id']}", f"gpx_view:{g['id']}")])
    if is_admin:
        rows.append([("Добавить", "gpx_add"), ("Проверить все", "gpx_checkall")])
    rows.append([("< Назад", "px_list"), home_btn()])
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))
    await cb.answer()


@router.callback_query(F.data == "gpx_add")
async def cb_gpx_add(cb: CallbackQuery):
    uid = cb.from_user.id
    if not await db.db_admins_check(uid):
        return await cb.answer("Только админ.", show_alert=True)
    await cb.answer()

    def _has_valid_proxy_g(text: str) -> bool:
        return any(parse_proxy_string(l.strip()) for l in text.splitlines() if l.strip())

    raw = await ask_with_retry(
        bot, cb.message.chat.id, uid,
        "Пришлите глобальные прокси (host:port:user:pass). Дубликаты будут проигнорированы.",
        validator=_has_valid_proxy_g,
        error_msg="Не нашёл валидных прокси. Формат: host:port:user:pass",
    )
    if raw is None:
        return
    added = 0
    skipped_dup = 0
    skipped_bad = 0
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        if not parse_proxy_string(line):
            skipped_bad += 1
            continue
        new_id = await db.db_gproxy_add(line, "")
        if new_id is None:
            skipped_dup += 1
        else:
            added += 1
    text = f"Добавлено: {added}"
    if skipped_dup:
        text += f"\nДубликатов пропущено: {skipped_dup}"
    if skipped_bad:
        text += f"\nНевалидных строк: {skipped_bad}"
    await cb.message.answer(text)


@router.callback_query(F.data == "gpx_checkall")
async def cb_gpx_checkall(cb: CallbackQuery):
    uid = cb.from_user.id
    if not await db.db_admins_check(uid):
        return await cb.answer("Только админ.", show_alert=True)
    globs = await db.db_gproxy_get_all()
    await cb.answer(f"Проверяю {len(globs)} прокси...")
    _sem = asyncio.Semaphore(10)

    async def _check(g):
        async with _sem:
            ok = await check_proxy_connection(g["proxy_str"])
            await db.db_gproxy_update_status(g["id"], "alive" if ok else "dead")
            return ok

    results = await asyncio.gather(*[_check(g) for g in globs])
    alive = sum(results)
    await cb.message.answer(
        f"Проверка завершена\nПроверено: {len(globs)}\nЖивых: {alive}\nМёртвых: {len(globs) - alive}"
    )


@router.callback_query(F.data.startswith("gpx_view:"))
async def cb_gpx_view(cb: CallbackQuery):
    uid = cb.from_user.id
    is_admin = await db.db_admins_check(uid)
    pid = int(cb.data.split(":", 1)[1])
    g = await db.db_gproxy_get_by_id(pid)
    if not g:
        return await cb.answer("Не найдено.", show_alert=True)
    mark = "OK" if g["status"] == "alive" else "X" if g["status"] == "dead" else "?"
    body = g['proxy_str'] if is_admin else mask_proxy(g['proxy_str'])
    text = (
        f"Глобал #{pid}\n"
        f"Адрес: {body}\n"
        f"Статус: {mark} {g['status']}\n"
        f"Заметка: {g.get('note') or '-'}"
    )
    rows = []
    if is_admin:
        rows.append([("Проверить", f"gpx_check:{pid}"), ("Заметка", f"gpx_note:{pid}")])
        rows.append([("Удалить прокси", f"gpx_del:{pid}")])
    rows.append([("< Назад", "gpx_list")])
    try:
        await cb.message.edit_text(text, reply_markup=kb(*rows))
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb(*rows))
    await cb.answer()


@router.callback_query(F.data.startswith("gpx_check:"))
async def cb_gpx_check(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("Только админ.", show_alert=True)
    pid = int(cb.data.split(":", 1)[1])
    g = await db.db_gproxy_get_by_id(pid)
    if not g:
        return await cb.answer("Не найдено.", show_alert=True)
    ok = await check_proxy_connection(g["proxy_str"])
    await db.db_gproxy_update_status(pid, "alive" if ok else "dead")
    await cb.answer("Жив" if ok else "Мёртв")
    await cb_gpx_view(cb)


@router.callback_query(F.data.startswith("gpx_note:"))
async def cb_gpx_note(cb: CallbackQuery):
    uid = cb.from_user.id
    if not await db.db_admins_check(uid):
        return await cb.answer("Только админ.", show_alert=True)
    pid = int(cb.data.split(":", 1)[1])
    await cb.answer()
    txt = await ask_with_cancel(bot, cb.message.chat.id, uid, f"Заметка для глобал-прокси #{pid}:")
    if txt is None:
        return
    await db.db_gproxy_update_note(pid, txt.strip()[:128])
    await cb.message.answer("Заметка обновлена.")


@router.callback_query(F.data.startswith("gpx_del:"))
async def cb_gpx_del(cb: CallbackQuery):
    if not await db.db_admins_check(cb.from_user.id):
        return await cb.answer("Только админ.", show_alert=True)
    pid = int(cb.data.split(":", 1)[1])
    await db.db_gproxy_delete(pid)
    await cb.answer("Удалён.")
    await cb_gpx_list(cb)
