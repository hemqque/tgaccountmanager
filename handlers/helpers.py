# -*- coding: utf-8 -*-
"""
handlers/helpers.py — Общие хелперы выбора целей для автоматизации.

  • _send_target_picker   — инлайн-меню «Все / Группа / Вручную»
  • _send_groups_picker   — список групп пользователя
  • _resolve_targets_*    — получение списка аккаунтов по способу выбора
"""

import re
from typing import Any, Dict, List

import db
from bot_globals import bot, _grp_index_cache, kb, home_btn
from utils import validate_phone, ask_with_retry


async def _send_target_picker(chat_id: int, prefix: str, title: str) -> None:
    """
    Показать инлайн-меню выбора целей: [Все] [Группа] [Вручную].
    prefix используется в callback_data: <prefix>:all / grp / man.
    """
    await bot.send_message(
        chat_id,
        title + "\n\n🎯 <b>Выберите цели:</b>",
        reply_markup=kb(
            [("📋 Все аккаунты", f"{prefix}:all")],
            [("📁 По группе", f"{prefix}:grp"),
             ("✏️ Вручную", f"{prefix}:man")],
            [home_btn()],
        ),
    )


async def _send_groups_picker(uid: int, chat_id: int, prefix: str) -> None:
    groups = await db.db_get_groups_by_owner(uid)
    if not groups:
        await bot.send_message(chat_id, "📁 У вас нет групп.")
        return
    _grp_index_cache[uid] = groups
    rows = []
    for i, g in enumerate(groups[:30]):
        rows.append([(f"📁 {g}", f"{prefix}:gi:{i}")])
    rows.append([("‹ Отмена", "action_cancel")])
    await bot.send_message(chat_id, "📁 Выберите группу:",
                           reply_markup=kb(*rows))


async def _resolve_targets_all(uid: int) -> List[Dict[str, Any]]:
    return await db.db_get_accounts_by_owner(uid)


async def _resolve_targets_group(uid: int, gi: int) -> List[Dict[str, Any]]:
    groups = _grp_index_cache.get(uid, [])
    if 0 <= gi < len(groups):
        return await db.db_get_accounts_by_group(uid, groups[gi])
    return []


async def _resolve_targets_manual(uid: int, chat_id: int
                                  ) -> List[Dict[str, Any]]:
    raw = await ask_with_retry(
        bot, chat_id, uid,
        "📱 Пришлите номера (через запятую или с новой строки):",
        validator=lambda t: any(
            validate_phone(tok) for tok in re.split(r"[,\n;\s]+", t) if tok
        ),
        error_msg="❌ Не нашёл валидных номеров. Формат: +79991112233",
    )
    if not raw:
        return []
    phones = [validate_phone(tok)
              for tok in re.split(r"[,\n;\s]+", raw) if tok]
    out = []
    for p in phones:
        if p:
            a = await db.db_get_account(p)
            if a and a.get("owner_id") == uid:
                out.append(a)
    return out
