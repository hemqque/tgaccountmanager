# -*- coding: utf-8 -*-
"""
progress.py — Прогресс-бар для длинных операций (массовый залив,
регистрация партии аккаунтов и т. п.).

Хранит состояние в store.progress_msg[uid]:
    {
        "chat_id": int,
        "message_id": int,
        "total":  int,
        "done":   int,
        "errors": List[str],
        "current": Optional[str],   # phone, который сейчас в работе
        "title":   str,
        "pinned":  bool,
    }

API:
    _start_progress(bot, chat_id, uid, total, store, title="Прогресс")
    _update_progress(bot, uid, store, *,
                     done_inc=0, current=None, error=None)
    _finish_progress(bot, uid, store, summary_extra="")
"""

import asyncio
import logging
from typing import Optional

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest

log = logging.getLogger("progress")

# Символы прогресс-бара
WAIT_CH = "⬛"
ACTIVE_CH = "🔷"
OK_CH = "✅"
ERR_CH = "❌"

# ─────────────────────────────────────────────────────────────────
# Внутреннее: рендер сообщения
# ─────────────────────────────────────────────────────────────────
def _render(state: dict) -> str:
    total = max(1, state["total"])
    done = state["done"]
    n_ok = max(0, done - len(state["errors"]))
    n_err = len(state["errors"])

    # ширина бара = кол-во аккаунтов (по одной ячейке на каждый)
    width = total
    n_ok_cells = round(width * n_ok / total)
    n_err_cells = round(width * n_err / total)
    if n_ok_cells + n_err_cells > width:
        n_err_cells = max(0, width - n_ok_cells)
    n_done_cells = n_ok_cells + n_err_cells
    n_active = 1 if (n_done_cells < width and state.get("current")) else 0
    n_wait = max(0, width - n_done_cells - n_active)
    bar = (OK_CH * n_ok_cells
           + ERR_CH * n_err_cells
           + ACTIVE_CH * n_active
           + WAIT_CH * n_wait)

    title = state.get("title") or "Прогресс"
    pct = int(100 * done / total)

    lines = [
        f"⏳ <b>{title}</b>",
        f"{bar}  {done}/{total}  ({pct}%)",
    ]
    cur = state.get("current")
    if cur:
        lines.append(f"⚙️ {cur}")
    if state["errors"]:
        last_errs = state["errors"][-5:]
        lines.append("⚠️ Ошибки:")
        for e in last_errs:
            lines.append(f"  • {e}")
        if len(state["errors"]) > 5:
            lines.append(f"  …и ещё {len(state['errors']) - 5}")
    return "\n".join(lines)


async def _safe_edit(bot: Bot, chat_id: int, message_id: int, text: str):
    try:
        await bot.edit_message_text(
            text=text, chat_id=chat_id, message_id=message_id,
            parse_mode="HTML",
        )
    except TelegramBadRequest as e:
        # «message is not modified» — игнор
        if "not modified" in str(e).lower():
            return
        log.debug("edit progress: %s", e)
    except Exception as e:
        log.debug("edit progress error: %s", e)


# ─────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────
async def _start_progress(bot: Bot, chat_id: int, uid: int, total: int,
                          store, title: str = "Прогресс") -> None:
    text = (f"⏳ <b>{title}</b>\n"
            f"{WAIT_CH * total}  0/{total}  (0%)")
    msg = await bot.send_message(chat_id, text, parse_mode="HTML")
    pinned = False
    try:
        await bot.pin_chat_message(chat_id, msg.message_id,
                                   disable_notification=True)
        pinned = True
    except Exception:
        pass
    store.progress_msg[uid] = {
        "chat_id":   chat_id,
        "message_id": msg.message_id,
        "total":     int(total),
        "done":      0,
        "errors":    [],
        "current":   None,
        "title":     title,
        "pinned":    pinned,
    }


async def _update_progress(bot: Bot, uid: int, store, *,
                           done_inc: int = 0,
                           current: Optional[str] = None,
                           error: Optional[str] = None) -> None:
    state = store.progress_msg.get(uid)
    if not state:
        return
    if done_inc:
        state["done"] = min(state["total"], state["done"] + done_inc)
    if current is not None:
        state["current"] = current
    if error is not None:
        state["errors"].append(str(error))
    text = _render(state)
    await _safe_edit(bot, state["chat_id"], state["message_id"], text)


async def _finish_progress(bot: Bot, uid: int, store,
                           summary_extra: str = "") -> None:
    state = store.progress_msg.get(uid)
    if not state:
        return
    total = state["total"]
    n_err = len(state["errors"])
    n_ok = max(0, state["done"] - n_err)
    title = state.get("title") or "Прогресс"

    bar_full = OK_CH * BAR_WIDTH if n_err == 0 else (
        OK_CH * max(0, BAR_WIDTH - 1) + ERR_CH
    )
    text = (
        f"✅ <b>{title} завершён</b>\n"
        f"{bar_full}  {state['done']}/{total}\n"
        f"Успешно: <b>{n_ok}/{total}</b>"
        + (f"  | Ошибок: <b>{n_err}</b>" if n_err else "")
    )
    if summary_extra:
        text += f"\n{summary_extra}"
    if state["errors"]:
        text += "\n\n⚠️ Ошибки:\n" + "\n".join(
            f"  • {e}" for e in state["errors"][-10:]
        )
    await _safe_edit(bot, state["chat_id"], state["message_id"], text)

    # отложенно: открепить и удалить
    chat_id = state["chat_id"]
    msg_id = state["message_id"]
    pinned = state.get("pinned")

    async def _cleanup():
        await asyncio.sleep(60)
        try:
            if pinned:
                await bot.unpin_chat_message(chat_id, msg_id)
        except Exception:
            pass
        try:
            await bot.delete_message(chat_id, msg_id)
        except Exception:
            pass

    asyncio.create_task(_cleanup())
    store.progress_msg.pop(uid, None)
