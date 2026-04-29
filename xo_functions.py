# -*- coding: utf-8 -*-
"""
xo_functions.py — Регистрация и лайкинг в @xo_xo.

Содержит:
  • _xo_norm                — нормализация текста (без эмодзи/пунктуации).
  • _xo_get_last_msg        — последнее сообщение от @xo_xo.
  • _xo_listen              — polling новых сообщений по ID.
  • _xo_click_button        — нечёткий клик по кнопке (inline / reply).
  • _xo_wait_for_button     — ожидание появления кнопки.
  • register_one_xo         — однократная регистрация одного аккаунта.
  • xo_liking_task          — бесконечный цикл "❤️" с обработкой лимита.
  • xo_liking_scheduler     — фоновый планировщик задач из xo_tasks.
"""

import asyncio
import os
import random
import re
import time
import logging
from typing import Optional, Callable, Awaitable, Dict, Any, Set, List

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import (
    KeyboardButtonCallback,
    ReplyInlineMarkup,
    ReplyKeyboardMarkup,
)

import db
from config import (
    API_ID, API_HASH, SESSIONS_DIR, XO_BOT,
    XO_PREMIUM_PHRASE, XO_PAUSE_SECONDS,
    XO_LIKE_INTERVAL, XO_CHECK_INTERVAL, XO_CHECK_LAST_N,
)
from global_proxy import proxy_to_telethon, get_proxy_for_account
from reg_resume import XO_BTNS

log = logging.getLogger("xo")


# =================================================================
# Нормализация и парсинг
# =================================================================
_EMOJI_RE = re.compile(
    "[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F000-\U0001F02F"
    "\U0001F0A0-\U0001F0FF\U0001F100-\U0001F2FF‍️]+",
    flags=re.UNICODE,
)


def _xo_norm(s: str) -> str:
    """Lowercase + удаление эмодзи + удаление пунктуации/пробелов."""
    if not s:
        return ""
    s = s.lower()
    s = _EMOJI_RE.sub("", s)
    s = re.sub(r"[^\w\sа-яёa-z0-9]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# =================================================================
# Получение последнего сообщения и polling
# =================================================================
async def _xo_get_last_msg(client: TelegramClient, peer):
    try:
        msgs = await client.get_messages(peer, limit=1)
        return msgs[0] if msgs else None
    except Exception:
        return None


async def _xo_listen(client: TelegramClient, peer, timeout: float = 8.0,
                     cancel_set: Optional[Set[str]] = None,
                     phone: Optional[str] = None):
    """
    Ждём новое сообщение от peer (любого формата) до timeout сек.
    Возвращает Message либо None.
    """
    start = time.time()
    initial = await _xo_get_last_msg(client, peer)
    initial_id = initial.id if initial else 0

    while time.time() - start < timeout:
        if cancel_set and phone and phone in cancel_set:
            return None
        await asyncio.sleep(0.3)
        last = await _xo_get_last_msg(client, peer)
        if last and last.id > initial_id:
            return last
    return None


# =================================================================
# Поиск кнопки по тексту с нечётким сравнением
# =================================================================
def _btns_match(target_norm: str, btn_text: str) -> bool:
    """
    Совпадение target_norm (то, что МЫ задали) как ПОДСТРОКИ в тексте кнопки.
    Пример: target="Форель" → совпадёт с "Форель сырая 🐟".
    Только одно направление: задаваемый текст должен быть в тексте кнопки.
    Эта логика — только для действий ЮЗЕРБОТА на чужих ботах (XO/LDV/...),
    в UI бота-менеджера (aiogram callback_data) она не используется.
    """
    bn = _xo_norm(btn_text)
    if not bn or not target_norm:
        return False
    return target_norm in bn


def _iter_buttons(msg) -> List:
    """Возвращает плоский список кнопок (Telethon Button-объектов)."""
    res = []
    if not msg or not msg.buttons:
        return res
    for row in msg.buttons:
        for b in row:
            res.append(b)
    return res


async def _xo_collect_buttons(client: TelegramClient, peer, lookback: int = 5):
    """
    Собрать кнопки из последних `lookback` сообщений от peer.
    Возвращает список (msg, button) пар. Берёт только те сообщения,
    у которых msg.buttons is not None. Сортирует по убыванию id (свежее сначала).
    """
    out = []
    try:
        msgs = await client.get_messages(peer, limit=lookback)
    except Exception:
        return out
    for m in msgs:
        if not m or not getattr(m, "buttons", None):
            continue
        for row in m.buttons:
            for b in row:
                out.append((m, b))
    return out


async def _xo_click_button(client: TelegramClient, peer, target_text: str,
                           timeout: float = 30.0,
                           cancel_set: Optional[Set[str]] = None,
                           phone: Optional[str] = None,
                           lookback: int = 5) -> bool:
    """
    Найти кнопку, в тексте которой как ПОДСТРОКА содержится target_text,
    и кликнуть.

    Логика поиска (только для действий юзербота, не для UI бота-менеджера):
      • смотрим последние `lookback` сообщений от peer (не только самое
        последнее), потому что бот может прислать кнопки, а потом ещё
        текстовое сообщение «поверх»;
      • нормализуем оба текста: lowercase + удаление эмодзи + удаление
        пунктуации/пробелов;
      • совпадение по правилу `target_norm in btn_norm`
        (т.е. «Форель» сматчит «Форель сырая 🐟»).

    Поддерживает inline-кнопки (msg.click) и reply-keyboard (отправляет
    текст как сообщение). Возвращает True при успешном клике/отправке.
    """
    target_norm = _xo_norm(target_text)
    if not target_norm:
        log.warning("xo click: empty target_norm for %r", target_text)
        return False

    deadline = time.time() + timeout
    last_seen_btns: List[str] = []
    while time.time() < deadline:
        if cancel_set and phone and phone in cancel_set:
            return False

        pairs = await _xo_collect_buttons(client, peer, lookback=lookback)
        # запомним для финального лога — что вообще видели
        last_seen_btns = [
            (getattr(b, "text", "") or "") for _m, b in pairs
        ]
        for m, b in pairs:
            btext = getattr(b, "text", "") or ""
            if not _btns_match(target_norm, btext):
                continue

            # 1) пробуем inline-click
            try:
                await b.click()
                log.info("xo click OK %r (matched %r) by .click()",
                         target_text, btext)
                return True
            except Exception as e1:
                log.debug("xo b.click() failed for %r: %s", btext, e1)
                # 2) фоллбэк — отправляем текст кнопки в чат
                try:
                    await client.send_message(peer, btext)
                    log.info("xo click OK %r (matched %r) by send_message",
                             target_text, btext)
                    return True
                except Exception as e2:
                    log.warning("xo send_message fallback failed: %s", e2)
                    # не возвращаем False — попробуем следующую кнопку

        await asyncio.sleep(0.5)

    log.warning(
        "xo click TIMEOUT for %r (target_norm=%r). Seen buttons in last %d "
        "messages: %r",
        target_text, target_norm, lookback, last_seen_btns,
    )
    return False


async def _xo_wait_for_button(client: TelegramClient, peer, target_text: str,
                              timeout: float = 30.0,
                              lookback: int = 5) -> bool:
    """Ждать появления кнопки с подстрокой target_text (без клика)."""
    target_norm = _xo_norm(target_text)
    if not target_norm:
        return False
    deadline = time.time() + timeout
    while time.time() < deadline:
        pairs = await _xo_collect_buttons(client, peer, lookback=lookback)
        for _m, b in pairs:
            if _btns_match(target_norm, getattr(b, "text", "") or ""):
                return True
        await asyncio.sleep(0.5)
    return False


async def _xo_wait_for_text(client: TelegramClient, peer, needle: str,
                            timeout: float = 15.0, lookback: int = 3) -> bool:
    """
    Ждать, пока в последних `lookback` сообщениях от peer не появится
    сообщение, содержащее подстроку `needle` (после _xo_norm-нормализации).
    Возвращает True если нашли, False по таймауту.
    """
    needle_n = _xo_norm(needle)
    if not needle_n:
        return False
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            msgs = await client.get_messages(peer, limit=lookback)
        except Exception:
            msgs = []
        for m in msgs:
            if not m:
                continue
            text = (getattr(m, "text", None) or
                    getattr(m, "message", None) or "")
            if needle_n in _xo_norm(text):
                return True
        await asyncio.sleep(0.4)
    return False


async def _xo_wait_button_or_text(client: TelegramClient, peer,
                                  btn_text: str, fallback_text: str,
                                  timeout: float = 60.0,
                                  lookback: int = 5) -> str:
    """
    Параллельно ждёт ОДНО из двух событий:
      • кнопка с подстрокой `btn_text` (приоритет, проверяется первой);
      • сообщение с подстрокой `fallback_text`.

    Возвращает:
      "button" — нашли кнопку (можно кликать),
      "text"   — нашли текст (xo_xo уже перешёл вперёд, кнопку пропускаем),
      ""       — таймаут, ничего не пришло.
    """
    btn_norm = _xo_norm(btn_text)
    txt_norm = _xo_norm(fallback_text)
    deadline = time.time() + timeout
    while time.time() < deadline:
        # 1) сначала ищем кнопку
        if btn_norm:
            pairs = await _xo_collect_buttons(client, peer, lookback=lookback)
            for _m, b in pairs:
                if _btns_match(btn_norm, getattr(b, "text", "") or ""):
                    return "button"
        # 2) текст
        if txt_norm:
            try:
                msgs = await client.get_messages(peer, limit=lookback)
            except Exception:
                msgs = []
            for m in msgs:
                if not m:
                    continue
                text = (getattr(m, "text", None) or
                        getattr(m, "message", None) or "")
                if txt_norm in _xo_norm(text):
                    return "text"
        await asyncio.sleep(0.4)
    return ""


# =================================================================
# Регистрация одного аккаунта в XO (полная — для свежей регистрации)
# =================================================================
async def register_one_xo(client: TelegramClient, phone: str,
                          data: Dict[str, Any],
                          notify_func: Optional[Callable[[str],
                                                         Awaitable[None]]] = None,
                          cancel_set: Optional[Set[str]] = None,
                          owner_id: Optional[int] = None) -> bool:
    """
    Свежая регистрация в @xo_xo. data — как в register_xo_resumable.
    Возвращает True/False.
    """
    bot = XO_BOT
    sex      = data.get("sex") or "💁‍♀️ я девушка"
    birthday = data.get("birthday") or "01.01.2003"
    city     = data.get("city") or "Москва"
    name     = data.get("name") or "Аня"
    photos   = list(data.get("photos") or [])

    is_girl = ("девушк" in sex.lower()) or ("girl" in sex.lower())

    def _is_cancel() -> bool:
        return bool(cancel_set and phone in cancel_set)

    async def _save_state(step: int):
        if owner_id is not None:
            await db.db_save_reg_state(phone, bot, step, data, owner_id)

    try:
        # 1. /start → определить язык по первому ответу
        await client.send_message(bot, "/start")
        await asyncio.sleep(random.uniform(1, 2))

        first = await _xo_get_last_msg(client, bot)
        text0 = ((first.text if first else "") or "").lower()
        if any(k in text0 for k in
               ("step 1", "gender", "what is your", "i am a girl",
                "i am a guy")):
            lang = "en"
        else:
            lang = "ru"
        btns = XO_BTNS[lang]
        data["lang"] = lang

        # 2. кнопка пола
        if _is_cancel(): return False
        sex_btn = btns["sex_girl"] if is_girl else btns["sex_guy"]
        ok = await _xo_click_button(client, bot, sex_btn, timeout=30,
                                    cancel_set=cancel_set, phone=phone)
        if not ok:
            await client.send_message(bot, sex_btn)
        await _save_state(1)
        await asyncio.sleep(random.uniform(4, 6))

        # 3. дата рождения
        if _is_cancel(): return False
        await _xo_listen(client, bot, timeout=8, cancel_set=cancel_set,
                         phone=phone)
        await client.send_message(bot, birthday)
        await _save_state(2)
        await asyncio.sleep(random.uniform(4, 6))

        # 4. город
        if _is_cancel(): return False
        await _xo_listen(client, bot, timeout=8, cancel_set=cancel_set,
                         phone=phone)
        await client.send_message(bot, city)
        await _save_state(3)
        await asyncio.sleep(random.uniform(4, 6))

        # 5. фото 1-3
        if _is_cancel(): return False
        await _xo_listen(client, bot, timeout=8, cancel_set=cancel_set,
                         phone=phone)
        if not photos:
            if notify_func:
                await notify_func(f"⚠️ {phone}: нет фото для XO — отмена.")
            return False
        chosen = random.sample(photos, k=min(len(photos),
                                             random.randint(1, 3)))
        for p in chosen:
            if _is_cancel(): return False
            try:
                await client.send_file(bot, p)
            except Exception as e:
                if notify_func:
                    await notify_func(f"⚠️ {phone}: фото '{p}': {e}")
            await asyncio.sleep(random.uniform(2, 3))
        await _save_state(4)

        # 6. fill_btn — ждём ИЛИ кнопку «Заполнить анкету», ИЛИ переход
        #    xo_xo сразу к запросу имени (если шаг fill_btn пропущен ботом).
        if _is_cancel(): return False
        name_keyword = "называть" if lang == "ru" else "what should"
        evt = await _xo_wait_button_or_text(
            client, bot,
            btn_text=btns["fill_btn"],
            fallback_text=name_keyword,
            timeout=60,
        )
        if evt == "button":
            ok = await _xo_click_button(
                client, bot, btns["fill_btn"], timeout=10,
                cancel_set=cancel_set, phone=phone,
            )
            if not ok:
                # если клик/фоллбэк не сработали — НЕ шлём текст вслепую
                # (это и есть тот баг, когда «Заполнить анкету» уходит
                # на «Как мне тебя называть?»). Молча идём дальше.
                log.warning("xo %s: клик fill_btn не удался, иду дальше",
                            phone)
        elif evt == "text":
            log.info("xo %s: xo_xo сразу спросил имя — шаг fill_btn пропущен",
                     phone)
        else:
            log.warning("xo %s: TIMEOUT на fill_btn/имя — иду дальше", phone)
        await _save_state(5)
        await asyncio.sleep(random.uniform(2, 4))

        # 7. имя — ждём «как мне тебя называть» / «what should i call»
        if _is_cancel(): return False
        got = await _xo_wait_for_text(client, bot, name_keyword, timeout=20)
        if not got:
            log.warning(
                "xo register %s: не дождался запроса имени, шлю имя «%s» "
                "вслепую", phone, name,
            )
        await client.send_message(bot, name)
        await _save_state(6)
        await asyncio.sleep(random.uniform(4, 6))

        # 8. skip
        if _is_cancel(): return False
        ok = await _xo_click_button(client, bot, btns["skip_btn"], timeout=30,
                                    cancel_set=cancel_set, phone=phone)
        if not ok:
            await client.send_message(bot, f"➡️ {btns['skip_btn']}")
        await _save_state(7)
        await asyncio.sleep(random.uniform(4, 6))

        # 9. target
        if _is_cancel(): return False
        target_btn = btns["target_guys"] if is_girl else btns["target_girls"]
        ok = await _xo_click_button(client, bot, target_btn, timeout=30,
                                    cancel_set=cancel_set, phone=phone)
        if not ok:
            await client.send_message(bot, target_btn)
        await _save_state(8)
        await asyncio.sleep(random.uniform(4, 6))

        # 10. интересы + done
        if _is_cancel(): return False
        await _xo_listen(client, bot, timeout=10, cancel_set=cancel_set,
                         phone=phone)
        for it in btns["interests"]:
            if _is_cancel(): return False
            await _xo_click_button(client, bot, it, timeout=10,
                                   cancel_set=cancel_set, phone=phone)
            await asyncio.sleep(3)
        await _xo_click_button(client, bot, btns["done_btn"], timeout=10)
        await _save_state(9)
        await asyncio.sleep(random.uniform(4, 6))

        # 11. контакт
        if _is_cancel(): return False
        await _xo_listen(client, bot, timeout=10, cancel_set=cancel_set,
                         phone=phone)
        try:
            from telethon.tl.functions.messages import SendMediaRequest
            from telethon.tl.types import InputMediaContact
            bot_entity = await client.get_input_entity(bot)
            me = await client.get_me()
            await client(SendMediaRequest(
                peer=bot_entity,
                media=InputMediaContact(
                    phone_number=str(phone),
                    first_name=name, last_name="", vcard="",
                    user_id=me.id,
                ),
                message="",
                random_id=random.randint(1, 2**62),
            ))
        except Exception as e:
            if notify_func:
                await notify_func(f"⚠️ {phone}: send_contact: {e}")
        await _save_state(10)
        await asyncio.sleep(random.uniform(4, 6))

        # 12. agree
        if _is_cancel(): return False
        ok = await _xo_click_button(client, bot, btns["agree_btn"], timeout=30,
                                    cancel_set=cancel_set, phone=phone)
        if not ok:
            await client.send_message(bot, btns["agree_btn"])
        await _save_state(11)
        await asyncio.sleep(random.uniform(2, 4))

    except Exception as e:
        if notify_func:
            await notify_func(f"❌ {phone}: ошибка XO: {e}")
        return False

    if owner_id is not None:
        await db.db_delete_reg_state(phone, bot)
    return True


# =================================================================
# Лайкинг XO — бесконечный цикл
# =================================================================
async def xo_liking_task(phone: str, owner_id: int,
                         proxy: Optional[str],
                         store,
                         notify_func: Optional[Callable[[int, str],
                                                        Awaitable[None]]] = None
                         ) -> None:
    """
    Бесконечный фоновый цикл «❤️» в @xo_xo.
    Останавливается, если phone исчезает из store.xo_liking_tasks.
    Уважает store.xo_liking_paused (set номеров).
    """
    bot = XO_BOT
    session_path = os.path.join(SESSIONS_DIR, phone)

    while phone in store.xo_liking_tasks:
        client = None
        try:
            tproxy = proxy_to_telethon(proxy or "")
            client = TelegramClient(session_path, API_ID, API_HASH,
                                    proxy=tproxy)
            await client.start()
            last_check = 0.0

            while phone in store.xo_liking_tasks:
                # Пауза по запросу пользователя
                if phone in store.xo_liking_paused:
                    await asyncio.sleep(5)
                    continue

                # Отправляем "❤️"
                try:
                    await client.send_message(bot, "❤️")
                except FloodWaitError as e:
                    log.warning("xo flood %s: wait %s", phone, e.seconds)
                    await asyncio.sleep(min(e.seconds + 5, 600))
                except Exception as e:
                    log.warning("xo like send %s: %s", phone, e)

                # Раз в XO_CHECK_INTERVAL секунд проверяем лимит
                now = time.time()
                if now - last_check >= XO_CHECK_INTERVAL:
                    last_check = now
                    try:
                        msgs = await client.get_messages(
                            bot, limit=XO_CHECK_LAST_N
                        )
                        joined = " | ".join(
                            (m.text or m.message or "").lower()
                            for m in msgs if m
                        )
                        if XO_PREMIUM_PHRASE.lower() in joined:
                            # Лимит исчерпан — пауза
                            until_ts = time.time() + XO_PAUSE_SECONDS
                            until_str = time.strftime(
                                "%d.%m %H:%M", time.localtime(until_ts)
                            )
                            await db.db_update_xo_task(
                                phone, status="paused", next_run=until_ts
                            )
                            store.xo_liking_paused.add(phone)
                            if notify_func:
                                try:
                                    await notify_func(
                                        owner_id,
                                        f"🛑 {phone}: XO лимит исчерпан. "
                                        f"Пауза до {until_str}.",
                                    )
                                except Exception:
                                    pass
                            # ждём с проверкой отмены каждые 60с
                            wait_until = time.time() + XO_PAUSE_SECONDS
                            while time.time() < wait_until:
                                if phone not in store.xo_liking_tasks:
                                    break
                                await asyncio.sleep(60)
                            store.xo_liking_paused.discard(phone)
                            await db.db_update_xo_task(
                                phone, status="pending",
                                next_run=time.time(),
                            )
                            continue
                    except Exception as e:
                        log.warning("xo limit-check %s: %s", phone, e)

                await asyncio.sleep(XO_LIKE_INTERVAL)

        except Exception as e:
            log.warning("xo_liking_task %s outer error: %s", phone, e)
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass

        if phone in store.xo_liking_tasks:
            await asyncio.sleep(30)   # подождать перед переподключением

    log.info("xo_liking_task %s stopped", phone)


# =================================================================
# Планировщик XO-лайкинга
# =================================================================
async def xo_liking_scheduler(store,
                              notify_func: Optional[
                                  Callable[[int, str], Awaitable[None]]
                              ] = None) -> None:
    """
    Каждые 10с забирает все pending xo_tasks с next_run<=now и запускает
    xo_liking_task как asyncio.Task. Идемпотентно — если задача уже бежит,
    повторно не стартуем.
    """
    while True:
        try:
            tasks = await db.db_get_pending_xo_tasks()
            for t in tasks:
                phone = t["phone"]
                owner_id = t["owner_id"]
                if phone in store.xo_liking_tasks:
                    continue
                proxy = await get_proxy_for_account(phone, owner_id)
                store.xo_liking_tasks[phone] = asyncio.create_task(
                    xo_liking_task(phone, owner_id, proxy, store,
                                   notify_func=notify_func)
                )
                await db.db_update_xo_task(phone, status="running")
                if notify_func:
                    try:
                        await notify_func(owner_id,
                                          f"▶️ XO лайкинг запущен: {phone}")
                    except Exception:
                        pass
        except Exception as e:
            log.warning("xo_scheduler error: %s", e)

        await asyncio.sleep(10)
