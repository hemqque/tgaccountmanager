# -*- coding: utf-8 -*-
"""
reg_resume.py — Возобновляемая регистрация в @leomatchbot и @xo_xo.

Идея:
  Перед началом отправки шагов читаем последние 3 сообщения от бота, ищем
  «маркер шага» по ключевой фразе и стартуем с того шага, на котором
  застряли. После каждого шага сохраняем состояние в reg_state.

Шаги LDV (после detected_step выполняется action этого шага):
   0: язык             → отправить "🇷🇺 Русский"
   1: «уже миллионы»   → "👌 давай начнем"
   2: «в интернете»    → "👌 Ok"
   3: возраст          → age
   4: пол              → sex
   5: «кого показывать»→ target
   6: город            → city
   7: «как называть»   → name
   8: «расскажи о себе»→ "Пропустить"
   9: «пришли фото»    → отправить 1-3 фото + "Это все, сохранить фото"
  10: «нужен номер»    → send_contact
  11: «все верно»      → "Да"

Шаги XO:
   0: до старта        → "/start"
   1: пол              → нажать кнопку пола
   2: дата рождения    → birthday
   3: город            → city
   4: фото             → 1-3 фото
   5: «заполни анкету» → fill_btn
   6: имя              → name
   7: «расскажи»       → skip_btn
   8: «кого ты ищешь»  → target
   9: «что ты любишь»  → interests + done_btn
  10: подтверди номер  → send_contact
  11: подтверди участие→ agree_btn
"""

import asyncio
import random
from typing import Callable, Awaitable, Optional, Set, List, Dict, Any

from telethon import TelegramClient
from telethon.tl.types import InputPeerUser

import db
from config import LDV_BOT, XO_BOT


# =================================================================
# Детектор шага LDV
# =================================================================
LDV_STEP_KEYS = [
    # (фраза-маркер, номер шага)
    ("выбери язык", 0),
    ("choose your language", 0),
    ("уже миллионы", 1),
    ("помните что в интернете", 2),
    ("сколько тебе лет", 3),
    ("теперь определимся с полом", 4),
    ("кого показывать", 5),
    ("из какого ты города", 6),
    ("как мне тебя называть", 7),
    ("расскажи о себе", 8),
    ("пришли своё фото", 9),
    ("пришли свое фото", 9),
    ("фото добавлено", 9),
    ("мне нужен твой номер", 10),
    ("все верно", 11),
    ("так выглядит твоя анкета", 11),
]


async def _detect_ldv_step(client: TelegramClient) -> int:
    """
    Читает последние 3 сообщения @leomatchbot, возвращает номер шага.
    Если не удалось ничего найти — возвращает -1 (нужно «/start» сначала).
    """
    try:
        msgs = await client.get_messages(LDV_BOT, limit=3)
    except Exception:
        return -1
    detected = -1
    for m in msgs:
        text = (m.text or m.message or "").lower()
        for key, step in LDV_STEP_KEYS:
            if key in text:
                # берём НАИБОЛЬШИЙ найденный шаг
                if step > detected:
                    detected = step
    return detected


# =================================================================
# Детектор шага XO
# =================================================================
XO_STEP_KEYS_RU = [
    ("шаг 1 из 4", 1),
    ("шаг 2 из 4", 2),
    ("шаг 3 из 4", 3),
    ("шаг 4", 4),
    ("отправь несколько фото", 4),
    ("заполни анкету", 5),
    ("как мне тебя называть", 6),
    ("расскажи о себе", 7),
    ("кого ты ищешь", 8),
    ("что ты любишь", 9),
    ("подтверди свой номер", 10),
    ("подтвердите участие", 11),
]
XO_STEP_KEYS_EN = [
    ("step 1 of 4", 1),
    ("step 2 of 4", 2),
    ("step 3 of 4", 3),
    ("step 4", 4),
    ("send several photos", 4),
    ("complete profile", 5),
    ("what should i call", 6),
    ("tell us about", 7),
    ("who are you looking for", 8),
    ("what do you love", 9),
    ("confirm your phone", 10),
    ("confirm participation", 11),
]


async def _detect_xo_step(client: TelegramClient) -> (int, str):
    """
    Читает последние 3 сообщения от @xo_xo.
    Возвращает (step, lang) где lang in {'ru','en'}. Если ничего не нашли,
    возвращает (-1, 'ru').
    """
    try:
        msgs = await client.get_messages(XO_BOT, limit=3)
    except Exception:
        return -1, "ru"
    detected = -1
    lang = "ru"
    for m in msgs:
        text = (m.text or m.message or "").lower()
        for key, step in XO_STEP_KEYS_RU:
            if key in text and step > detected:
                detected = step
                lang = "ru"
        for key, step in XO_STEP_KEYS_EN:
            if key in text and step > detected:
                detected = step
                lang = "en"
    return detected, lang


# =================================================================
# Утилиты
# =================================================================
async def _safe_call(fn, *args, **kwargs):
    if fn is None:
        return None
    res = fn(*args, **kwargs)
    if hasattr(res, "__await__"):
        return await res
    return res


async def _rsleep(lo: float, hi: float) -> None:
    await asyncio.sleep(random.uniform(lo, hi))


# =================================================================
# LDV resumable
# =================================================================
async def register_ldv_resumable(
    client: TelegramClient,
    phone: str,
    data: Dict[str, Any],
    owner_id: int,
    notify_func: Optional[Callable[[str], Awaitable[None]]] = None,
    photos_request_func: Optional[Callable[[], Awaitable[List[str]]]] = None,
    cancel_set: Optional[Set[str]] = None,
) -> bool:
    """
    Возобновляемая регистрация в @leomatchbot.

    data ожидается со следующими полями:
      ages:  List[str|int],
      sex:   "Я девушка"|"Я парень",
      target:"Парни"|"Девушки",
      cities:List[str],
      names: List[str],
      photos:List[str]   — пути к файлам (могут быть пустыми, тогда вызовется
                           photos_request_func() для запроса)

    Возвращает True при успехе, False при ошибке/отмене.
    """
    bot = LDV_BOT

    def _is_cancel() -> bool:
        return bool(cancel_set and phone in cancel_set)

    ages   = data.get("ages") or data.get("age") or []
    if isinstance(ages, (str, int)):
        ages = [ages]
    sex    = data.get("sex") or "Я девушка"
    target = data.get("target") or "Парни"
    cities = data.get("cities") or data.get("city") or []
    if isinstance(cities, str):
        cities = [cities]
    names  = data.get("names") or data.get("name") or []
    if isinstance(names, str):
        names = [names]
    photos = list(data.get("photos") or [])

    age   = str(random.choice(ages))   if ages   else "20"
    city  = random.choice(cities)      if cities else "Москва"
    name  = random.choice(names)       if names  else "Аня"

    detected = await _detect_ldv_step(client)
    if detected < 0:
        # совсем чистая история — начинаем с /start
        try:
            await client.send_message(bot, "/start")
        except Exception as e:
            await _safe_call(notify_func, f"❌ {phone}: {e}")
            return False
        await _rsleep(3, 7)
        detected = await _detect_ldv_step(client)
        if detected < 0:
            detected = 0   # будем считать, что мы на шаге 0

    await _safe_call(notify_func, f"♻️ {phone}: возобновление с шага {detected}")

    async def _save(step: int):
        await db.db_save_reg_state(phone, bot, step, data, owner_id)

    try:
        # ── 0: язык ──
        if detected <= 0:
            if _is_cancel(): return False
            await client.send_message(bot, "🇷🇺 Русский");  await _save(0)
            await _rsleep(3, 7)
        # ── 1: «давай начнем» ──
        if detected <= 1:
            if _is_cancel(): return False
            await client.send_message(bot, "👌 давай начнем"); await _save(1)
            await _rsleep(3, 7)
        # ── 2: «Ok» ──
        if detected <= 2:
            if _is_cancel(): return False
            await client.send_message(bot, "👌 Ok"); await _save(2)
            await _rsleep(3, 7)
        # ── 3: возраст ──
        if detected <= 3:
            if _is_cancel(): return False
            await client.send_message(bot, age); await _save(3)
            await _rsleep(3, 7)
        # ── 4: пол ──
        if detected <= 4:
            if _is_cancel(): return False
            await client.send_message(bot, sex); await _save(4)
            await _rsleep(3, 7)
        # ── 5: target ──
        if detected <= 5:
            if _is_cancel(): return False
            await client.send_message(bot, target); await _save(5)
            await _rsleep(3, 7)
        # ── 6: город ──
        if detected <= 6:
            if _is_cancel(): return False
            await client.send_message(bot, city); await _save(6)
            await _rsleep(3, 7)
        # ── 7: имя ──
        if detected <= 7:
            if _is_cancel(): return False
            await client.send_message(bot, name); await _save(7)
            await _rsleep(3, 7)
        # ── 8: «Пропустить» ──
        if detected <= 8:
            if _is_cancel(): return False
            await client.send_message(bot, "Пропустить"); await _save(8)
            await _rsleep(3, 7)
        # ── 9: фото ──
        if detected <= 9:
            if _is_cancel(): return False
            if not photos and photos_request_func:
                photos = await photos_request_func() or []
                data["photos"] = photos
            if not photos:
                await _safe_call(notify_func,
                                 f"⚠️ {phone}: не присланы фото — отмена.")
                return False
            chosen = random.sample(photos, k=min(len(photos),
                                                 random.randint(1, 3)))
            for p in chosen:
                if _is_cancel(): return False
                try:
                    await client.send_file(bot, p)
                except Exception as e:
                    await _safe_call(notify_func,
                                     f"⚠️ {phone}: фото '{p}' не ушло: {e}")
                await _rsleep(3, 7)
            if _is_cancel(): return False
            await client.send_message(bot, "Это все, сохранить фото")
            await _save(9)
            await _rsleep(3, 7)
        # ── 10: контакт ──
        if detected <= 10:
            if _is_cancel(): return False
            try:
                await client.send_message(
                    bot,
                    file=__import__("telethon").tl.types.InputMediaContact(
                        phone_number=str(phone),
                        first_name=name,
                        last_name="",
                        vcard="",
                    )
                )
            except Exception:
                # запасной путь — стандартный Telethon API
                try:
                    await client.send_message(
                        bot,
                        f"📞 {phone}",
                    )
                except Exception:
                    pass
            await _save(10)
            await _rsleep(3, 7)
        # ── 11: подтверждение анкеты ──
        if detected <= 11:
            if _is_cancel(): return False
            await client.send_message(bot, "Да")
            await _save(11)
            await _rsleep(3, 7)
    except Exception as e:
        await _safe_call(notify_func, f"❌ {phone}: ошибка регистрации LDV: {e}")
        return False

    # успешно — стираем состояние
    await db.db_delete_reg_state(phone, bot)
    await _safe_call(notify_func, f"✅ {phone}: регистрация LDV завершена.")
    return True


# =================================================================
# XO resumable
# =================================================================
XO_BTNS = {
    "ru": dict(
        sex_girl="💁‍♀️ я девушка", sex_guy="🙋‍♂️ я парень",
        target_guys="Парней", target_girls="Девушек",
        skip_btn="Оставить пустым", fill_btn="Заполнить анкету",
        agree_btn="Соглашаюсь",
        interests=["Интим", "Любовь", "Флирт"],
        done_btn="Готово",
    ),
    "en": dict(
        sex_girl="I am a girl", sex_guy="I am a guy",
        target_guys="Guys", target_girls="Girls",
        skip_btn="Leave empty", fill_btn="Complete profile",
        agree_btn="I agree",
        interests=["Intimacy", "Love", "Flirting"],
        done_btn="Done",
    ),
}


async def register_xo_resumable(
    client: TelegramClient,
    phone: str,
    data: Dict[str, Any],
    owner_id: int,
    notify_func: Optional[Callable[[str], Awaitable[None]]] = None,
    photos_request_func: Optional[Callable[[], Awaitable[List[str]]]] = None,
    cancel_set: Optional[Set[str]] = None,
) -> bool:
    """
    Возобновляемая регистрация в @xo_xo. Использует функции из xo_functions
    (через ленивый импорт, чтобы избежать циклической зависимости).
    """
    from xo_functions import (
        _xo_click_button, _xo_listen, _xo_wait_for_button,
        _xo_wait_for_text, _xo_wait_button_or_text,
    )
    import logging
    _log = logging.getLogger("xo")

    bot = XO_BOT

    sex      = data.get("sex") or "💁‍♀️ я девушка"
    birthday = data.get("birthday") or "01.01.2003"
    city     = data.get("city") or "Москва"
    name     = data.get("name") or "Аня"
    photos   = list(data.get("photos") or [])

    def _is_cancel() -> bool:
        return bool(cancel_set and phone in cancel_set)

    detected, lang = await _detect_xo_step(client)
    btns = XO_BTNS[lang]
    is_girl = ("девушк" in sex.lower()) or ("girl" in sex.lower())

    if detected < 0:
        try:
            await client.send_message(bot, "/start")
        except Exception as e:
            await _safe_call(notify_func, f"❌ {phone}: {e}")
            return False
        await _rsleep(1, 2)
        detected, lang = await _detect_xo_step(client)
        btns = XO_BTNS[lang]
        if detected < 0:
            detected = 1

    await _safe_call(notify_func,
                     f"♻️ {phone}: XO возобновление с шага {detected} ({lang})")

    async def _save(step: int):
        data["lang"] = lang
        await db.db_save_reg_state(phone, bot, step, data, owner_id)

    try:
        # ── 1: пол ──
        if detected <= 1:
            if _is_cancel(): return False
            sex_btn = btns["sex_girl"] if is_girl else btns["sex_guy"]
            ok = await _xo_click_button(client, bot, sex_btn, timeout=30)
            if not ok:
                await client.send_message(bot, sex_btn)
            await _save(1); await _rsleep(4, 6)

        # ── 2: дата рождения ──
        if detected <= 2:
            if _is_cancel(): return False
            await _xo_listen(client, bot, timeout=8)
            await client.send_message(bot, birthday)
            await _save(2); await _rsleep(4, 6)

        # ── 3: город ──
        if detected <= 3:
            if _is_cancel(): return False
            await _xo_listen(client, bot, timeout=8)
            await client.send_message(bot, city)
            await _save(3); await _rsleep(4, 6)

        # ── 4: фото ──
        if detected <= 4:
            if _is_cancel(): return False
            await _xo_listen(client, bot, timeout=8)
            if not photos and photos_request_func:
                photos = await photos_request_func() or []
                data["photos"] = photos
            if not photos:
                await _safe_call(notify_func,
                                 f"⚠️ {phone}: нет фото — отмена.")
                return False
            chosen = random.sample(photos, k=min(len(photos),
                                                 random.randint(1, 3)))
            for p in chosen:
                if _is_cancel(): return False
                try:
                    await client.send_file(bot, p)
                except Exception as e:
                    await _safe_call(notify_func,
                                     f"⚠️ {phone}: фото '{p}': {e}")
                await _rsleep(2, 3)
            await _save(4)

        # ── 5: fill_btn ── ждём ИЛИ кнопку, ИЛИ переход xo_xo
        #    сразу к запросу имени (тогда шаг fill_btn пропускаем).
        name_keyword = "называть" if lang == "ru" else "what should"
        if detected <= 5:
            if _is_cancel(): return False
            evt = await _xo_wait_button_or_text(
                client, bot,
                btn_text=btns["fill_btn"],
                fallback_text=name_keyword,
                timeout=60,
            )
            if evt == "button":
                ok = await _xo_click_button(
                    client, bot, btns["fill_btn"], timeout=10,
                )
                if not ok:
                    _log.warning("xo resume %s: клик fill_btn не удался, "
                                 "иду дальше", phone)
            elif evt == "text":
                _log.info("xo resume %s: xo_xo сразу спросил имя — "
                          "шаг fill_btn пропущен", phone)
            else:
                _log.warning("xo resume %s: TIMEOUT fill_btn/имя — "
                             "иду дальше", phone)
            await _save(5); await _rsleep(2, 4)

        # ── 6: имя ── (ждём «как мне тебя называть» / «what should i call»)
        if detected <= 6:
            if _is_cancel(): return False
            got = await _xo_wait_for_text(client, bot, name_keyword,
                                          timeout=20)
            if not got:
                _log.warning(
                    "xo resume %s: не дождался запроса имени, шлю «%s» "
                    "вслепую", phone, name,
                )
            await client.send_message(bot, name)
            await _save(6); await _rsleep(4, 6)

        # ── 7: skip ──
        if detected <= 7:
            if _is_cancel(): return False
            ok = await _xo_click_button(client, bot, btns["skip_btn"],
                                        timeout=30)
            if not ok:
                await client.send_message(bot, f"➡️ {btns['skip_btn']}")
            await _save(7); await _rsleep(4, 6)

        # ── 8: target ──
        if detected <= 8:
            if _is_cancel(): return False
            target_btn = (btns["target_guys"] if is_girl
                          else btns["target_girls"])
            ok = await _xo_click_button(client, bot, target_btn, timeout=30)
            if not ok:
                await client.send_message(bot, target_btn)
            await _save(8); await _rsleep(4, 6)

        # ── 9: интересы + done ──
        if detected <= 9:
            if _is_cancel(): return False
            await _xo_listen(client, bot, timeout=10)
            for it in btns["interests"]:
                if _is_cancel(): return False
                await _xo_click_button(client, bot, it, timeout=10)
                await asyncio.sleep(3)
            await _xo_click_button(client, bot, btns["done_btn"], timeout=10)
            await _save(9); await _rsleep(4, 6)

        # ── 10: контакт ──
        if detected <= 10:
            if _is_cancel(): return False
            await _xo_listen(client, bot, timeout=10)
            try:
                from telethon.tl.functions.messages import SendMediaRequest
                from telethon.tl.types import InputMediaContact
                bot_entity = await client.get_input_entity(bot)
                await client(SendMediaRequest(
                    peer=bot_entity,
                    media=InputMediaContact(
                        phone_number=str(phone),
                        first_name=name, last_name="", vcard="",
                    ),
                    message="",
                    random_id=random.randint(1, 2**62),
                ))
            except Exception as e:
                await _safe_call(notify_func,
                                 f"⚠️ {phone}: send_contact: {e}")
            await _save(10); await _rsleep(4, 6)

        # ── 11: agree ──
        if detected <= 11:
            if _is_cancel(): return False
            ok = await _xo_click_button(client, bot, btns["agree_btn"],
                                        timeout=30)
            if not ok:
                await client.send_message(bot, btns["agree_btn"])
            await _save(11); await _rsleep(4, 6)

    except Exception as e:
        await _safe_call(notify_func, f"❌ {phone}: ошибка регистрации XO: {e}")
        return False

    await db.db_delete_reg_state(phone, bot)
    await _safe_call(notify_func, f"✅ {phone}: регистрация XO завершена.")
    return True
