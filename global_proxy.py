# -*- coding: utf-8 -*-
"""
global_proxy.py — Управление пулом SOCKS5-прокси.

Возможности:
  • parse_proxy_string  — распарсить строку host:port:user:pass.
  • get_proxy_for_account — выбрать прокси для конкретного аккаунта
                            (приоритет: уже сохранённый в accounts.proxy,
                             иначе — рандомный alive-прокси владельца).
  • check_proxy_connection — TCP-проверка живости через python-socks.
  • run_health_check_loop — фоновая корутина, периодически проверяющая
                            всё, что есть в user_proxies, и обновляющая
                            status('alive'/'dead') в БД.
  • reassign_phones     — рандомно раздать живые прокси владельца его
                            аккаунтам (обновляет accounts.proxy).
  • count_alive_socks5  — счётчик живых прокси у владельца.
"""

import asyncio
import random
import time
import logging
from typing import Optional, Tuple, List, Dict, Any, Callable, Awaitable

from python_socks.async_.asyncio import Proxy
from python_socks import ProxyType

import db
from config import (
    PROXY_HEALTH_CHECK_INTERVAL,
    PROXY_CHECK_TIMEOUT,
)

log = logging.getLogger("global_proxy")


# Callback, который main.py устанавливает для уведомлений админов о
# смерти глобал-прокси. Тип: async fn(text: str) -> None.
_admin_notifier: Optional[Callable[[str], Awaitable[None]]] = None


def set_admin_notifier(fn: Optional[Callable[[str], Awaitable[None]]]) -> None:
    """Регистрирует callback для уведомлений админов из health-check."""
    global _admin_notifier
    _admin_notifier = fn


# ─────────────────────────────────────────────────────────────────
# Парсинг прокси-строки
# ─────────────────────────────────────────────────────────────────
def parse_proxy_string(s: str) -> Optional[Tuple[str, int, str, str]]:
    """
    Принимает строку формата 'host:port:user:pass' (user/pass опциональны).
    Возвращает (host, port, user, pass) либо None, если не парсится.
    Если строка пустая/равна 'нет'/'no'/'-' — возвращает None.
    """
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    if s.lower() in ("нет", "no", "none", "-", "без прокси"):
        return None
    parts = s.split(":")
    if len(parts) < 2:
        return None
    host = parts[0].strip()
    try:
        port = int(parts[1].strip())
    except ValueError:
        return None
    user = parts[2].strip() if len(parts) >= 3 else ""
    password = parts[3].strip() if len(parts) >= 4 else ""
    if not host or not (1 <= port <= 65535):
        return None
    return host, port, user, password


def proxy_to_telethon(proxy_str: str):
    """
    Преобразует строку в кортеж формата, который ждёт Telethon:
      ('socks5', host, port, rdns=True, user, pass)
    либо None, если строка пустая/некорректная.
    """
    parsed = parse_proxy_string(proxy_str or "")
    if not parsed:
        return None
    host, port, user, password = parsed
    return ("socks5", host, port, True, user or None, password or None)


# ─────────────────────────────────────────────────────────────────
# Sticky-выбор глобал-прокси
# ─────────────────────────────────────────────────────────────────
def _sticky_index(phone: str, n: int) -> int:
    """
    Детерминированный индекс из [0, n) по phone — чтобы один и тот же
    телефон всегда попадал на один и тот же глобал (пока их количество
    не меняется).
    """
    if n <= 0:
        return 0
    h = 0
    for ch in phone:
        h = (h * 131 + ord(ch)) & 0xFFFFFFFF
    return h % n


async def get_sticky_global_proxy(phone: str) -> Optional[Dict[str, Any]]:
    """
    Возвращает sticky-выбранный (по hash(phone)) живой глобальный прокси,
    либо None если живых нет.
    """
    alive = await db.db_gproxy_get_alive()
    if not alive:
        return None
    # Стабильный порядок (по id) — чтобы sticky не «прыгал» при разных
    # перестановках строк
    alive_sorted = sorted(alive, key=lambda r: r["id"])
    idx = _sticky_index(phone, len(alive_sorted))
    return alive_sorted[idx]


# ─────────────────────────────────────────────────────────────────
# Маскировка прокси (для не-админов в read-only списке)
# ─────────────────────────────────────────────────────────────────
def mask_proxy(proxy_str: str) -> str:
    """
    Возвращает 'host:****:****:****' (всё кроме host скрыто).
    Для строк без user/pass — то же самое: 'host:****:****:****'.
    Если строка не парсится — возвращается прочерк.
    """
    parsed = parse_proxy_string(proxy_str or "")
    if not parsed:
        return "—"
    host, _port, _user, _pwd = parsed
    return f"{host}:****:****:****"


def proxy_host(proxy_str: str) -> str:
    """Возвращает только host (без порта/учёток); '' если не парсится."""
    parsed = parse_proxy_string(proxy_str or "")
    if not parsed:
        return ""
    return parsed[0]


# ─────────────────────────────────────────────────────────────────
# Получить прокси для аккаунта
# ─────────────────────────────────────────────────────────────────
async def get_proxy_for_account(phone: str, owner_id: int) -> Optional[str]:
    """
    Возвращает строку прокси для конкретного аккаунта в порядке приоритета:
      1) accounts.proxy — личный прокси аккаунта (если задан и валидный);
      2) случайный alive-прокси из user_proxies владельца;
      3) sticky-глобал из global_proxies (если есть живые);  ← NEW
      4) None — без прокси.
    """
    acc = await db.db_get_account(phone)
    if acc:
        existing = (acc.get("proxy") or "").strip()
        if existing and parse_proxy_string(existing):
            return existing
    alive = await db.db_proxy_get_alive(owner_id)
    if alive:
        return random.choice(alive)["proxy_str"]
    # фоллбэк на глобальный пул (sticky)
    g = await get_sticky_global_proxy(phone)
    if g:
        return g["proxy_str"]
    return None


# ─────────────────────────────────────────────────────────────────
# Применить глобал к моим без-прокси аккаунтам (ручная кнопка)
# ─────────────────────────────────────────────────────────────────
async def apply_global_to_unproxied(owner_id: int,
                                    recheck: bool = True
                                    ) -> Dict[str, int]:
    """
    Записывает в accounts.proxy случайный (с повторением) глобал-прокси
    для всех аккаунтов владельца, у которых сейчас accounts.proxy="".

    Перед применением (если recheck=True) делает health-check всех
    глобалов и берёт ТОЛЬКО живые.

    Возвращает: {'updated': N, 'skipped': M, 'alive_globals': K}.
    """
    globs_all = await db.db_gproxy_get_all()
    if not globs_all:
        return {"updated": 0, "skipped": 0, "alive_globals": 0}

    if recheck:
        # параллельно проверяем все
        async def _one(row):
            ok = await check_proxy_connection(row.get("proxy_str") or "")
            await db.db_gproxy_update_status(
                row["id"], "alive" if ok else "dead"
            )
        await asyncio.gather(*[_one(g) for g in globs_all],
                             return_exceptions=True)

    alive = await db.db_gproxy_get_alive()
    if not alive:
        # ничего живого — ничего не делаем
        accs = await db.db_get_accounts_by_owner(owner_id)
        empties = [a for a in accs if not (a.get("proxy") or "").strip()]
        return {"updated": 0, "skipped": len(empties), "alive_globals": 0}

    accs = await db.db_get_accounts_by_owner(owner_id)
    empties = [a for a in accs if not (a.get("proxy") or "").strip()]
    pool = [g["proxy_str"] for g in alive]
    updated = 0
    for acc in empties:
        chosen = random.choice(pool)   # с повторением — Q13 (C)
        await db.db_update_account_field(acc["phone"], "proxy", chosen)
        updated += 1
    return {"updated": updated, "skipped": 0, "alive_globals": len(alive)}


# ─────────────────────────────────────────────────────────────────
# Проверка соединения через SOCKS5
# ─────────────────────────────────────────────────────────────────
async def check_proxy_connection(proxy_str: str,
                                 test_host: str = "api.telegram.org",
                                 test_port: int = 443,
                                 timeout: float = PROXY_CHECK_TIMEOUT) -> bool:
    """
    Пытается пробить SOCKS5-прокси, открыв TCP-соединение до test_host:test_port.
    Возвращает True, если соединение установлено за < timeout, иначе False.
    """
    parsed = parse_proxy_string(proxy_str)
    if not parsed:
        return False
    host, port, user, password = parsed
    try:
        proxy = Proxy.create(
            proxy_type=ProxyType.SOCKS5,
            host=host,
            port=port,
            username=user or None,
            password=password or None,
            rdns=True,
        )
        sock = await asyncio.wait_for(
            proxy.connect(dest_host=test_host, dest_port=test_port),
            timeout=timeout,
        )
        try:
            sock.close()
        except Exception:
            pass
        return True
    except Exception as e:
        log.debug("proxy %s:%s dead: %s", host, port, e)
        return False


# ─────────────────────────────────────────────────────────────────
# Фоновая проверка живости всех прокси
# ─────────────────────────────────────────────────────────────────
async def run_health_check_loop(stop_event: Optional[asyncio.Event] = None,
                                interval: int = PROXY_HEALTH_CHECK_INTERVAL):
    """
    Запускается один раз в main и работает до завершения процесса
    (или пока stop_event не set). Каждые `interval` секунд:
      • достаёт все прокси из user_proxies И global_proxies,
      • параллельно проверяет каждую,
      • обновляет статус 'alive'/'dead' в БД.
      • при alive→dead для глобал-прокси шлёт уведомление админам
        через зарегистрированный _admin_notifier.
    """
    while True:
        if stop_event is not None and stop_event.is_set():
            break
        try:
            # ── Пользовательские прокси ──
            import aiosqlite
            from config import DB_NAME
            async with aiosqlite.connect(DB_NAME) as conn:
                conn.row_factory = aiosqlite.Row
                cur = await conn.execute("SELECT * FROM user_proxies")
                user_rows = [dict(r) for r in await cur.fetchall()]

            async def _one_user(row):
                ok = await check_proxy_connection(row.get("proxy_str") or "")
                await db.db_proxy_update_status(
                    row["id"], "alive" if ok else "dead"
                )

            if user_rows:
                await asyncio.gather(*[_one_user(r) for r in user_rows],
                                     return_exceptions=True)

            # ── Глобальные прокси (с уведомлением админов о смерти) ──
            globs_before = await db.db_gproxy_get_all()
            died_notes: List[str] = []  # будут отправлены батчем

            async def _one_global(row):
                prev = (row.get("status") or "unknown").lower()
                ok = await check_proxy_connection(row.get("proxy_str") or "")
                new_status = "alive" if ok else "dead"
                await db.db_gproxy_update_status(row["id"], new_status)
                # alive→dead — уведомляем (Q14 A)
                if prev == "alive" and new_status == "dead":
                    parsed = parse_proxy_string(row.get("proxy_str") or "")
                    host_port = (
                        f"{parsed[0]}:{parsed[1]}" if parsed else
                        (row.get("proxy_str") or "—")
                    )
                    died_notes.append(
                        f"❌ Глобал #{row['id']} умер: <code>{host_port}</code>"
                        + (f" — {row['note']}" if row.get("note") else "")
                    )

            if globs_before:
                await asyncio.gather(
                    *[_one_global(r) for r in globs_before],
                    return_exceptions=True,
                )

            if died_notes and _admin_notifier is not None:
                try:
                    await _admin_notifier("\n".join(died_notes))
                except Exception as e:
                    log.warning("admin notify (global died): %s", e)

        except Exception as e:
            log.warning("health-check loop error: %s", e)

        try:
            await asyncio.wait_for(
                stop_event.wait() if stop_event else asyncio.sleep(interval),
                timeout=interval,
            )
        except asyncio.TimeoutError:
            pass
        except Exception:
            await asyncio.sleep(interval)


# ─────────────────────────────────────────────────────────────────
# Назначить прокси аккаунтам владельца
# ─────────────────────────────────────────────────────────────────
async def reassign_phones(owner_id: int) -> Dict[str, int]:
    """
    Берёт всех живых прокси владельца и рандомно распределяет их по
    его аккаунтам (одной прокси может достаться несколько аккаунтов,
    если живых меньше, чем аккаунтов).
    Возвращает {'updated': N, 'skipped': M} — сколько аккаунтов получили
    новый прокси и сколько остались без него (если alive-прокси нет).
    """
    accounts = await db.db_get_accounts_by_owner(owner_id)
    alive = await db.db_proxy_get_alive(owner_id)
    if not accounts:
        return {"updated": 0, "skipped": 0}
    if not alive:
        return {"updated": 0, "skipped": len(accounts)}

    updated = 0
    pool = [p["proxy_str"] for p in alive]
    random.shuffle(pool)
    for i, acc in enumerate(accounts):
        proxy_str = pool[i % len(pool)]
        await db.db_update_account_field(acc["phone"], "proxy", proxy_str)
        updated += 1
    return {"updated": updated, "skipped": 0}


# ─────────────────────────────────────────────────────────────────
# Подсчёт живых прокси
# ─────────────────────────────────────────────────────────────────
async def count_alive_socks5(owner_id: int) -> int:
    """Возвращает количество прокси со статусом 'alive' у владельца."""
    alive = await db.db_proxy_get_alive(owner_id)
    return len(alive)
