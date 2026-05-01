# -*- coding: utf-8 -*-
"""
db.py — Слой работы с базой данных (SQLite через aiosqlite).

Содержит инициализацию схемы и все CRUD-функции, используемые менеджером:
аккаунты, задачи LDV/XO, whitelist/admins, прокси, настройки автоответов,
пользовательские настройки и сохранение состояний регистрации.
"""

import time
import json
import aiosqlite
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any

from config import DB_NAME, INITIAL_ADMIN_IDS

# Опциональный начальный whitelist (если в config объявлен)
try:
    from config import INITIAL_WHITELIST_IDS  # type: ignore
except Exception:
    INITIAL_WHITELIST_IDS = []


# ─────────────────────────────────────────────────────────────────
# Единый хелпер соединения: WAL + busy_timeout + Row factory везде
# ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def _conn():
    """
    Async context manager для подключения к БД.
    Гарантирует busy_timeout и Row factory для всех запросов.
    """
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA busy_timeout=5000")
        yield db


# ─────────────────────────────────────────────────────────────────
# init_db: создаёт все таблицы, если их ещё нет, и загружает в admins
# идентификаторы из INITIAL_ADMIN_IDS.
# ─────────────────────────────────────────────────────────────────
async def init_db() -> None:
    async with _conn() as db:
        # WAL-режим: параллельные читатели не блокируют писателей
        await db.execute("PRAGMA journal_mode=WAL")
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS accounts(
                phone     TEXT PRIMARY KEY,
                proxy     TEXT,
                note      TEXT,
                grp       TEXT,
                username  TEXT,
                owner_id  INTEGER
            );

            CREATE TABLE IF NOT EXISTS ldv_tasks(
                phone     TEXT PRIMARY KEY,
                step      INTEGER DEFAULT 0,
                next_run  REAL,
                status    TEXT DEFAULT 'pending',
                owner_id  INTEGER
            );

            CREATE TABLE IF NOT EXISTS xo_tasks(
                phone     TEXT PRIMARY KEY,
                next_run  REAL,
                status    TEXT DEFAULT 'pending',
                owner_id  INTEGER
            );

            CREATE TABLE IF NOT EXISTS whitelist(
                user_id   INTEGER PRIMARY KEY,
                username  TEXT,
                added_at  REAL
            );

            CREATE TABLE IF NOT EXISTS admins(
                user_id   INTEGER PRIMARY KEY,
                added_at  REAL
            );

            CREATE TABLE IF NOT EXISTS user_proxies(
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id   INTEGER,
                proxy_str  TEXT,
                note       TEXT,
                status     TEXT DEFAULT 'unknown',
                added_at   REAL
            );

            CREATE TABLE IF NOT EXISTS autoreply_settings(
                owner_id     INTEGER,
                phone        TEXT,
                enabled      INTEGER DEFAULT 0,
                custom_text  TEXT,
                PRIMARY KEY (owner_id, phone)
            );

            CREATE TABLE IF NOT EXISTS user_settings(
                owner_id      INTEGER PRIMARY KEY,
                logs_enabled  INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS reg_state(
                phone       TEXT,
                bot         TEXT,
                step        INTEGER,
                data_json   TEXT,
                owner_id    INTEGER,
                updated_at  REAL,
                PRIMARY KEY (phone, bot)
            );

            CREATE TABLE IF NOT EXISTS global_proxies(
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                proxy_str  TEXT UNIQUE,
                note       TEXT,
                status     TEXT DEFAULT 'unknown',
                added_at   REAL
            );

            -- Индексы для горячих запросов (планировщики проверяют каждые 10 сек)
            CREATE INDEX IF NOT EXISTS idx_ldv_pending
                ON ldv_tasks(status, next_run);
            CREATE INDEX IF NOT EXISTS idx_xo_pending
                ON xo_tasks(status, next_run);
            CREATE INDEX IF NOT EXISTS idx_acc_owner
                ON accounts(owner_id);
            CREATE INDEX IF NOT EXISTS idx_acc_grp
                ON accounts(owner_id, grp);
            CREATE INDEX IF NOT EXISTS idx_ar_owner
                ON autoreply_settings(owner_id);
            """
        )
        # Загрузить начальных админов
        for uid in INITIAL_ADMIN_IDS:
            await db.execute(
                "INSERT OR IGNORE INTO admins(user_id, added_at) VALUES (?, ?)",
                (uid, time.time()),
            )
        # Загрузить начальный whitelist
        for uid in INITIAL_WHITELIST_IDS:
            await db.execute(
                "INSERT OR IGNORE INTO whitelist(user_id, username, added_at) "
                "VALUES (?, ?, ?)",
                (uid, "", time.time()),
            )
        await db.commit()


# =================================================================
# ── ACCOUNTS ─────────────────────────────────────────────────────
# =================================================================

async def db_add_account(phone: str, proxy: str, note: str, grp: str,
                         username: str, owner_id: int) -> None:
    async with _conn() as db:
        await db.execute(
            "INSERT OR REPLACE INTO accounts(phone, proxy, note, grp, "
            "username, owner_id) VALUES (?,?,?,?,?,?)",
            (phone, proxy, note, grp, username, owner_id),
        )
        await db.commit()


async def db_get_account(phone: str) -> Optional[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute("SELECT * FROM accounts WHERE phone=?", (phone,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def db_get_accounts_by_owner(owner_id: int) -> List[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT * FROM accounts WHERE owner_id=? ORDER BY phone", (owner_id,)
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def db_get_all_accounts() -> List[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute("SELECT * FROM accounts ORDER BY owner_id, phone")
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def db_update_account_field(phone: str, field: str, value: Any) -> None:
    if field not in ("proxy", "note", "grp", "username"):
        raise ValueError(f"Bad field: {field}")
    async with _conn() as db:
        await db.execute(
            f"UPDATE accounts SET {field}=? WHERE phone=?", (value, phone)
        )
        await db.commit()


async def db_delete_account(phone: str) -> None:
    async with _conn() as db:
        await db.execute("DELETE FROM accounts WHERE phone=?", (phone,))
        await db.execute("DELETE FROM ldv_tasks WHERE phone=?", (phone,))
        await db.execute("DELETE FROM xo_tasks WHERE phone=?", (phone,))
        await db.execute("DELETE FROM autoreply_settings WHERE phone=?", (phone,))
        await db.execute("DELETE FROM reg_state WHERE phone=?", (phone,))
        await db.commit()


async def db_get_groups_by_owner(owner_id: int) -> List[str]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT DISTINCT grp FROM accounts WHERE owner_id=? AND grp IS NOT NULL "
            "AND grp<>'' ORDER BY grp", (owner_id,)
        )
        rows = await cur.fetchall()
        return [r[0] for r in rows]


async def db_get_accounts_by_group(owner_id: int, grp: str) -> List[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT * FROM accounts WHERE owner_id=? AND grp=? ORDER BY phone",
            (owner_id, grp),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


# =================================================================
# ── LDV TASKS ────────────────────────────────────────────────────
# =================================================================

async def db_schedule_ldv_task(phone: str, owner_id: int, next_run: float,
                               step: int = 0, status: str = "pending") -> None:
    async with _conn() as db:
        await db.execute(
            "INSERT OR REPLACE INTO ldv_tasks(phone, step, next_run, status, "
            "owner_id) VALUES (?,?,?,?,?)",
            (phone, step, next_run, status, owner_id),
        )
        await db.commit()


async def db_get_pending_ldv_tasks() -> List[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT * FROM ldv_tasks WHERE status='pending' AND next_run<=?",
            (time.time(),),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def db_get_ldv_tasks_by_owner(owner_id: int) -> List[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT * FROM ldv_tasks WHERE owner_id=? ORDER BY next_run",
            (owner_id,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def db_update_ldv_task(phone: str, step: Optional[int] = None,
                             next_run: Optional[float] = None,
                             status: Optional[str] = None) -> None:
    sets, args = [], []
    if step is not None:
        sets.append("step=?"); args.append(step)
    if next_run is not None:
        sets.append("next_run=?"); args.append(next_run)
    if status is not None:
        sets.append("status=?"); args.append(status)
    if not sets:
        return
    args.append(phone)
    async with _conn() as db:
        await db.execute(
            f"UPDATE ldv_tasks SET {', '.join(sets)} WHERE phone=?", args
        )
        await db.commit()


async def db_delete_ldv_task(phone: str) -> None:
    async with _conn() as db:
        await db.execute("DELETE FROM ldv_tasks WHERE phone=?", (phone,))
        await db.commit()


async def db_delete_ldv_tasks_by_owner(owner_id: int) -> int:
    async with _conn() as db:
        cur = await db.execute(
            "DELETE FROM ldv_tasks WHERE owner_id=?", (owner_id,)
        )
        await db.commit()
        return cur.rowcount or 0


async def db_delete_ldv_tasks_by_group(owner_id: int, grp: str) -> int:
    async with _conn() as db:
        cur = await db.execute(
            "DELETE FROM ldv_tasks WHERE owner_id=? AND phone IN "
            "(SELECT phone FROM accounts WHERE owner_id=? AND grp=?)",
            (owner_id, owner_id, grp),
        )
        await db.commit()
        return cur.rowcount or 0


# =================================================================
# ── XO TASKS ─────────────────────────────────────────────────────
# =================================================================

async def db_schedule_xo_task(phone: str, owner_id: int, next_run: float,
                              status: str = "pending") -> None:
    async with _conn() as db:
        await db.execute(
            "INSERT OR REPLACE INTO xo_tasks(phone, next_run, status, owner_id) "
            "VALUES (?,?,?,?)",
            (phone, next_run, status, owner_id),
        )
        await db.commit()


async def db_get_pending_xo_tasks() -> List[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT * FROM xo_tasks WHERE status='pending' AND next_run<=?",
            (time.time(),),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def db_get_xo_tasks_by_owner(owner_id: int) -> List[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT * FROM xo_tasks WHERE owner_id=? ORDER BY next_run",
            (owner_id,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def db_update_xo_task(phone: str, next_run: Optional[float] = None,
                            status: Optional[str] = None) -> None:
    sets, args = [], []
    if next_run is not None:
        sets.append("next_run=?"); args.append(next_run)
    if status is not None:
        sets.append("status=?"); args.append(status)
    if not sets:
        return
    args.append(phone)
    async with _conn() as db:
        await db.execute(
            f"UPDATE xo_tasks SET {', '.join(sets)} WHERE phone=?", args
        )
        await db.commit()


async def db_delete_xo_task(phone: str) -> None:
    async with _conn() as db:
        await db.execute("DELETE FROM xo_tasks WHERE phone=?", (phone,))
        await db.commit()


# =================================================================
# ── WHITELIST / ADMINS ───────────────────────────────────────────
# =================================================================

async def db_whitelist_add(user_id: int, username: str = "") -> None:
    async with _conn() as db:
        await db.execute(
            "INSERT OR REPLACE INTO whitelist(user_id, username, added_at) "
            "VALUES (?,?,?)", (user_id, username, time.time())
        )
        await db.commit()


async def db_whitelist_remove(user_id: int) -> None:
    async with _conn() as db:
        await db.execute("DELETE FROM whitelist WHERE user_id=?", (user_id,))
        await db.commit()


async def db_whitelist_get_all() -> List[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute("SELECT * FROM whitelist ORDER BY added_at DESC")
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def db_whitelist_check(user_id: int) -> bool:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT 1 FROM whitelist WHERE user_id=?", (user_id,)
        )
        return (await cur.fetchone()) is not None


async def db_admins_add(user_id: int) -> None:
    async with _conn() as db:
        await db.execute(
            "INSERT OR IGNORE INTO admins(user_id, added_at) VALUES (?,?)",
            (user_id, time.time())
        )
        await db.commit()


async def db_admins_remove(user_id: int) -> None:
    async with _conn() as db:
        await db.execute("DELETE FROM admins WHERE user_id=?", (user_id,))
        await db.commit()


async def db_admins_get_all() -> List[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute("SELECT * FROM admins ORDER BY added_at")
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def db_admins_check(user_id: int) -> bool:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT 1 FROM admins WHERE user_id=?", (user_id,)
        )
        return (await cur.fetchone()) is not None


# =================================================================
# ── USER PROXIES ─────────────────────────────────────────────────
# =================================================================

async def db_proxy_add(owner_id: int, proxy_str: str, note: str = "") -> int:
    async with _conn() as db:
        cur = await db.execute(
            "INSERT INTO user_proxies(owner_id, proxy_str, note, status, added_at) "
            "VALUES (?,?,?,?,?)",
            (owner_id, proxy_str, note, "unknown", time.time()),
        )
        await db.commit()
        return cur.lastrowid


async def db_proxy_get_by_id(proxy_id: int) -> Optional[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT * FROM user_proxies WHERE id=?", (proxy_id,)
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def db_proxy_get_by_owner(owner_id: int) -> List[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT * FROM user_proxies WHERE owner_id=? ORDER BY id", (owner_id,)
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def db_proxy_get_alive(owner_id: int) -> List[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT * FROM user_proxies WHERE owner_id=? AND status='alive'",
            (owner_id,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def db_proxy_update_status(proxy_id: int, status: str) -> None:
    async with _conn() as db:
        await db.execute(
            "UPDATE user_proxies SET status=? WHERE id=?", (status, proxy_id)
        )
        await db.commit()


async def db_proxy_update_note(proxy_id: int, note: str) -> None:
    async with _conn() as db:
        await db.execute(
            "UPDATE user_proxies SET note=? WHERE id=?", (note, proxy_id)
        )
        await db.commit()


async def db_proxy_delete(proxy_id: int) -> None:
    async with _conn() as db:
        await db.execute("DELETE FROM user_proxies WHERE id=?", (proxy_id,))
        await db.commit()


# =================================================================
# ── AUTOREPLY SETTINGS ───────────────────────────────────────────
# =================================================================

async def db_ar_set_enabled(owner_id: int, phone: str, enabled: bool) -> None:
    async with _conn() as db:
        await db.execute(
            "INSERT INTO autoreply_settings(owner_id, phone, enabled, custom_text) "
            "VALUES (?,?,?, COALESCE((SELECT custom_text FROM autoreply_settings "
            "WHERE owner_id=? AND phone=?), NULL)) "
            "ON CONFLICT(owner_id, phone) DO UPDATE SET enabled=excluded.enabled",
            (owner_id, phone, 1 if enabled else 0, owner_id, phone),
        )
        await db.commit()


async def db_ar_set_custom_text(owner_id: int, phone: str,
                                custom_text: Optional[str]) -> None:
    async with _conn() as db:
        await db.execute(
            "INSERT INTO autoreply_settings(owner_id, phone, enabled, custom_text) "
            "VALUES (?,?, COALESCE((SELECT enabled FROM autoreply_settings "
            "WHERE owner_id=? AND phone=?), 0), ?) "
            "ON CONFLICT(owner_id, phone) DO UPDATE "
            "SET custom_text=excluded.custom_text",
            (owner_id, phone, owner_id, phone, custom_text),
        )
        await db.commit()


async def db_ar_get_settings(owner_id: int, phone: str) -> Dict[str, Any]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT * FROM autoreply_settings WHERE owner_id=? AND phone=?",
            (owner_id, phone),
        )
        row = await cur.fetchone()
        if row:
            return dict(row)
        return {"owner_id": owner_id, "phone": phone, "enabled": 0,
                "custom_text": None}


async def db_ar_is_enabled(owner_id: int, phone: str) -> bool:
    s = await db_ar_get_settings(owner_id, phone)
    return bool(s.get("enabled"))


async def db_ar_get_enabled_phones() -> List[Dict[str, Any]]:
    """Возвращает все enabled-пары (owner_id, phone, custom_text) — для запуска
    автоответов при старте процесса."""
    async with _conn() as db:
        cur = await db.execute(
            "SELECT owner_id, phone, custom_text FROM autoreply_settings "
            "WHERE enabled=1"
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def db_ar_get_enabled_phones_by_owner(owner_id: int) -> List[str]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT phone FROM autoreply_settings WHERE owner_id=? AND enabled=1",
            (owner_id,),
        )
        rows = await cur.fetchall()
        return [r[0] for r in rows]


async def db_ar_get_settings_bulk(owner_id: int) -> Dict[str, Dict[str, Any]]:
    """
    Возвращает {phone: settings_dict} для всех аккаунтов владельца одним запросом.
    Вместо N отдельных db_ar_get_settings() при отрисовке списка автоответов.
    """
    async with _conn() as db:
        cur = await db.execute(
            "SELECT phone, enabled, custom_text "
            "FROM autoreply_settings WHERE owner_id=?",
            (owner_id,),
        )
        rows = await cur.fetchall()
        return {r["phone"]: dict(r) for r in rows}


# =================================================================
# ── USER SETTINGS ────────────────────────────────────────────────
# =================================================================

async def db_user_settings_get(owner_id: int) -> Dict[str, Any]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT * FROM user_settings WHERE owner_id=?", (owner_id,)
        )
        row = await cur.fetchone()
        if row:
            return dict(row)
        return {"owner_id": owner_id, "logs_enabled": 0}


async def db_user_settings_set_logs(owner_id: int, enabled: bool) -> None:
    async with _conn() as db:
        await db.execute(
            "INSERT INTO user_settings(owner_id, logs_enabled) VALUES (?,?) "
            "ON CONFLICT(owner_id) DO UPDATE SET logs_enabled=excluded.logs_enabled",
            (owner_id, 1 if enabled else 0),
        )
        await db.commit()


# =================================================================
# ── REG STATE (возобновление регистраций) ───────────────────────
# =================================================================

async def db_save_reg_state(phone: str, bot: str, step: int,
                            data: Dict[str, Any], owner_id: int) -> None:
    async with _conn() as db:
        await db.execute(
            "INSERT OR REPLACE INTO reg_state(phone, bot, step, data_json, "
            "owner_id, updated_at) VALUES (?,?,?,?,?,?)",
            (phone, bot, step, json.dumps(data, ensure_ascii=False),
             owner_id, time.time()),
        )
        await db.commit()


async def db_get_reg_state(phone: str, bot: str) -> Optional[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT * FROM reg_state WHERE phone=? AND bot=?", (phone, bot)
        )
        row = await cur.fetchone()
        if not row:
            return None
        d = dict(row)
        try:
            d["data"] = json.loads(d.get("data_json") or "{}")
        except Exception:
            d["data"] = {}
        return d


async def db_delete_reg_state(phone: str, bot: str) -> None:
    async with _conn() as db:
        await db.execute(
            "DELETE FROM reg_state WHERE phone=? AND bot=?", (phone, bot)
        )
        await db.commit()


# =================================================================
# ── GLOBAL PROXIES (общий пул, управляется админами) ─────────────
# =================================================================

async def db_gproxy_add(proxy_str: str, note: str = "") -> Optional[int]:
    """
    Добавляет глобальный прокси. Возвращает id новой записи, либо
    None если такой proxy_str уже есть (UNIQUE).
    """
    async with _conn() as db:
        try:
            cur = await db.execute(
                "INSERT INTO global_proxies(proxy_str, note, status, added_at) "
                "VALUES (?,?,?,?)",
                (proxy_str, note, "unknown", time.time()),
            )
            await db.commit()
            return cur.lastrowid
        except aiosqlite.IntegrityError:
            return None


async def db_gproxy_get_by_id(proxy_id: int) -> Optional[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT * FROM global_proxies WHERE id=?", (proxy_id,)
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def db_gproxy_get_all() -> List[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute("SELECT * FROM global_proxies ORDER BY id")
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def db_gproxy_get_alive() -> List[Dict[str, Any]]:
    async with _conn() as db:
        cur = await db.execute(
            "SELECT * FROM global_proxies WHERE status='alive' ORDER BY id"
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def db_gproxy_update_status(proxy_id: int, status: str) -> None:
    async with _conn() as db:
        await db.execute(
            "UPDATE global_proxies SET status=? WHERE id=?", (status, proxy_id)
        )
        await db.commit()


async def db_gproxy_update_note(proxy_id: int, note: str) -> None:
    async with _conn() as db:
        await db.execute(
            "UPDATE global_proxies SET note=? WHERE id=?", (note, proxy_id)
        )
        await db.commit()


async def db_gproxy_delete(proxy_id: int) -> None:
    async with _conn() as db:
        await db.execute("DELETE FROM global_proxies WHERE id=?", (proxy_id,))
        await db.commit()
