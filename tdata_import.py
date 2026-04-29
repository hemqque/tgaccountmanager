# -*- coding: utf-8 -*-
"""
tdata_import.py - Import accounts from TData (Telegram Desktop) format.

Accepts:
  - ZIP archive containing one or more tdata folders.
  - Already extracted folder.

Converts each TData to a Telethon .session via opentele.
"""

import os
import zipfile
import logging
import asyncio
from typing import List, Optional, Tuple

log = logging.getLogger("tdata")


def extract_archive(archive_path: str, dest_dir: str) -> bool:
    """Extracts .zip into dest_dir. Returns True/False."""
    os.makedirs(dest_dir, exist_ok=True)
    try:
        if zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path) as z:
                z.extractall(dest_dir)
            return True
    except Exception as e:
        log.warning("extract_archive %s: %s", archive_path, e)
    return False


TDATA_KEYFILES = ("key_datas", "key_data")
# Имя папки-слота — 16 hex-символов (например D877F783D5D3EF8C)
import re as _re
_HEX_SLOT_RE = _re.compile(r"^[0-9A-Fa-f]{16}$")


def _looks_like_tdata(folder: str) -> bool:
    try:
        items = os.listdir(folder)
    except Exception:
        return False
    items_low = {x.lower() for x in items}
    return any(k in items_low for k in TDATA_KEYFILES)


def validate_tdata_structure(folder: str) -> Optional[str]:
    """
    Проверяет, что в папке `folder` действительно есть валидная структура
    tdata. Минимальный набор файлов для импорта:
      • key_datas                  — главный ключ
      • <HEX_SLOT>/                — папка слота (16 hex-символов)
      • <HEX_SLOT>/configs         — MTP-конфиг
      • <HEX_SLOT>/maps            — индекс
      • <HEX_SLOT>s | …0 | …1      — MTP-AUTHORIZATION файл (КРИТИЧЕСКИ
                                     ВАЖНЫЙ — без него аккаунт нельзя
                                     подключить в Telegram).

    Возвращает None если всё ок, либо строку с описанием проблемы.
    """
    try:
        items = os.listdir(folder)
    except Exception as e:
        return f"не читается: {e}"

    items_low = {x.lower() for x in items}
    if not any(k in items_low for k in TDATA_KEYFILES):
        return "нет файла key_datas"

    # ищем hex-папку слота
    slot_name = None
    for it in items:
        full = os.path.join(folder, it)
        if os.path.isdir(full) and _HEX_SLOT_RE.match(it):
            slot_name = it
            break
    if not slot_name:
        return "нет папки-слота (имя из 16 hex-символов, напр. D877F783D5D3EF8C)"

    slot = os.path.join(folder, slot_name)

    # внутри слота должны быть configs и maps
    try:
        slot_items = {x.lower() for x in os.listdir(slot)}
    except Exception as e:
        return f"слот не читается: {e}"
    missing = []
    if "configs" not in slot_items:
        missing.append("configs")
    if "maps" not in slot_items:
        missing.append("maps")
    if missing:
        return f"в слоте {slot_name} нет: " + ", ".join(missing)

    # Проверяем наличие MTP-authorization файла рядом с key_datas
    # (имя слота + суффикс s / 0 / 1).
    mtp_candidates = [slot_name + "s", slot_name + "0", slot_name + "1"]
    mtp_exists = False
    items_real = set(items)  # сохраняем оригинальный регистр
    for cand in mtp_candidates:
        if cand in items_real or cand.lower() in items_low:
            mtp_exists = True
            break
        # также проверим прямо файл
        for it in items:
            if it.lower() == cand.lower():
                mtp_exists = True
                break
        if mtp_exists:
            break
    if not mtp_exists:
        return (f"нет MTP-authorization файла рядом с key_datas "
                f"(должен быть {slot_name}s или {slot_name}0/{slot_name}1) — "
                f"архив неполный")

    return None  # всё ок


# =================================================================
# Monkey-patch для opentele: совместимость с Telegram Desktop 4.x+
# =================================================================
_OPENTELE_PATCHED = False


def _patch_opentele() -> None:
    """
    Применяет monkey-патчи к opentele 1.15.x:
      1) kMaxAccounts = 32 (вместо 3) — для tdata с большим числом аккаунтов.
      2) StorageAccount.readMapWith — даже если mapData.read падает на
         неизвестном `key type` (Telegram Desktop 4.x добавил новые),
         всё равно вызвать readMtpData для извлечения MTP-ключа.
         RecursionError ловится и оборачивается в OpenTeleException,
         чтобы opentele пропустил «битый» аккаунт и пошёл дальше.
    Идемпотентно: патчит только один раз.
    """
    global _OPENTELE_PATCHED
    if _OPENTELE_PATCHED:
        return
    try:
        from opentele.td import tdesktop as tdt
        from opentele.td import account as acc_mod
        from opentele.exception import OpenTeleException
    except Exception as e:
        log.warning("opentele patch: import failed: %s", e)
        return

    # 1) kMaxAccounts: лимит на количество аккаунтов в tdata
    try:
        tdt.TDesktop.kMaxAccounts = 32
    except Exception:
        pass

    # 2) Resilient readMapWith
    StorageAccount = getattr(acc_mod, "StorageAccount", None)
    if StorageAccount is not None and hasattr(StorageAccount, "readMapWith"):
        try:
            from PyQt5.QtCore import QByteArray
        except Exception:
            QByteArray = None  # type: ignore

        def _patched_readMapWith(self, localKey, legacyPasscode=None):
            if legacyPasscode is None and QByteArray is not None:
                legacyPasscode = QByteArray()

            # 1. mapData.read — всё что угодно ловим
            try:
                self.mapData.read(localKey, legacyPasscode)
            except RecursionError:
                log.info("opentele: mapData.read recursion - пропускаем map")
            except BaseException as e:
                log.info("opentele: map read failed (%s) — продолжаем "
                         "к readMtpData", str(e)[:80])

            # 2. readMtpData — критически важен. Любая RecursionError или
            # глубокая ошибка → конвертируем в OpenTeleException, чтобы
            # opentele пропустил аккаунт без падения процесса.
            try:
                self.readMtpData()
            except RecursionError:
                raise OpenTeleException(
                    "readMtpData recursion (Telegram Desktop 4.x?)"
                )
            except OpenTeleException:
                raise  # opentele штатно перехватит и пропустит аккаунт
            except BaseException as e:
                # любые другие → тоже OpenTeleException, чтобы пропустить
                raise OpenTeleException(
                    f"readMtpData failed: {str(e)[:160]}"
                )

        StorageAccount.readMapWith = _patched_readMapWith
        log.info("opentele patch applied: kMaxAccounts=%d, "
                 "readMapWith resilient",
                 getattr(tdt.TDesktop, "kMaxAccounts", -1))
    else:
        log.warning("opentele patch: StorageAccount class not found")

    _OPENTELE_PATCHED = True


def find_tdata_folders(root: str) -> List[str]:
    """Recursively find all tdata-like folders under root."""
    found: List[str] = []
    if not os.path.isdir(root):
        return found
    if _looks_like_tdata(root):
        found.append(root)
        return found
    for dirpath, dirnames, filenames in os.walk(root):
        names_low = {n.lower() for n in filenames}
        if any(k in names_low for k in TDATA_KEYFILES):
            if not any(dirpath.startswith(f + os.sep) for f in found):
                found.append(dirpath)
    return found


async def import_one_tdata(tdata_folder: str,
                           sessions_dir: str,
                           proxy=None,
                           timeout: float = 30.0
                           ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Convert one tdata folder to Telethon session.
    Returns (phone, session_path, error).

    ВАЖНО: opentele 1.15.x бросает OpenTeleException(BaseException),
    т.е. он НЕ ловится обычным `except Exception`. Поэтому всё, что
    касается opentele, обернуто в `except BaseException`.
    """
    import sys as _sys
    _prev_limit = _sys.getrecursionlimit()
    if _prev_limit < 5000:
        _sys.setrecursionlimit(5000)

    # 0) Предварительная валидация — отсекает битые папки без opentele.
    err = validate_tdata_structure(tdata_folder)
    if err:
        return None, None, f"структура: {err}"

    # Применяем patch-набор для opentele (kMaxAccounts + resilient read).
    _patch_opentele()

    try:
        from opentele.td import TDesktop
        from opentele.api import UseCurrentSession
    except ImportError as e:
        return None, None, f"opentele not installed: {e}"

    os.makedirs(sessions_dir, exist_ok=True)
    tmp_session = os.path.join(
        sessions_dir,
        f"_tdata_tmp_{os.getpid()}_{abs(hash(tdata_folder)) % 10**8}",
    )
    for ext in (".session", ".session-journal"):
        try:
            if os.path.exists(tmp_session + ext):
                os.remove(tmp_session + ext)
        except Exception:
            pass

    # 1) Конвертация tdata → Telethon. opentele работает синхронно и
    #    может зависнуть/уйти в долгую рекурсию — выносим вызов в
    #    отдельный thread с таймаутом, чтобы не блокировать event loop.
    client = None

    def _sync_load_and_convert():
        """Синхронная часть: TDesktop() + блокирующий рекурсивный декрипт."""
        tdesk_local = TDesktop(tdata_folder)
        if not tdesk_local.isLoaded():
            return None, "TDesktop did not load (corrupt tdata?)"
        return tdesk_local, None

    try:
        tdesk_or_none, err = await asyncio.wait_for(
            asyncio.to_thread(_sync_load_and_convert),
            timeout=20,
        )
    except asyncio.TimeoutError:
        return None, None, ("opentele зависла (>20с) — tdata несовместима "
                            "с opentele 1.15")
    except RecursionError:
        return None, None, "opentele recursion overflow"
    except (Exception, BaseException) as e:
        if isinstance(e, (KeyboardInterrupt, SystemExit)):
            raise
        return None, None, f"TDesktop: {str(e)[:160]}"

    if err:
        return None, None, err
    tdesk = tdesk_or_none

    # ToTelethon — async, но внутри тоже может быть синхронный декрипт.
    # Тоже даём таймаут.
    try:
        client = await asyncio.wait_for(
            tdesk.ToTelethon(
                session=tmp_session,
                flag=UseCurrentSession,
                proxy=proxy,
            ),
            timeout=30,
        )
    except asyncio.TimeoutError:
        return None, None, "ToTelethon зависла (>30с)"
    except RecursionError:
        return None, None, "ToTelethon recursion overflow"
    except (Exception, BaseException) as e:
        if isinstance(e, (KeyboardInterrupt, SystemExit)):
            raise
        return None, None, f"ToTelethon: {str(e)[:160]}"

    # 2) Connect + get_me
    try:
        await asyncio.wait_for(client.connect(), timeout=timeout)
        if not await client.is_user_authorized():
            try:
                await client.disconnect()
            except Exception:
                pass
            return None, None, "session not authorized"
        me = await client.get_me()
        phone = ("+" + me.phone) if (me and me.phone) else None
        try:
            await client.disconnect()
        except Exception:
            pass
    except RecursionError:
        try:
            await client.disconnect()
        except Exception:
            pass
        return None, None, "connect/get_me recursion overflow"
    except (Exception, BaseException) as e:  # noqa: B902
        if isinstance(e, (KeyboardInterrupt, SystemExit)):
            raise
        try:
            await client.disconnect()
        except Exception:
            pass
        for ext in (".session", ".session-journal"):
            try:
                if os.path.exists(tmp_session + ext):
                    os.remove(tmp_session + ext)
            except Exception:
                pass
        return None, None, f"connect/get_me: {str(e)[:160]}"

    if not phone:
        for ext in (".session", ".session-journal"):
            try:
                if os.path.exists(tmp_session + ext):
                    os.remove(tmp_session + ext)
            except Exception:
                pass
        return None, None, "no phone in session"

    final_path = os.path.join(sessions_dir, phone)
    try:
        if os.path.exists(final_path + ".session"):
            os.remove(final_path + ".session")
        os.replace(tmp_session + ".session", final_path + ".session")
        if os.path.exists(tmp_session + ".session-journal"):
            try:
                os.replace(tmp_session + ".session-journal",
                           final_path + ".session-journal")
            except Exception:
                pass
    except Exception as e:
        return None, None, f"rename: {e}"

    return phone, final_path, None


async def import_from_archive(archive_path: str,
                              work_dir: str,
                              sessions_dir: str,
                              proxy=None
                              ) -> List[Tuple[str, Optional[str], Optional[str]]]:
    """High-level: extract archive, find all tdata, import each."""
    out: List[Tuple[str, Optional[str], Optional[str]]] = []
    extract_dir = os.path.join(work_dir, "extracted")
    if not extract_archive(archive_path, extract_dir):
        out.append((archive_path, None, "archive not extractable (need .zip)"))
        return out
    folders = find_tdata_folders(extract_dir)
    if not folders:
        out.append((archive_path, None, "no tdata folders in archive"))
        return out
    for tdata_folder in folders:
        try:
            phone, sess, err = await import_one_tdata(
                tdata_folder, sessions_dir=sessions_dir, proxy=proxy
            )
        except (Exception, BaseException) as e:  # noqa: B902
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            phone, err = None, f"unhandled: {e}"
        out.append((tdata_folder, phone, err))
    return out


async def import_from_local_folder(root: str,
                                   sessions_dir: str,
                                   proxy=None
                                   ) -> List[Tuple[str, Optional[str], Optional[str]]]:
    """
    Высокоуровневый импорт из УЖЕ распакованной локальной папки.
    Не требует распаковки — просто обходит указанный путь рекурсивно
    и импортирует все найденные tdata.
    """
    out: List[Tuple[str, Optional[str], Optional[str]]] = []
    if not os.path.isdir(root):
        out.append((root, None, "folder not found"))
        return out
    folders = find_tdata_folders(root)
    if not folders:
        out.append((root, None, "no tdata folders in this directory"))
        return out
    for tdata_folder in folders:
        try:
            phone, sess, err = await import_one_tdata(
                tdata_folder, sessions_dir=sessions_dir, proxy=proxy
            )
        except (Exception, BaseException) as e:  # noqa: B902
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            phone, err = None, f"unhandled: {e}"
        out.append((tdata_folder, phone, err))
    return out


# =================================================================
# Импорт .session файлов (Telethon-формат) — без opentele.
# =================================================================
def find_session_files(root: str) -> List[str]:
    """Рекурсивно ищет все *.session файлы в root."""
    found: List[str] = []
    if not os.path.isdir(root):
        return found
    for dirpath, _dirnames, filenames in os.walk(root):
        for f in filenames:
            if f.lower().endswith(".session"):
                # пропускаем journal-файлы и наши служебные tmp-сессии
                if f.lower().endswith(".session-journal"):
                    continue
                if f.startswith("_tdata_tmp_"):
                    continue
                found.append(os.path.join(dirpath, f))
    return found


def _is_telethon_session(path: str) -> bool:
    """
    Telethon хранит сессию как SQLite-БД с таблицами `sessions`, `entities` и др.
    Проверяем наличие таблицы `sessions` — самый надёжный признак.
    """
    try:
        import sqlite3
        con = sqlite3.connect(path)
        cur = con.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='sessions'"
        )
        row = cur.fetchone()
        con.close()
        return bool(row)
    except Exception:
        return False


async def import_one_session(src_session: str,
                             sessions_dir: str,
                             api_id: int,
                             api_hash: str,
                             proxy=None,
                             timeout: float = 30.0
                             ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Импорт одного .session-файла (формат Telethon).
      • копируется в sessions/<phone>.session,
      • открывается через TelegramClient, проверяется авторизация,
      • извлекается номер телефона из аккаунта.

    Возвращает (phone, session_path, error).
    """
    if not os.path.isfile(src_session):
        return None, None, "session file not found"

    if not _is_telethon_session(src_session):
        return None, None, ("файл не похож на Telethon .session "
                            "(нет таблицы sessions в SQLite)")

    os.makedirs(sessions_dir, exist_ok=True)

    # Копируем в tmp-имя в sessions/, потом откроем и переименуем по phone
    import shutil
    tmp_name = f"_session_tmp_{os.getpid()}_{abs(hash(src_session)) % 10**8}"
    tmp_path = os.path.join(sessions_dir, tmp_name + ".session")
    try:
        # очистим предыдущий tmp если был
        for ext in (".session", ".session-journal"):
            p = os.path.join(sessions_dir, tmp_name + ext)
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        shutil.copy2(src_session, tmp_path)
    except Exception as e:
        return None, None, f"copy: {e}"

    # Открываем через Telethon
    try:
        from telethon import TelegramClient
        from global_proxy import proxy_to_telethon
    except Exception as e:
        return None, None, f"telethon imports: {e}"

    tproxy = proxy_to_telethon(proxy or "") if proxy else None
    session_base = os.path.join(sessions_dir, tmp_name)
    client = TelegramClient(session_base, api_id, api_hash, proxy=tproxy)

    try:
        await asyncio.wait_for(client.connect(), timeout=timeout)
        if not await client.is_user_authorized():
            try:
                await client.disconnect()
            except Exception:
                pass
            # удалим tmp
            for ext in (".session", ".session-journal"):
                p = os.path.join(sessions_dir, tmp_name + ext)
                try:
                    if os.path.exists(p):
                        os.remove(p)
                except Exception:
                    pass
            return None, None, "session not authorized"
        me = await client.get_me()
        phone = ("+" + me.phone) if (me and me.phone) else None
        try:
            await client.disconnect()
        except Exception:
            pass
    except Exception as e:
        try:
            await client.disconnect()
        except Exception:
            pass
        for ext in (".session", ".session-journal"):
            p = os.path.join(sessions_dir, tmp_name + ext)
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        return None, None, f"connect/get_me: {str(e)[:160]}"

    if not phone:
        for ext in (".session", ".session-journal"):
            p = os.path.join(sessions_dir, tmp_name + ext)
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        return None, None, "no phone in session"

    # переименуем tmp в <phone>
    final_base = os.path.join(sessions_dir, phone)
    try:
        if os.path.exists(final_base + ".session"):
            os.remove(final_base + ".session")
        os.replace(os.path.join(sessions_dir, tmp_name + ".session"),
                   final_base + ".session")
        # journal — если есть
        j_src = os.path.join(sessions_dir, tmp_name + ".session-journal")
        if os.path.exists(j_src):
            try:
                os.replace(j_src, final_base + ".session-journal")
            except Exception:
                pass
    except Exception as e:
        return None, None, f"rename: {e}"

    return phone, final_base, None


async def import_sessions_from_archive(archive_path: str,
                                       work_dir: str,
                                       sessions_dir: str,
                                       api_id: int,
                                       api_hash: str,
                                       proxy=None
                                       ) -> List[Tuple[str, Optional[str], Optional[str]]]:
    """Распаковать архив и импортировать все *.session внутри."""
    out: List[Tuple[str, Optional[str], Optional[str]]] = []
    extract_dir = os.path.join(work_dir, "extracted")
    if not extract_archive(archive_path, extract_dir):
        out.append((archive_path, None, "архив не распаковался (нужен .zip)"))
        return out
    files = find_session_files(extract_dir)
    if not files:
        out.append((archive_path, None,
                    "в архиве не найдено ни одного *.session"))
        return out
    for sf in files:
        try:
            phone, sess, err = await import_one_session(
                sf, sessions_dir=sessions_dir,
                api_id=api_id, api_hash=api_hash, proxy=proxy,
            )
        except Exception as e:
            phone, err = None, f"unhandled: {e}"
        out.append((sf, phone, err))
    return out


async def import_sessions_from_folder(root: str,
                                      sessions_dir: str,
                                      api_id: int,
                                      api_hash: str,
                                      proxy=None
                                      ) -> List[Tuple[str, Optional[str], Optional[str]]]:
    """Сканировать локальную папку и импортировать все *.session внутри."""
    out: List[Tuple[str, Optional[str], Optional[str]]] = []
    if not os.path.isdir(root):
        out.append((root, None, "folder not found"))
        return out
    files = find_session_files(root)
    if not files:
        out.append((root, None, "*.session-файлы не найдены"))
        return out
    for sf in files:
        try:
            phone, sess, err = await import_one_session(
                sf, sessions_dir=sessions_dir,
                api_id=api_id, api_hash=api_hash, proxy=proxy,
            )
        except Exception as e:
            phone, err = None, f"unhandled: {e}"
        out.append((sf, phone, err))
    return out
