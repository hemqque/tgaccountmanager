# -*- coding: utf-8 -*-
"""
tdata_worker.py — subprocess-воркер: tdata → Telethon .session

Запускается дочерним процессом из tdata_import.py.
Выводит одну строку JSON в stdout: {"ok": true} или {"error": "..."}
Логи пишет в stderr.
"""

import sys
import os
import json
import asyncio
import logging

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] tdata_worker: %(message)s",
)
log = logging.getLogger("tdata_worker")

_PATCHED = False


def _patch_opentele() -> None:
    global _PATCHED
    if _PATCHED:
        return

    try:
        from opentele.td import tdesktop as tdt
        from opentele.td.account import MapData
        from opentele.exception import OpenTeleException, TDataReadMapDataFailed
    except Exception as e:
        log.warning("patch import failed: %s", e)
        return

    # 1) kMaxAccounts — поддержка tdata с >3 аккаунтами
    try:
        tdt.TDesktop.kMaxAccounts = 32
    except Exception:
        pass

    # 2) Патч MapData.read — пропускаем неизвестные key type (TD 4.x+)
    #    вместо того чтобы кидать исключение и обрывать загрузку.
    #    Когда Unknown key type не бросает — оригинальный readMapWith
    #    продолжает и сам вызывает readMtpData, которому для работы
    #    нужен только StorageAccount.localKey (уже установлен через start()).
    _orig_read = MapData.read

    def _resilient_read(self, localKey, legacyPasscode=None):
        try:
            from PyQt5.QtCore import QByteArray
            if legacyPasscode is None:
                legacyPasscode = QByteArray()
        except Exception:
            pass
        try:
            _orig_read(self, localKey, legacyPasscode)
        except TDataReadMapDataFailed as e:
            if "Unknown key type" in str(e):
                # TD 4.x добавил новые типы — просто пропускаем,
                # readMtpData всё равно извлечёт auth key.
                log.info("map: %s — пропускаем, продолжаем", str(e)[:80])
            else:
                # Файл не найден / не расшифрован — пустой слот, пусть
                # оригинальный readMapWith вернёт False.
                raise

    MapData.read = _resilient_read
    log.info("patch applied: kMaxAccounts=32, MapData.read resilient")
    _PATCHED = True


async def run(tdata_folder: str, tmp_session: str) -> None:
    _patch_opentele()

    try:
        from opentele.td import TDesktop
        from opentele.api import UseCurrentSession
    except ImportError as e:
        _out({"error": f"opentele not installed: {e}"})
        return

    try:
        tdesk = TDesktop(tdata_folder)
    except BaseException as e:
        _out({"error": f"TDesktop init: {str(e)[:200]}"})
        return

    if not tdesk.isLoaded():
        _out({"error": "TDesktop did not load (tdata несовместима или повреждена)"})
        return

    log.info("TDesktop loaded, accounts: %d", len(tdesk.accounts))

    try:
        client = await asyncio.wait_for(
            tdesk.ToTelethon(session=tmp_session, flag=UseCurrentSession),
            timeout=30,
        )
        try:
            await client.disconnect()
        except Exception:
            pass
    except asyncio.TimeoutError:
        _out({"error": "ToTelethon timeout (>30s)"})
        return
    except BaseException as e:
        _out({"error": f"ToTelethon: {str(e)[:200]}"})
        return

    if not os.path.exists(tmp_session + ".session"):
        _out({"error": "session file was not created"})
        return

    _out({"ok": True})


def _out(data: dict) -> None:
    sys.stdout.write(json.dumps(data, ensure_ascii=False) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        _out({"error": "usage: tdata_worker.py <tdata_folder> <tmp_session>"})
        sys.exit(1)
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run(sys.argv[1], sys.argv[2]))
