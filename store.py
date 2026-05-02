# -*- coding: utf-8 -*-
"""
store.py — Глобальный in-memory Store: всё состояние, которое нужно держать
между обработчиками aiogram и фоновыми тасками.

ВАЖНО: Store создаётся один раз в main.py и передаётся всем модулям, которым
он нужен. После перезапуска процесса Store пустой — всё восстанавливается из
БД (accounts, ldv_tasks, xo_tasks, autoreply_settings, reg_state).
"""

from typing import Any, Dict, List, Optional, Set
import asyncio


class Store:
    """
    Поля:
      temp_photos          — uid -> List[str] (пути к загруженным файлам)
      mass_data            — uid -> dict с данными «массового залива»
                             (names, bios, photos)
      last_ldv_msg         — phone -> Telethon Message (последнее от
                             @leomatchbot)
      current_liking_phones — set(phone), сейчас активно лайкающие в LDV
      ldv_data             — uid -> dict (ages/sex/target/cities/names/photos
                             /delay_min/targets[List[phone]])
      xo_data              — uid -> dict (sex/birthday/city/name/photos
                             /targets[List[phone]])
      selected_accs        — uid -> List[phone] (выбранные пользователем
                             в шагах диалогов)
      photo_collecting     — uid -> bool (в данный момент собираем фото)
      collected_photos     — uid -> set(file_unique_id) (для дедупликации)
      liking_queue         — простой список ожидающих циклов LDV
                             (для UI «Активные циклы»)
      paused_phones        — set(phone) — LDV-аккаунты, которые на паузе
      cancelled_phones     — set(phone) — LDV-аккаунты, которые надо стопнуть
      xo_liking_tasks      — phone -> asyncio.Task (бегущие XO-лайкинги)
      xo_liking_paused     — set(phone) — паузанутые XO-лайкинги
      active_action        — uid -> str (что сейчас делает пользователь;
                             пусто/None = свободен)
      progress_msg         — uid -> dict {chat_id, message_id, total, done,
                                          errors, current}
      ldv_reg_cancel       — set(phone) — отмена регистрации LDV «партии»
      xo_reg_cancel        — set(phone) — отмена регистрации XO «партии»
    """

    def __init__(self) -> None:
        self.temp_photos: Dict[int, List[str]] = {}
        self.mass_data: Dict[int, Dict[str, Any]] = {}

        self.last_ldv_msg: Dict[str, Any] = {}
        self.current_liking_phones: Set[str] = set()

        self.ldv_data: Dict[int, Dict[str, Any]] = {}
        self.xo_data: Dict[int, Dict[str, Any]] = {}

        self.selected_accs: Dict[int, List[str]] = {}
        self.photo_collecting: Dict[int, bool] = {}
        self.collected_photos: Dict[int, Set[str]] = {}

        self.liking_queue: List[Dict[str, Any]] = []

        self.paused_phones: Set[str] = set()
        self.cancelled_phones: Set[str] = set()

        self.xo_liking_tasks: Dict[str, asyncio.Task] = {}
        self.xo_liking_paused: Set[str] = set()

        self.active_action: Dict[int, str] = {}
        self.progress_msg: Dict[int, Dict[str, Any]] = {}

        self.ldv_reg_cancel: Set[str] = set()
        self.xo_reg_cancel: Set[str] = set()

    # ───────────────── Helpers ─────────────────
    def reset_user(self, uid: int) -> None:
        """Полная очистка пользовательских dialog-данных (после Главное меню).
        progress_msg НЕ сбрасывается намеренно: если фоновая задача ещё работает,
        _finish_progress сама удалит запись когда завершится."""
        for d in (self.temp_photos, self.mass_data, self.ldv_data, self.xo_data,
                  self.selected_accs, self.photo_collecting,
                  self.collected_photos, self.active_action):
            d.pop(uid, None)

    def is_busy(self, uid: int) -> bool:
        return bool(self.active_action.get(uid))

    def set_action(self, uid: int, name: Optional[str]) -> None:
        if name:
            self.active_action[uid] = name
        else:
            self.active_action.pop(uid, None)

    def add_temp_photo(self, uid: int, path: str) -> None:
        self.temp_photos.setdefault(uid, []).append(path)

    def get_temp_photos(self, uid: int) -> List[str]:
        return list(self.temp_photos.get(uid, []))

    def clear_temp_photos(self, uid: int) -> None:
        self.temp_photos.pop(uid, None)
        self.collected_photos.pop(uid, None)
