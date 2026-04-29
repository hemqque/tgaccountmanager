# -*- coding: utf-8 -*-
"""
task_queue.py — Очередь «тяжёлых» задач.

Логика:
  • На весь процесс есть один Semaphore с ёмкостью MAX_CONCURRENT_TASKS
    (по умолчанию 2).
  • Любая «тяжёлая» задача (массовый залив / партия регистраций /
    LDV-цикл партией / XO-партия) кладётся через TaskQueue.submit().
  • Если все слоты заняты — пользователь получает сообщение
    «⏳ В очереди (позиция: N) — <title>». Когда подошла очередь и
    задача стартует — «▶️ Задача запущена: <title>».
  • Очередь FIFO, считается позиция от количества ожидающих + бегущих.
"""

import asyncio
import logging
from typing import Awaitable, Callable, Optional, List, Dict, Any

from config import MAX_CONCURRENT_TASKS

log = logging.getLogger("task_queue")

# Тип callback: notify(owner_id: int, text: str) -> Awaitable[None]
NotifyFn = Callable[[int, str], Awaitable[None]]


class TaskQueue:
    """
    submit(coro_factory, owner_id, notify, title)
       coro_factory — async-функция без аргументов или partial; то, что будет
                      запущено внутри слота семафора.
       owner_id     — кому слать уведомления.
       notify       — async fn(owner_id, text). Если None — молча.
       title        — короткое имя задачи для уведомлений.
    """

    def __init__(self, max_concurrent: int = MAX_CONCURRENT_TASKS) -> None:
        self.max_concurrent = max_concurrent
        self._sem = asyncio.Semaphore(max_concurrent)
        # счётчики
        self._running: int = 0
        self._waiting: List[Dict[str, Any]] = []   # видимая «очередь»
        self._lock = asyncio.Lock()

    # -------------- public --------------
    @property
    def running(self) -> int:
        return self._running

    @property
    def waiting(self) -> int:
        return len(self._waiting)

    def status(self) -> Dict[str, int]:
        return {
            "max": self.max_concurrent,
            "running": self._running,
            "waiting": self.waiting,
        }

    async def submit(self,
                     coro_factory: Callable[[], Awaitable[Any]],
                     owner_id: Optional[int] = None,
                     notify: Optional[NotifyFn] = None,
                     title: str = "Задача") -> asyncio.Task:
        """
        Положить задачу в очередь и сразу вернуть asyncio.Task,
        реальный запуск произойдёт после получения слота семафора.
        """
        item = {
            "title": title,
            "owner_id": owner_id,
            "notify": notify,
        }

        # Если все слоты заняты — сообщить позицию ОЖИДАНИЯ.
        async with self._lock:
            self._waiting.append(item)
            position = len(self._waiting)
            slots_busy = self._running >= self.max_concurrent

        if slots_busy and notify and owner_id is not None:
            try:
                await notify(
                    owner_id,
                    f"⏳ В очереди (позиция: {position}) — {title}"
                )
            except Exception:
                pass

        async def _wrapper():
            await self._sem.acquire()
            try:
                # выйти из «ожидающих»
                async with self._lock:
                    if item in self._waiting:
                        self._waiting.remove(item)
                    self._running += 1

                # уведомить о старте
                if notify and owner_id is not None:
                    try:
                        await notify(owner_id, f"▶️ Задача запущена: {title}")
                    except Exception:
                        pass

                try:
                    return await coro_factory()
                except Exception as e:
                    log.warning("TaskQueue: %s failed: %s", title, e)
                    if notify and owner_id is not None:
                        try:
                            await notify(owner_id,
                                         f"❌ {title}: ошибка — {e}")
                        except Exception:
                            pass
                    raise
            finally:
                async with self._lock:
                    self._running = max(0, self._running - 1)
                self._sem.release()

        return asyncio.create_task(_wrapper())
