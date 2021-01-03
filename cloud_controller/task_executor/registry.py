import logging
from queue import Queue, Empty
from typing import Dict, Optional

from cloud_controller.tasks.task import Task


class TaskRegistry:

    def __init__(self):
        self._tasks: Dict[str, Task] = {}
        self._failed_tasks: Dict[str, Task] = {}
        self._task_queue: Queue[Task] = Queue()

    def task_exists(self, task_id: str):
        return task_id in self._tasks

    def add_task(self, task: Task):
        assert task.task_id not in self._tasks
        self._tasks[task.task_id] = task
        self._task_queue.put_nowait(task)

    def return_task(self, task: Task):
        assert self.task_exists(task.task_id)
        self._task_queue.put_nowait(task)

    def get_task(self, task_id: str) -> Optional[Task]:
        if not self.task_exists(task_id):
            return None
        return self._tasks[task_id]

    def next_task(self) -> Optional[Task]:
        while True:
            if self._task_queue.empty():
                return None
            try:
                task = self._task_queue.get_nowait()
                if task.task_id in self._tasks:
                    return task
            except Empty:
                return None

    def complete_task(self, task_id: str):
        assert task_id in self._tasks
        logging.info(f"Task {task_id} is completed")
        del self._tasks[task_id]

    def remove_task(self, task_id: str):
        assert task_id in self._tasks
        logging.info(f"Task {task_id} was removed")
        del self._tasks[task_id]