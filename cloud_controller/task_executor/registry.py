import logging
from enum import Enum
from queue import Queue, Empty
from threading import RLock
from typing import Dict, Iterable

from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.monitor.monitor import Monitor
from cloud_controller.tasks.task import Task


class TaskState(Enum):
    PENDING = 1
    RUNNING = 2
    COMPLETED = 3
    CANCELED = 5


class TaskRegistry(Monitor):
    """
    Responsible for managing the tasks related to the planing and execution phases. Preserves the
    integrity of the task system (this is important since the tasks run asynchronously and their
    effects may influence the rest of the adaptation loop.
    """

    def __init__(self, knowledge: Knowledge):
        super().__init__(knowledge)
        self._lock = RLock()
        self._knowledge = knowledge
        self._tasks: Dict[str, Task] = {}
        self._failed_tasks: Dict[str, Task] = {}
        self._pending_queue: Queue[Task] = Queue()
        self._completed_queue: Queue[Task] = Queue()
        self._task_states: Dict[str, TaskState] = {}

    def task_count(self) -> int:
        """
        :return: Number of tasks currently present in the registry.
        """
        return len(self._tasks)

    def monitor(self) -> None:
        """
        This method is called during the monitoring phase in order to perform the modifications to
        the Knowledge corresponding to the effects of executed tasks.
        """
        while True:
            try:
                task = self._completed_queue.get_nowait()
                task.update_model(self._knowledge)
                logging.info(f"Task {task.task_id} was removed after model update")
                self._remove_task(task.task_id)
            except Empty:
                return

    def add_task(self, task: Task) -> None:
        """
        Adds a task into the registry, if it is not present yet.
        Called by Planner
        :param task: A task to add
        """
        if not self._task_exists(task.task_id):
            self._tasks[task.task_id] = task
            self._task_states[task.task_id] = TaskState.PENDING
            self._pending_queue.put_nowait(task)
            logging.info(f"Task {task.task_id} was added to the registry")
        else:
            with self._lock:
                if self._get_state(task.task_id) == TaskState.CANCELED:
                    self._set_state(task.task_id, TaskState.RUNNING)

    def cancel_task(self, task_id: str):
        """
        If a task is pending, deletes it. Otherwise, puts it into the canceled state.
        Called by Planner.
        :param task_id: task id
        """
        if self._task_exists(task_id):
            state = self._get_state(task_id)
            with self._lock:
                if state == TaskState.RUNNING:
                    self._set_state(task_id, TaskState.CANCELED)
            if state == TaskState.PENDING:
                self._remove_task(task_id)
            else:
                assert state != TaskState.CANCELED
        logging.info(f"Task {task_id} was canceled")

    def stream_tasks(self) -> Iterable[Task]:
        """
        Called by executor (synchronously)
        :return: Stream of pending tasks
        """
        count = self._pending_queue.qsize()
        for _ in range(0, count):
            try:
                task = self._pending_queue.get_nowait()
                if self._task_exists(task.task_id):
                    assert self._get_state(task.task_id) == TaskState.PENDING
                    self._set_state(task.task_id, TaskState.RUNNING)
                    logging.info(f"Task {task.task_id} was submitted for execution")
                    yield task
            except Empty:
                yield
                return

    def return_task(self, task_id: str):
        """
        Puts a task back into pending state, if its preconditions have failed.
        Called by executor (asynchronously).
        :param task_id:
        :return:
        """
        assert self._task_exists(task_id)
        with self._lock:
            if self._get_state(task_id) == TaskState.CANCELED:
                self._remove_task(task_id)
            else:
                assert self._get_state(task_id) == TaskState.RUNNING
                self._set_state(task_id, TaskState.PENDING)
                self._pending_queue.put_nowait(self._tasks[task_id])
        logging.info(f"Task {task_id} was returned back to the registry")

    def complete_task(self, task_id: str):
        """
        Marks the task as completed.
        Called by executor (asynchronously).
        :param task_id: task id
        """
        assert self._task_exists(task_id)
        state = self._get_state(task_id)
        assert state == TaskState.RUNNING or state == TaskState.CANCELED
        self._set_state(task_id, TaskState.COMPLETED)
        self._completed_queue.put_nowait(self._tasks[task_id])
        logging.info(f"Task {task_id} was completed successfully")

    def _task_exists(self, task_id: str):
        return task_id in self._tasks

    def _set_state(self, task_id: str, state: TaskState):
        self._task_states[task_id] = state

    def _get_state(self, task_id: str):
        return self._task_states[task_id]

    def _remove_task(self, task_id: str):
        assert task_id in self._tasks
        del self._tasks[task_id]
        del self._task_states[task_id]
        logging.debug(f"Task {task_id} was removed")
