"""
Contains the TaskExecutor class responsible for the execution phase.
"""
from typing import List, Dict, Type

from multiprocessing.pool import ApplyResult, ThreadPool

import logging

from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.task_executor.execution_context import ExecutionContext
from cloud_controller.tasks.task import Task
from cloud_controller.task_executor.registry import TaskRegistry


class TaskExecutor:
    """
    Executes the tasks created by Planner. Can execute the tasks sequentially or in parallel (with thread
    pool, default way). Applies its _execute method for each task.

    Attributes:
        knowledge           A reference to the Knowledge
        pool                ThreadPool shared by all PlanExecutor instances
    """

    def __init__(self, knowledge: Knowledge, registry: TaskRegistry, pool: ThreadPool):
        """
        :param knowledge: A reference to the Knowledge
        :param pool: ThreadPool shared with Analyzer instances
        :param registry: task registry
        """
        self._knowledge: Knowledge = knowledge
        self._registry: TaskRegistry = registry
        self._pool: ThreadPool = pool
        self._futures: List[ApplyResult] = []
        self._execution_contexts: Dict[Type, ExecutionContext] = {}
        self._contexts_by_task_type: Dict[Type, ExecutionContext] = {}

    def add_execution_context(self, executor: ExecutionContext) -> None:
        self._execution_contexts[executor.__class__] = executor

    def add_task_type(self, task_type: Type, context_type: Type):
        self._contexts_by_task_type[task_type] = self._execution_contexts[context_type]

    def execute_all(self) -> int:
        """
        Submits all pending tasks for execution.
        Executes all the tasks in parallel with a thread pool.
        :return: Number of tasks submitted
        """
        count = 0
        for task in self._registry.stream_tasks():
            if task is not None:
                result = self._pool.apply_async(self._execute, (task,))
                self._futures.append(result)
                count += 1
        self._print_errors()
        return count

    def _print_errors(self):
        """
        Will crash the whole system if a single task fails. (default behavior, needed for debugging purposes)
        """
        for result in self._futures:
            if result.ready():
                if not result.successful():
                    result.get()
                else:
                    self._futures.remove(result)

    def _execute(self, task: Task):
        """
        Handles the process of task execution
        """
        logging.basicConfig(level=logging.INFO)
        if task.check_preconditions(self._knowledge):
            context = self._contexts_by_task_type[task.__class__]
            task.execute(context)
            self._registry.complete_task(task.task_id)
        else:
            self._registry.return_task(task.task_id)


class FakeExecutor(TaskExecutor):
    """
    A fake Executor for testing. Instead of executing the plans, it just prints them.
    """
    def execute_all(self):
        for task in self._registry.stream_tasks():
            if task is not None:
                print(task.task_id)
        return 0
