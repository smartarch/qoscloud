from multiprocessing.pool import Pool, ApplyResult
from typing import List, Dict, Type

from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.task_executor.execution_context import ExecutionContext
from cloud_controller.tasks.task import Task
from cloud_controller.task_executor.registry import TaskRegistry


class TaskExecutor:

    def __init__(self, knowledge: Knowledge, registry: TaskRegistry, pool: Pool):
        self._knowledge: Knowledge = knowledge
        self._registry: TaskRegistry = registry
        self._pool: Pool = pool
        self._futures: List[ApplyResult] = []
        self._execution_contexts: Dict[Type, ExecutionContext] = {}
        self._contexts_by_task_type: Dict[Type, ExecutionContext] = {}

    def add_execution_context(self, executor: ExecutionContext) -> None:
        self._execution_contexts[executor.__class__] = executor

    def add_task_type(self, task_type: Type, context_type: Type):
        self._contexts_by_task_type[task_type] = self._execution_contexts[context_type]

    def execute_all(self) -> int:
        count = 0
        task = self._registry.next_task()
        while task is not None:
            result = self._pool.apply_async(self.execute, (task,))
            self._futures.append(result)
            count += 1
        return count

    def print_errors(self):
        for result in self._futures:
            if result.ready():
                if not result.successful():
                    result.get()
                else:
                    self._futures.remove(result)

    def execute(self, task: Task):
        if not task.check_preconditions(self._knowledge):
            self._registry.return_task(task)
            return
        context = self._contexts_by_task_type[task.__class__]
        task.execute(context)
        task.update_model(self._knowledge)
        self._registry.complete_task(task.task_id)