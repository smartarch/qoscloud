from abc import abstractmethod
from typing import List, Callable, Tuple

from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.task_executor.execution_context import ExecutionContext


class Task:

    class_preconditions: List[Tuple[Callable, Tuple]] = []

    def __init__(self, task_id):
        self.task_id: str = task_id
        self.preconditions: List[Tuple[Callable, Tuple]] = []
        for precondition, args in self.__class__.class_preconditions:
            self.add_precondition(precondition, args)

    def check_preconditions(self, knowledge: Knowledge) -> bool:
        for precondition, args in self.preconditions:
            if not precondition(knowledge, *args):
                return False
        return True

    def add_precondition(self, precondition: Callable, args: Tuple) -> None:
        self.preconditions.append((precondition, args))

    @classmethod
    def add_precondition_static(cls, precondition: Callable, args: Tuple) -> None:
        cls.class_preconditions.append((precondition, args))

    @abstractmethod
    def execute(self, context: ExecutionContext) -> bool:
        pass

    def update_model(self, knowledge: Knowledge) -> None:
        pass

    @abstractmethod
    def generate_id(self) -> str:
        pass


