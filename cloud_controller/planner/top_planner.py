from abc import abstractmethod
from typing import List

from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import CloudState
from cloud_controller.task_executor.registry import TaskRegistry


class Planner:

    def __init__(self, knowledge: Knowledge, task_registry: TaskRegistry):
        self.knowledge: Knowledge = knowledge
        self.task_registry: TaskRegistry = task_registry

    @abstractmethod
    def plan_tasks(self, desired_state: CloudState):
        pass


class TopLevelPlanner(Planner):

    def __init__(self, knowledge: Knowledge, task_registry: TaskRegistry):
        super().__init__(knowledge, task_registry)
        self._planners: List[Planner] = []

    def add_planner(self, planner: Planner):
        self._planners.append(planner)

    def plan_tasks(self, desired_state: CloudState):
        for planner in self._planners:
            planner.plan_tasks(desired_state)
