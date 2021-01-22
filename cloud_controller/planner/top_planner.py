"""
This module contains the Planner interface and the TopLevelPlanner class which is responsible for the _planning_
phase of the adaptation process.
"""
from abc import abstractmethod
from typing import List, Dict, Set

from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import CloudState
from cloud_controller.task_executor.registry import TaskRegistry
from cloud_controller.tasks.task import Task


class Planner:

    def __init__(self, knowledge: Knowledge, task_registry: TaskRegistry):
        self.knowledge: Knowledge = knowledge
        self.task_registry: TaskRegistry = task_registry
        self._current_round: Set[str] = set()
        self._last_round: Set[str] = set()

    def _create_task(self, task: Task):
        self._current_round.add(task.task_id)
        self.task_registry.add_task(task)

    def _complete_planning(self):
        for task_id in self._last_round:
            if task_id not in self._current_round:
                self.task_registry.cancel_task(task_id)
        self._last_round = self._current_round
        self._current_round = set()

    @abstractmethod
    def plan_tasks(self, desired_state: CloudState):
        pass


class TopLevelPlanner(Planner):

    def __init__(self, knowledge: Knowledge, task_registry: TaskRegistry):
        """
        This class handles creating the executable tasks based on the differences between the actual and the
        desired states with its plan_tasks method. The actual state is taken from the Knowledge. For the different types
        of task that exist refer to the documentation task module.

        Attributes:
            knowledge:  reference to the Knowledge
            task_registry: refernce to task registry
        """
        super().__init__(knowledge, task_registry)
        self._planners: List[Planner] = []

    def add_planner(self, planner: Planner):
        self._planners.append(planner)

    def plan_tasks(self, desired_state: CloudState):
        for planner in self._planners:
            planner.plan_tasks(desired_state)
            planner._complete_planning()
