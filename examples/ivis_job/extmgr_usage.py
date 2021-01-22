from cloud_controller.analyzer.csp_analyzer import CSPAnalyzer
from cloud_controller.analyzer.objective_function import ObjectiveFunction
from cloud_controller.analyzer.variables import Variables
from cloud_controller.extension_manager import ExtensionManager
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.monitor.monitor import TopLevelMonitor
from cloud_controller.task_executor.task_executor import TaskExecutor
from cloud_controller.task_executor.execution_context import ExecutionContext


class CustomTask(object):
    pass


class CustomMonitor(TopLevelMonitor):
    pass


class CustomExecutionContext(ExecutionContext):
    pass


class NewObjectiveFunction(ObjectiveFunction):

    def expression(self, variables: Variables):
        pass


# First, we need an instance of the extension manager:
extension_mgr = ExtensionManager()

# Now, let us change the objective function for the CS problem:
analyzer: CSPAnalyzer = extension_mgr.get_default_analyzer()
objective = NewObjectiveFunction()
analyzer.set_objective_function(objective)

# Adding support for new tasks works in the following way:
executor: TaskExecutor = extension_mgr.get_default_executor()
knowledge: Knowledge = extension_mgr.get_default_knowledge()
executor.add_execution_context(CustomExecutionContext(knowledge))
executor.add_task_type(CustomTask, CustomExecutionContext)

# Changing the monitor for a different implementation:
monitor = CustomMonitor(knowledge)
extension_mgr.set_monitor(monitor)

# Getting the customized adaptation loop.
# After this call the extension manager will not allow to
# do any more modifications:
adaptation_loop = extension_mgr.get_adaptation_loop()
adaptation_loop.start()