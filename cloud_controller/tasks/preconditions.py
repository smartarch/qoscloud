from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import NamespacePhase, CompinPhase, ManagedCompin
from cloud_controller.task_executor.execution_context import ExecutionContext


def dependency_is_set(knowledge: Knowledge, app_name: str, component_name: str, instance_id: str,
                      dependency_name: str, dependency_value: str) -> bool:
    instance = knowledge.actual_state.get_compin(app_name, component_name, instance_id)
    if instance is None:
        return False
    return instance.get_dependency(dependency_name).id == dependency_value


def compin_exists(knowledge: Knowledge, app: str, component: str, id_: str):
    return knowledge.actual_state.compin_exists(app, component, id_)


def namespace_exists(knowledge: Knowledge, ns: str) -> bool:
    return ns in knowledge.namespaces


def namespace_active(knowledge: Knowledge, ns: str) -> bool:
    if ns in knowledge.namespaces:
        return knowledge.namespaces[ns].phase == NamespacePhase.ACTIVE
    return False


def check_phase(knowledge: Knowledge, app_name:str, component_name: str, instance_id: str,
                required_phase: CompinPhase) -> bool:
    instance = knowledge.actual_state.get_compin(app_name, component_name, instance_id)
    if instance is None:
        return False
    assert isinstance(instance, ManagedCompin)
    instance.phase = ExecutionContext.ping_instance_ip(instance.ip)
    return instance.phase >= required_phase


def application_deployed(knowledge: Knowledge, app_name: str):
    return knowledge.actual_state.contains_application(app_name)