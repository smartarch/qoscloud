import logging

import yaml
from kubernetes import client

from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import ManagedCompin, Component, CompinPhase
from cloud_controller.planning.k8s_generators import create_deployment_for_compin, create_service_for_compin
from cloud_controller.task_executor.execution_context import KubernetesExecutionContext, ExecutionContext
from cloud_controller.tasks.preconditions import compin_exists, namespace_exists
from cloud_controller.tasks.task import Task


class CreateInstanceTask(Task):
    """
    Creates a Kubernetes deployment and a Kubernetes service for the compin. Adds the compin to the actual state
    with phase=CREATING.
    """

    def __init__(self, namespace: str, instance: ManagedCompin, production: bool):
        super().__init__(
            task_id=self.generate_id()
        )
        self._namespace = namespace
        self._component: Component = instance.component
        self._node_name: str = instance.node_name
        self._instance_id: str = instance.id
        self._deployment = yaml.load(create_deployment_for_compin(instance, not production))
        self._service = yaml.load(create_service_for_compin(self._deployment, instance))
        self._chain_id: str = instance.chain_id
        self._force: bool = instance.force_keep
        self._ip: str = ""
        self.add_precondition(namespace_exists, (self._namespace,))

    def execute(self, context: KubernetesExecutionContext) -> bool:
        context.extensions_api.create_namespaced_deployment(body=self._deployment, namespace=self._namespace)
        logging.info(f"Deployment {self._deployment['metadata']['name']} created")

        api_response = context.basic_api.create_namespaced_service(namespace=self._namespace, body=self._service)
        self._ip = api_response.spec.cluster_ip
        logging.info(f"Service {self._service['metadata']['name']} created")
        return True

    def update_model(self, knowledge: Knowledge) -> None:
        instance = ManagedCompin(
            component=self._component,
            id_=self._instance_id,
            node=self._node_name,
            chain_id=self._chain_id
        )
        if self._force:
            instance.set_force_keep()
        instance.phase = CompinPhase.CREATING
        instance.ip = self._ip
        knowledge.actual_state.add_instance(instance)

    def generate_id(self):
        return f"{self.__class__.__name__}_{self._namespace}_{self._component.application.name}_{self._component.name}_" \
               f"{self._node_name}_{self._instance_id}"


class DeleteInstanceTask(Task):
    """
    Deletes a Kubernetes deployment and a Kubernetes service for the instance. Deletes the instance from the actual
    state. Will perform these actions only if the instance had already reached the FINISHED state.
    :return: True if successful, False if instance is not in FINISHED state yet.
    """

    def __init__(self, namespace: str, instance: ManagedCompin):
        super(DeleteInstanceTask, self).__init__(
            task_id=self.generate_id()
        )
        self._namespace = namespace
        self._instance: ManagedCompin = instance
        self.add_precondition(namespace_exists, (self._namespace,))
        self.add_precondition(compin_exists, (self._instance.id, self._instance.component.name,
                                              self._instance.component.application.name))
        self.add_precondition(DeleteInstanceTask.check_phase_finished, (self._instance,))

    @staticmethod
    def check_phase_finished(_, instance: ManagedCompin):
        if not instance.is_serving:
            return True
        phase = ExecutionContext.ping_instance_ip(instance.ip)
        return phase >= CompinPhase.FINISHED

    def execute(self, context: KubernetesExecutionContext) -> bool:
        # Delete the corresponding deployment:
        options = client.V1DeleteOptions()
        options.propagation_policy = 'Background'
        api_response = context.extensions_api.delete_namespaced_deployment(
            name=self._instance.deployment_name(),
            namespace=self._namespace,
            body=options,
            propagation_policy='Background',
            grace_period_seconds=0
        )
        logging.info(f"Deployment {self._instance.deployment_name()} deleted")
        # Delete the corresponding service:
        api_response = context.basic_api.delete_namespaced_service(
            name=self._instance.service_name(),
            namespace=self._namespace,
            body=options,
            propagation_policy='Background',
            grace_period_seconds=0
        )
        logging.info(f"Service {self._instance.service_name()} deleted")
        return True

    def update_model(self, knowledge: Knowledge) -> None:
        knowledge.actual_state.delete_instance(
            self._instance.component.application.name,
            self._instance.component.name,
            self._instance.id
        )

    def generate_id(self) -> str:
        return f"{self.__class__.__name__}_{self._namespace}_{self._instance.component.application.name}_" \
               f"{self._instance.component.name}_{self._instance.node_name}_{self._instance.id}"


