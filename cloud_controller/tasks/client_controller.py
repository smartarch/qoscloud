import logging

from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.knowledge_pb2 import ClientDependencyDescription, ApplicationName
from cloud_controller.knowledge.model import Component, CompinPhase, ManagedCompin, UnmanagedCompin, Application
from cloud_controller.knowledge.user_equipment import UserEquipmentContainer
from cloud_controller.task_executor.execution_context import ClientControllerExecutionContext
from cloud_controller.tasks.preconditions import namespace_active, check_phase, application_deployed
from cloud_controller.tasks.task import Task


class DeleteApplicationFromCCTask(Task):
    """
    Signals to the client controller that it has to disconnect all the clients in the given application, and delete
    the application from its database.
    """

    def __init__(self, app_name: str):
        self._app_name = app_name
        super(DeleteApplicationFromCCTask, self).__init__(
            task_id=self.generate_id()
        )

    def execute(self, context: ClientControllerExecutionContext) -> bool:
        context.client_controller.CloseApplicationChannels(
            ApplicationName(name=self._app_name)
        )
        logging.info(f"Disconnected all clients of application {self._app_name}")
        return True

    def generate_id(self) -> str:
        return f"{self.__class__.__name__}_{self._app_name}"

    def update_model(self, knowledge: Knowledge) -> None:
        if self._app_name in knowledge.applications:
            knowledge.applications[self._app_name].cc_remove_completed = True


class AddApplicationToCCTask(Task):
    """
    Signals to the client controller that it has to add an application to its database and start accepting new
    clients for that application.
    """

    def __init__(self, app: Application):
        self._app_pb = app.get_pb_representation()
        self._app_name = app.name
        super(AddApplicationToCCTask, self).__init__(
            task_id=self.generate_id()
        )
        self.add_precondition(namespace_active, (self._app_name,))
        self.add_precondition(application_deployed, (self._app_name,))

    def execute(self, context: ClientControllerExecutionContext) -> bool:
        context.client_controller.AddNewApplication(self._app_pb)
        logging.info(f"Sent application descriptor for {self._app_name} app to Client Controller")
        return True

    def generate_id(self) -> str:
        return f"{self.__class__.__name__}_{self._app_name}"

    def update_model(self, knowledge: Knowledge) -> None:
        if self._app_name in knowledge.applications:
            knowledge.applications[self._app_name].cc_add_completed = True


class SetClientDependencyTask(Task):
    """
    Tries to set an IP address of a dependency for a client via Client Controller. If all of the client's
    dependencies have already been moved to a new datacenter, reports the full client handover. Will set the
    dependency only if the compin is ready to accept client connections (i.e. is in READY state)
    :preconditions return: True if successful, False if compin is not in READY state.
    """

    def __init__(self, component: Component, instance_id: str, client_component: Component, client_id: str):
        self._app_name = component.application.name
        self._component = component
        self._instance_id = instance_id
        self._client_component = client_component
        self._client_id = client_id
        super(SetClientDependencyTask, self).__init__(
            task_id=self.generate_id()
        )
        self.add_precondition(check_phase, (self._app_name, self._component.name, self._instance_id, CompinPhase.READY))

    def execute(self, context: ClientControllerExecutionContext) -> bool:
        dependency = ClientDependencyDescription()
        dependency.dependencyAddress.name = self._component.name
        ip = context.get_instance_ip(self._app_name, self._component.name, self._instance_id)
        dependency.dependencyAddress.ip = ip
        dependency.application = self._app_name
        dependency.clientType = self._client_component.name
        dependency.clientId = self._client_id
        context.client_controller.SetClientDependency(dependency),
        logging.info(f"Dependency {self._component.name} for client {self._client_id} set to {ip}.")
        return True

    def update_model(self, knowledge: Knowledge) -> None:
        server = knowledge.actual_state.get_compin(self._app_name, self._component.name, self._instance_id)
        assert isinstance(server, ManagedCompin)
        client = knowledge.actual_state.get_compin(self._app_name, self._client_component.name, self._client_id)
        assert isinstance(client, UnmanagedCompin)
        # If all dependencies are now located in the same DC, we have to report full handover
        self._check_and_report_handover(knowledge, client, server)
        # Finally, change the record in the actual state model
        client.set_dependency(server)

    @staticmethod
    def _check_and_report_handover(knowledge: Knowledge, client: UnmanagedCompin, server: ManagedCompin):
        """
        Checks whether after setting _server_ to be the new dependency of the _client_, all dependencies of the
        client will be located in the same data center. If that is the case, reports the completed handover.
        """
        if len(client.list_dependencies()) == len(client.component.dependencies):
            new_dc_name = knowledge.nodes[server.node_name].data_center
            all_switched = True
            for compin in client.list_dependencies():
                dc_name = knowledge.nodes[compin.node_name].data_center
                if dc_name != new_dc_name:
                    all_switched = False
                    break
            if all_switched:
                old_dependency = client.get_dependency(server.component.name)
                if old_dependency is not None:
                    old_dc_name = knowledge.nodes[old_dependency.node_name].data_center
                else:
                    old_dc_name = ""
                UserEquipmentContainer.report_handover(client.ip, client.component.application.name,
                                                       client.component.name, old_dc_name, new_dc_name)

    def generate_id(self) -> str:
        return f"{self.__class__.__name__}_{self._app_name}_{self._client_component.name}_" \
               f"{self._client_id}_{self._component.name}_{self._instance_id}"
