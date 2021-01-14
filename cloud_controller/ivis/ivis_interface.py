import logging
from typing import Dict, List

from cloud_controller.architecture_pb2 import Architecture, Cardinality, ComponentType
from cloud_controller.assessment import CTL_HOST, CTL_PORT
from cloud_controller.assessment.deploy_controller_pb2 import AppName, AppAdmissionStatus
from cloud_controller.assessment.deploy_controller_pb2_grpc import DeployControllerStub
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import ManagedCompin, CompinPhase, add_resource_requirements
from cloud_controller.planner.k8s_generators import JOB_DEPLOYMENT_TEMPLATE, get_job_deployment
from cloud_controller.middleware import AGENT_PORT
from cloud_controller.middleware.helpers import connect_to_grpc_server
from cloud_controller.ivis.ivis_pb2 import SubmissionAck, JobStatus, JobAdmissionStatus, UnscheduleJobAck, \
    RunJobAck, AccessTokenAck
from cloud_controller.ivis.ivis_pb2_grpc import IvisInterfaceServicer
from cloud_controller.middleware.middleware_pb2 import RunParameters
from cloud_controller.middleware.middleware_pb2_grpc import MiddlewareAgentStub


class IvisInterface(IvisInterfaceServicer):

    def __init__(self, knowledge: Knowledge):
        self._deploy_controller: DeployControllerStub = connect_to_grpc_server(DeployControllerStub, CTL_HOST, CTL_PORT)
        self._knowledge: Knowledge = knowledge
        self._templates: List[str] = []
        self._jobs: Dict[str, Architecture] = {}

    def _compose_application_pb(self, request):
        job_name = request.job_id
        application_pb = Architecture()
        application_pb.name = job_name
        application_pb.access_token = self._knowledge.api_endpoint_access_token
        application_pb.is_complete = False
        application_pb.components[job_name].name = job_name
        template = get_job_deployment(request.job_id, request.docker_container)
        template = add_resource_requirements(
            template=template,
            min_memory=request.min_memory,
            max_memory=request.max_memory,
            min_cpu=request.min_cpu,
            max_cpu=request.max_cpu,
            k8s_labels=request.k8s_labels
        )
        application_pb.components[job_name].deployment = template
        application_pb.components[job_name].cardinality = Cardinality.Value('SINGLE')
        application_pb.components[job_name].type = ComponentType.Value('MANAGED')
        probe = application_pb.components[job_name].probes.add()
        probe.name = job_name
        probe.application = job_name
        probe.component = job_name
        probe.code = request.code
        probe.config = request.config
        probe.signal_set = request.signal_set
        probe.execution_time_signal = request.execution_time_signal
        probe.run_count_signal = request.run_count_signal
        return application_pb

    def _create_app_from_template(self, request, old_name: str, new_name: str):
        app = self._knowledge.applications[old_name]
        old_application_pb = app.get_pb_representation()
        application_pb = Architecture()
        application_pb.CopyFrom(old_application_pb)
        application_pb.name = new_name
        application_pb.components[new_name].CopyFrom(application_pb.components[old_name])
        application_pb.components[new_name].name = new_name
        del application_pb.components[old_name]
        probe = application_pb.components[new_name].probes[0]
        probe.name = new_name
        probe.application = new_name
        probe.component = new_name
        probe.signal_set = request.signal_set
        probe.execution_time_signal = request.execution_time_signal
        probe.run_count_signal = request.run_count_signal
        self._knowledge.new_apps.put_nowait(application_pb)

    def SubmitJob(self, request, context):
        if self._knowledge.api_endpoint_access_token is None:
            logging.error(f"Cannot deploy a job due to the absence of an access token")
            return SubmissionAck(success=False)
        if request.template_job_id is not None and request.template_job_id != "":
            self._templates.append(request.job_id)
            self._create_app_from_template(request, request.template_job_id, request.job_id)
        else:
            application_pb = self._compose_application_pb(request)
            self._jobs[application_pb.name] = application_pb
            self._deploy_controller.SubmitArchitecture(application_pb)
            logging.info(f"Job {request.job_id} was accepted for measurements")
        return SubmissionAck(success=True)

    def DeployJob(self, request, context):
        job_id = request.job_id
        contract = self._jobs[job_id].components[job_id].probes[0].requirements.add()
        if request.type == 0:
            contract.time.time = request.time
            contract.time.percentile = request.percentile
        elif request.type == 1:
            contract.throughput.requests = request.throughput
            contract.throughput.per = request.per
        self._deploy_controller.SubmitRequirements(self._jobs[job_id])
        return SubmissionAck(success=True)

    def GetJobStatus(self, request, context):
        job_id = request.job_id
        if job_id in self._knowledge.applications:
            if job_id in self._knowledge.unique_components_without_resources:
                return JobStatus(status=JobAdmissionStatus.Value('NO_RESOURCES'))
            job_compin = self._knowledge.actual_state.get_job_compin(job_id)
            assert job_compin is None or isinstance(job_compin, ManagedCompin)
            if job_compin is not None and job_compin.phase == CompinPhase.READY:
                return JobStatus(status=JobAdmissionStatus.Value('DEPLOYED'))
            else:
                return JobStatus(status=JobAdmissionStatus.Value('ACCEPTED'))
        elif job_id in self._templates:
            return JobStatus(status=JobAdmissionStatus.Value('ACCEPTED'))
        else:
            status = self._deploy_controller.GetApplicationStats(AppName(name=job_id))
            if status.status == AppAdmissionStatus.Value('UNKNOWN'):
                return JobStatus(status=JobAdmissionStatus.Value('NOT_PRESENT'))
            elif status.status == AppAdmissionStatus.Value('RECEIVED'):
                return JobStatus(status=JobAdmissionStatus.Value('MEASURING'))
            elif status.status == AppAdmissionStatus.Value('REJECTED'):
                return JobStatus(status=JobAdmissionStatus.Value('REJECTED'))
            elif status.status == AppAdmissionStatus.Value('ACCEPTED') or \
                    status.status == AppAdmissionStatus.Value('PUBLISHED'):
                return JobStatus(status=JobAdmissionStatus.Value('ACCEPTED'))
            elif status.status == AppAdmissionStatus.Value('MEASURED'):
                return JobStatus(status=JobAdmissionStatus.Value('MEASURED'))
            else:
                raise RuntimeError(f"App admission status {status.status} is not supported.")

    def RunJob(self, request, context):
        job_compin = self._knowledge.actual_state.get_unique_compin(request.job_id)
        if job_compin is None or job_compin.phase != CompinPhase.READY:
            return RunJobAck()
        logging.info(f"Running job {job_compin.id}")
        params = RunParameters(
            instance_id=request.job_id,
            probe_id=request.job_id,
            run_id=request.run_id,
            state=request.state
        )
        job_agent: MiddlewareAgentStub = connect_to_grpc_server(MiddlewareAgentStub, job_compin.ip, AGENT_PORT)
        return job_agent.RunProbe(params)

    def UnscheduleJob(self, request, context):
        self._deploy_controller.DeleteApplication(AppName(name=request.job_id))
        return UnscheduleJobAck()

    def UpdateAccessToken(self, request, context):
        assessment_response = self._deploy_controller.UpdateAccessToken(request)
        if not assessment_response.success:
            logging.error(f"Cannot update the access token due to the jobs already being measured")
            return AccessTokenAck(success=False)
        if self._knowledge.there_are_applications():
            logging.error(f"Cannot update the access token due to the jobs already deployed")
            return AccessTokenAck(success=False)
        self._knowledge.update_access_token(request.token)
        logging.info(f"Access token was updated successfully")
        return AccessTokenAck(success=True)