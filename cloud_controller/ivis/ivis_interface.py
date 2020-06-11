import logging

from cloud_controller.assessment import CTL_HOST, CTL_PORT
from cloud_controller.assessment.deploy_controller_pb2 import AppName, AppAdmissionStatus
from cloud_controller.assessment.deploy_controller_pb2_grpc import DeployControllerStub
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.knowledge.model import ManagedCompin, CompinPhase
from cloud_controller.middleware import AGENT_PORT
from cloud_controller.middleware.helpers import connect_to_grpc_server
from cloud_controller.middleware.ivis_pb2 import SubmissionAck, JobStatus, JobAdmissionStatus, UnscheduleJobAck, \
    RunJobAck, AccessTokenAck
from cloud_controller.middleware.ivis_pb2_grpc import IvisInterfaceServicer, JobMiddlewareAgentStub


class IvisInterface(IvisInterfaceServicer):

    def __init__(self, knowledge: Knowledge):
        self._deploy_controller: DeployControllerStub = connect_to_grpc_server(DeployControllerStub, CTL_HOST, CTL_PORT)
        self._knowledge: Knowledge = knowledge

    def SubmitJob(self, request, context):
        if self._knowledge.ivis_access_token is None:
            logging.error(f"Cannot deploy a job due to the absence of an access token")
            return SubmissionAck(success=False)
        self._deploy_controller.SubmitArchitecture(request)
        logging.info(f"Job {request.job_id} was accepted for measurements")
        return SubmissionAck(success=True)

    def GetJobStatus(self, request, context):
        job_id = request.job_id
        if job_id in self._knowledge.applications:
            if job_id in self._knowledge.jobs_without_resources:
                return JobStatus(status=JobAdmissionStatus.Value('NO_RESOURCES'))
            job_compin = self._knowledge.actual_state.get_job_compin(job_id)
            assert job_compin is None or isinstance(job_compin, ManagedCompin)
            if job_compin is not None and job_compin.phase == CompinPhase.READY:
                return JobStatus(status=JobAdmissionStatus.Value('DEPLOYED'))
            else:
                return JobStatus(status=JobAdmissionStatus.Value('ACCEPTED'))
        else:
            status = self._deploy_controller.GetApplicationStats(AppName(name=job_id))
            if status.status == AppAdmissionStatus.Value('UNKNOWN'):
                return JobStatus(status=JobAdmissionStatus.Value('NOT_PRESENT'))
            elif status.status == AppAdmissionStatus.Value('RECEIVED'):
                return JobStatus(status=JobAdmissionStatus.Value('MEASURING'))
            elif status.status == AppAdmissionStatus.Value('REJECTED'):
                return JobStatus(status=JobAdmissionStatus.Value('REJECTED'))
            else:
                return JobStatus(status=JobAdmissionStatus.Value('ACCEPTED'))

    def RunJob(self, request, context):
        job_compin = self._knowledge.actual_state.get_job_compin(request.job_id)
        if job_compin is None or job_compin.phase != CompinPhase.READY:
            return RunJobAck()
        job_agent: JobMiddlewareAgentStub = connect_to_grpc_server(JobMiddlewareAgentStub, job_compin.ip, AGENT_PORT)
        return job_agent.RunJob(request)

    def UnscheduleJob(self, request, context):
        self._deploy_controller.DeleteApplication(AppName(name=request.job_id))
        return UnscheduleJobAck()

    def UpdateAccessToken(self, request, context):
        if self._knowledge.there_are_jobs():
            logging.error(f"Cannot update the access token due to the jobs already deployed")
            return AccessTokenAck(success=False)
        else:
            self._knowledge.update_access_token(request.token)
            logging.info(f"Access token was updated successfully")
            return AccessTokenAck(success=True)