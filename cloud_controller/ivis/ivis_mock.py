import logging
import time

import json

from threading import Thread

from cloud_controller.architecture_pb2 import Architecture, ApplicationType
from cloud_controller.ivis.ivis_data import SAMPLE_JOB_CODE, SAMPLE_JOB_PARAMETERS, SAMPLE_JOB_INTERVAL, \
    SAMPLE_JOB_CONFIG, SAMPLE_JOB_CREATE_SS_RESPONSE
from cloud_controller.middleware import AGENT_HOST, AGENT_PORT
from cloud_controller.middleware.helpers import connect_to_grpc_server, setup_logging, start_grpc_server
from cloud_controller.middleware.ivis_pb2 import JobAdmissionStatus, JobStatus, JobID, RunParameters, RunFinalStateAck, \
    RunStatus, RunResponse, JobDescriptor
from cloud_controller.middleware.ivis_pb2_grpc import IvisCoreServicer, IvisInterfaceStub, \
    add_IvisCoreServicer_to_server, JobMiddlewareAgentStub

# message RunStatus {
#     enum Status {
#         RUNNING = 0;
#         COMPLETED = 1;
#         FAILED = 2;
#     }
#     string run_id = 1;
#     Status status = 2;
#     double start_time = 3;
#     double end_time = 4;
#     string stdout = 5;
#     string stderr = 6;
#     int32 return_code = 7;
#     string job_id = 8;
#     string config = 9; // this field is not used in the current IVIS implementation
# }
from cloud_controller.middleware.job_agent import IVIS_CORE_HOST, IVIS_CORE_PORT


IVIS_INTERFACE_HOST = "0.0.0.0"
IVIS_INTERFACE_PORT = 7766


class IvisCoreMock(IvisCoreServicer):

    def __init__(self):
        self.state = None

    def HandleRunRequest(self, request, context):
        request_json = json.loads(request.request)
        if 'type' in request_json and request_json['type'] == "sets":
            logging.info(f"Received signal set creation request from job {request.job_id}. Request:\n{request.request}")
            if self.state is None:
                self.state = json.loads(SAMPLE_JOB_CREATE_SS_RESPONSE)
            return RunResponse(response=json.dumps(self.state))
        elif 'type' in request_json and request_json['type'] == "store":
            logging.info(f"Received store state request from job {request.job_id}. Request:\n{request.request}")
            self.state = request_json['state']
            return RunResponse(response=json.dumps(
                {"id": request_json['id']}
            ))
        else:
            logging.info(f"Received incorrect runtime request from job {request.job_id}. Request:\n{request.request}")
            return RunResponse(response=json.dumps(
                {"error": "Incorrect request format"}
            ))

    @staticmethod
    def log_run_status(run_status):
        logging.info(f"Run {run_status.run_id} of the job {run_status.job_id} was completed successfully in "
                     f"{run_status.end_time - run_status.start_time} with return code {run_status.return_code}")
        logging.info("-----------------------------------------------------------------------------\nSTDOUT:")
        print(run_status.stdout)
        logging.info("-----------------------------------------------------------------------------\nSTDERR:")
        print(run_status.stderr)

    def OnSuccess(self, run_status: RunStatus, context):
        self.log_run_status(run_status)
        return RunFinalStateAck()

    def OnFail(self, run_status: RunStatus, context):
        self.log_run_status(run_status)
        return RunFinalStateAck()


# message Architecture {
#     string name = 1;
#     map<string, Component> components = 2;
#     Secret secret = 3;
#     ApplicationType type = 4;
#     string job_id = 5;
#     string code = 6;
#     string parameters = 7;
#     string config = 8;
#     int32 minimal_interval = 9;
# }

testing_job_agent = False


if __name__ == "__main__":
    setup_logging()
    ivis_core_mock = IvisCoreMock()
    ivis_core_thread = Thread(target=start_grpc_server, args=(ivis_core_mock, add_IvisCoreServicer_to_server,
                                                              IVIS_CORE_HOST, IVIS_CORE_PORT, 10, True))
    ivis_core_thread.start()
    if not testing_job_agent:
        ivis_interface: IvisInterfaceStub = connect_to_grpc_server(IvisInterfaceStub,
                                                                   IVIS_INTERFACE_HOST, IVIS_INTERFACE_PORT)
        # Submit the job
        ivis_interface.SubmitJob(
            Architecture(
                name="ivisjob",
                type=ApplicationType.Value("IVIS"),
                job_id="ivisjob",
                code=SAMPLE_JOB_CODE,
                parameters=SAMPLE_JOB_PARAMETERS,
                config=json.dumps(json.loads(SAMPLE_JOB_CONFIG)),
                minimal_interval=SAMPLE_JOB_INTERVAL
            )
        )
        logging.info("Job has been submitted")
        # Check the job status until DEPLOYED
        status = JobStatus()
        while status.status != JobAdmissionStatus.Value("DEPLOYED"):
            status = ivis_interface.GetJobStatus(JobID(job_id="ivisjob"))
            logging.info(f"Current job admission status: {JobAdmissionStatus.Name(status.status)}")
            time.sleep(1)
    else:
        ivis_interface: JobMiddlewareAgentStub = connect_to_grpc_server(JobMiddlewareAgentStub,
                                                                        AGENT_HOST, AGENT_PORT)
        ivis_interface.InitializeJob(JobDescriptor(
            job_id="ivisjob",
            code=SAMPLE_JOB_CODE,
            parameters=SAMPLE_JOB_PARAMETERS,
            config=json.dumps(json.loads(SAMPLE_JOB_CONFIG)),
            minimal_interval=SAMPLE_JOB_INTERVAL
        ))
        logging.info("Job has been initialized")
    # Run the job 4 times
    for i in range(4):
        logging.info(f"Executing run {i}")
        ivis_interface.RunJob(RunParameters(job_id="ivisjob", run_id=f"run{i}",
                                            state=json.dumps(ivis_core_mock.state)))
        time.sleep(SAMPLE_JOB_INTERVAL)
    if not testing_job_agent:
        # Unschedule the job
        ivis_interface.UnscheduleJob(JobID(job_id="ivisjob"))
        logging.info("Job has been unscheduled")
        status = JobStatus()
        while status.status != JobAdmissionStatus.Value("NOT_PRESENT"):
            status = ivis_interface.GetJobStatus(JobID(job_id="ivisjob"))
            logging.info(f"Current job admission status: {JobAdmissionStatus.Name(status.status)}")
