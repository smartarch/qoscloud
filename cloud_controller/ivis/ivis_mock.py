import logging
import time

from threading import Thread

from cloud_controller import IVIS_CORE_IP, IVIS_CORE_PORT
from cloud_controller.architecture_pb2 import Architecture, ApplicationType
from cloud_controller.ivis.ivis_data import SAMPLE_JOB_CODE, SAMPLE_JOB_PARAMETERS, SAMPLE_JOB_INTERVAL, \
    SAMPLE_JOB_CONFIG, SAMPLE_JOB_CREATE_SS_RESPONSE
from cloud_controller.middleware import AGENT_HOST, AGENT_PORT
from cloud_controller.middleware.helpers import connect_to_grpc_server, setup_logging
from cloud_controller.middleware.ivis_pb2 import JobAdmissionStatus, JobStatus, JobID, RunParameters, JobDescriptor
from cloud_controller.middleware.ivis_pb2_grpc import IvisInterfaceStub, JobMiddlewareAgentStub
from cloud_controller.middleware.job_agent import IVIS_HOST, IVIS_PORT

from flask import Flask, request, g, json

app = Flask(__name__)
state = None

IVIS_INTERFACE_HOST = "0.0.0.0"
IVIS_INTERFACE_PORT = 62533
testing_job_agent = False

state_ = None

with app.app_context() as ctx:
    g.state = None
    ctx.push()


def log_run_status(run_status):
    logging.info(f"Run {run_status['runId']} of the job {run_status['jobId']} was completed successfully in "
                 f"{run_status['endTime'] - run_status['startTime']} with return code {run_status['returnCode']}")
    logging.info("-----------------------------------------------------------------------------\nSTDOUT:")
    print(run_status['output'])
    logging.info("-----------------------------------------------------------------------------\nSTDERR:")
    print(run_status['error'])


@app.route("/ccapi/run-request", methods=['POST'])
def run_request():
    global state_
    request_ = request.json
    print(request_)
    request_json = json.loads(request_['request'])
    if 'type' in request_json and request_json['type'] == "sets":
        logging.info(f"Received signal set creation request from job {request_['jobId']}. Request:\n{request_['jobId']}")
        if state_ is None:
            state_ = json.loads(SAMPLE_JOB_CREATE_SS_RESPONSE)
        return json.jsonify({'response': json.dumps(state_)})
    elif 'type' in request_json and request_json['type'] == "store":
        logging.info(f"Received store state request from job {request_['jobId']}. Request:\n{request_['jobId']}")
        state_ = request_json['state']
        return json.jsonify({'response': json.dumps({"id": request_json['id']})})
    else:
        logging.info(f"Received incorrect runtime request from job {request_['jobId']}. Request:\n{request_['jobId']}")
        return json.jsonify({'response': json.dumps({"error": "Incorrect request format"})})


@app.route("/ccapi/on-success", methods=['POST'])
def on_success():
    run_status = request.json
    log_run_status(run_status)
    return "200 OK"


@app.route("/ccapi/on-fail", methods=['POST'])
def on_fail():
    run_status = request.json
    log_run_status(run_status)
    return "200 OK"

# TODO: implement /ccapi/on-fail

if __name__ == "__main__":
    setup_logging()
    ivis_core_thread = Thread(target=app.run, args=(IVIS_INTERFACE_HOST, IVIS_PORT), daemon=True)
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
            minimal_interval=SAMPLE_JOB_INTERVAL,
            ivis_core_ip=IVIS_CORE_IP,
            ivis_core_port=IVIS_CORE_PORT
        ))
        logging.info("Job has been initialized")
    # Run the job 4 times
    for i in range(4):
        logging.info(f"Executing run {i}")
        ivis_interface.RunJob(RunParameters(job_id="ivisjob", run_id=f"run{i}",
                                            state=json.dumps(state)))
        time.sleep(SAMPLE_JOB_INTERVAL)
    if not testing_job_agent:
        # Unschedule the job
        ivis_interface.UnscheduleJob(JobID(job_id="ivisjob"))
        logging.info("Job has been unscheduled")
        status = JobStatus()
        while status.status != JobAdmissionStatus.Value("NOT_PRESENT"):
            status = ivis_interface.GetJobStatus(JobID(job_id="ivisjob"))
            logging.info(f"Current job admission status: {JobAdmissionStatus.Name(status.status)}")
