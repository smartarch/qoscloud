"""
Contains the functions that create Kubernetes YAML descriptors from the provided templates.
"""
import yaml

from cloud_controller import middleware, HOSTNAME_LABEL, DEFAULT_SECRET_NAME, DEFAULT_DOCKER_IMAGE
from cloud_controller.knowledge.instance import ManagedCompin

SERVICE_TEMPLATE = """
    kind: Service
    apiVersion: v1
    metadata:
      name: name
    spec:
      selector:
        deployment: deployment
      ports:
      - name: service-port
        port: 7777
        targetPort: service-port
"""


CONTAINER_TEMPLATE = """
name: container
image: %s
imagePullPolicy: Always
args: []
env:
- name: PYTHONUNBUFFERED
  value: "0"
"""


def get_job_deployment(job_id, container):
    return CONTAINER_TEMPLATE % container


DEPLOYMENT_TEMPLATE = f"""
kind: Deployment
metadata:
  name: name
  labels:
    deployment: name
spec:
  selector:
    matchLabels:
      deployment: name
  template:
    metadata:
      labels:
        deployment: name
    spec:
      containers:
      - name: container
        image: {DEFAULT_DOCKER_IMAGE}
        imagePullPolicy: Always
        args: []
        env:
        - name: PYTHONUNBUFFERED
          value: "0"
"""

def create_deployment_for_compin(compin: ManagedCompin, assessment: bool = False) -> str:
    """
    Creates a Kubernetes deployment YAML descriptor for a provided compin. The compin's deployment template is enhanced
     in the following ways:
        (1) A node selector is added in order to ensure that Kubernetes scheduler deploys this compin in the selected
                node.
        (2) The image pull secret of the application is added to the descriptor so that the image of the compin would
                be downloaded.
        (3) The port for MiddlewareAgent is opened
    :param compin: A compin to create the deployment descriptor for
    :param assessment: Specify if compin will run in assessment cloud
    :return: Kubernetes deployment descriptor converted to string
    """
    deployment = yaml.load(DEPLOYMENT_TEMPLATE)
    if compin.component.container_spec != "":
        deployment['spec']['template']['spec']['containers'][0] = yaml.load(compin.component.container_spec)
    deployment['metadata']['name'] = compin.deployment_name()
    deployment['spec']['template']['metadata']['labels']['deployment'] = compin.deployment_name()
    deployment['spec']['selector']['matchLabels']['deployment'] = compin.deployment_name()
    deployment['metadata']['labels']['deployment'] = compin.deployment_name()
    deployment['spec']['template']['spec']['nodeSelector'] = {}
    deployment['spec']['template']['spec']['nodeSelector'][HOSTNAME_LABEL] = compin.node_name
    deployment['spec']['template']['spec']['imagePullSecrets'] = []
    deployment['spec']['template']['spec']['imagePullSecrets'].append({})
    deployment['spec']['template']['spec']['imagePullSecrets'][0]['name'] = DEFAULT_SECRET_NAME
    if 'ports' not in deployment['spec']['template']['spec']['containers'][0]:
        deployment['spec']['template']['spec']['containers'][0]['ports'] = []
    deployment['spec']['template']['spec']['containers'][0]['ports'].append(
        {
            'containerPort': middleware.AGENT_PORT,
            'name': 'service-port'
        }
    )
    if assessment:
        # Enable SYS_ADMIN capability needed for CPU measuring
        deployment['spec']['template']['spec']['containers'][0]['securityContext'] = {}
        deployment['spec']['template']['spec']['containers'][0]['securityContext']['capabilities'] = {}
        deployment['spec']['template']['spec']['containers'][0]['securityContext']['capabilities']['add'] = [
            "SYS_ADMIN"]
    return yaml.dump(deployment)


def create_service_for_compin(deployment_str: str, compin: ManagedCompin) -> str:
    """
    Creates a Kubernetes service YAML descriptor for a provided compin. Ensures that this service will serve only for
    that compin. Opens all the ports that are needed for that compin.
    :param deployment_str: Kubernetes deployment descriptor for this compin
    :param compin: A compin to create the service descriptor for
    :return: Kubernetes service descriptor converted to string
    """
    service = yaml.load(SERVICE_TEMPLATE)
    deployment = yaml.load(deployment_str)
    # The name of the service can contain only alphanumeric characters or '-', so we need to filter out all the
    # other characters:
    service['metadata']['name'] = compin.service_name()
    service['spec']['selector']['deployment'] = compin.deployment_name()
    for port in deployment['spec']['template']['spec']['containers'][0]['ports']:
        if 'name' not in port or port['name'] != "service-port":
            port_dict = {
                'port': port['containerPort'],
                'targetPort': port['containerPort']
            }
            if 'protocol' in port:
                port_dict['protocol'] = port['protocol']
            name = port['name'] if 'name' in port else "random"
            port_dict['name'] = name
            service['spec']['ports'].append(port_dict)
    return yaml.dump(service)


def add_resource_requirements(template: str, min_memory="", max_memory="",
                              min_cpu="", max_cpu="", k8s_labels="") -> str:
    return template