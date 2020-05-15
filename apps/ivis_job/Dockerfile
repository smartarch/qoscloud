FROM centos:7

RUN yum -y update; yum clean all
RUN yum -y install epel-release; yum clean all
RUN yum -y install git python36 python36-devel python36-pip python36-numpy python36-scipy python36-PyYAML python2 python2-devel
RUN pip3.6 install grpcio
RUN pip3.6 install grpcio-tools
RUN pip3.6 install typing
RUN yum -y install git gcc gcc-c++ make cmake
RUN pip3.6 install pymongo Pillow python_papi==5.5.1.3 flask requests elasticsearch


COPY ./cloud_controller/middleware /root/ivisjob/cloud_controller/middleware
COPY ./cloud_controller/architecture.proto /root/ivisjob/cloud_controller/architecture.proto
WORKDIR /root/ivisjob
RUN PYTHONPATH=. python3.6 cloud_controller/middleware/generate_grpc.py
RUN chmod +x cloud_controller/middleware/job_agent.py

ENV LC_ALL="en_US.utf8"
ENV LANG="en_US.utf8"
ENV PYTHONPATH="/usr/local/lib/python3.4/site-packages/:/root/ivisjob"

ENTRYPOINT [ "./cloud_controller/middleware/job_agent.py" ]
