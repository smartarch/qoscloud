FROM dankhalev/odm-7

RUN apt-get update
RUN apt-get -y install python3-pip
RUN pip3 install grpcio grpcio-tools typing pymongo Pillow python_papi==5.5.1.3 flask requests elasticsearch PyYAML


COPY ./cloud_controller/middleware /root/cloud_controller/middleware
#COPY ./cloud_controller/architecture.proto /root/ivisjob/cloud_controller/architecture.proto
WORKDIR /root
RUN PYTHONPATH=. python3.6 cloud_controller/middleware/generate_grpc.py
RUN chmod +x cloud_controller/middleware/run_middleware_agent.py

ENV LC_ALL="en_US.utf8"
ENV LANG="en_US.utf8"
ENV PYTHONPATH="/usr/local/lib/python3.4/site-packages/:/root"

#COPY ./examples/featuredetection/datasets /datasets

#RUN pip3 install

RUN ln -s /usr/bin/python3 /bin/python3

RUN apt-get install -y time nano

ENTRYPOINT [ "./cloud_controller/middleware/run_middleware_agent.py" ]
