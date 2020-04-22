STANDALONE = False

if STANDALONE:
    from cloud_controller.middleware.helpers import setup_logging
    from cloud_controller.middleware.ivis_pb2 import RunRequest
    from cloud_controller.ivis.ivis_mock import IvisCoreMock
    from cloud_controller.ivis.ivis_data import SAMPLE_JOB_CONFIG
    setup_logging()
    ivis_core = IvisCoreMock()

from threading import RLock, Thread
from time import perf_counter, sleep
from typing import Dict, List


class Elasticsearch:

    def __init__(self, config):
        print(f"Connected to Elastic Search at {config[0]['host']}:{config[0]['port']}")
        self.items: List[Dict[str, Dict]] = []
        self.means: List[Dict[str, float]] = []
        self.last_index = 0
        self.last_value = 0
        self.lock = RLock()
        for i in range(10):
            self.add_next_item()
        self.generator_thread = Thread(target=self.generator, args=(), daemon=True)
        self.generator_thread.start()

    def get_new_items(self):
        with self.lock:
            previous_index = self.last_index
            self.last_index = len(self.items)
            return self.items[previous_index:self.last_index]

    def add_next_item(self):
        self.last_value += 1
        with self.lock:
            self.items.append(
                {
                    "_source": {
                        "s1": self.last_value,
                        "s2": perf_counter()
                    }
                }
            )

    def generator(self):
        while True:
            self.add_next_item()
            sleep(2)

    def index(self, index, doc_type, body):
        self.means.append(body)


class helpers:

    @staticmethod
    def scan(es: Elasticsearch, preserve_order: bool, query, index: str) -> List[Dict[str, Dict]]:
        # if 'range' in query['query']:
        #     starting_time = query['query']['range']['s2']['gt']
        return es.get_new_items()


import sys
import os
import json

from collections import deque

# Get parameters and set up elasticsearch
print("reading")
if not STANDALONE:
    fd = int(sys.argv[1])
    data = json.loads(sys.stdin.readline())
else:
    data = json.loads(SAMPLE_JOB_CONFIG)


# with open("/opt/ivis-core/1.txt", "w") as file:
#     file.write(json.dumps(data) + "\n\n\n")

print("setting up ES")
es = Elasticsearch([{'host': data['es']['host'], 'port': int(data['es']['port'])}])
state = data.get('state')
params = data['params']
entities = data['entities']
# Task parameters' values
# from params we get cid of signal/signal set and from according key in entities dictionary
# we can access data for that entity (like es index or namespace)
sig_set = entities['signalSets'][params['sigSet']]
ts = entities['signals'][params['sigSet']][params['ts_signal']]
source = entities['signals'][params['sigSet']][params['source_signal']]
window = int(params['window'])
values = []  # TODO: what is values? Add to the state
if state is not None and "values" in state:
    values = state["values"]

print("creating deque")
queue = deque(values, maxlen=window)
if state is None or state.get('index') is None:
    ns = sig_set['namespace']

    msg = {}
    msg['type'] = 'sets'
    # Request new signal set creation
    msg['sigSet'] = {
        "cid": "moving_average",
        "name": "moving average",
        "namespace": ns,
        "description": "moving average",
        "aggs": "0"
    }
    signals = []
    signals.append({
        "cid": "mean",
        "name": "mean",
        "description": "mean",
        "namespace": ns,
        "type": "raw_double",
        "indexed": False,
        "settings": {}
    })
    msg['sigSet']['signals'] = signals
    if STANDALONE:
        run_response = ivis_core.HandleRunRequest(RunRequest(
            job_id="job1",
            request=json.dumps(msg)
        ), None)
        state = json.loads(run_response.response)
    else:
        ret = os.write(fd, (json.dumps(msg) + '\n').encode())
        print(f"written {ret} chars")
        print("loading json from stdin")
        print(json.dumps(msg))
        state = json.loads(sys.stdin.readline())
    print("checking error")
    error = state.get('error')
    if error:
        sys.stderr.write(error + "\n")
        sys.exit(1)

last = None

if state is not None and state.get('last') is not None:
    last = state['last']
    query_content = {
        "range": {
            ts['field']: {
                "gt": last
            }
        }
    }
else:
    query_content = {'match_all': {}}
query = {
    'size': 10000,
    '_source': [source['field'], ts['field']],
    'sort': [{ts['field']: 'asc'}],
    'query': query_content
}
print("scanning with query")
results = helpers.scan(es,
                       preserve_order=True,
                       query=query,
                       index=sig_set['index']
                       )
i = 0
print("composing state")
for item in results:
    last = item["_source"][ts['field']]
    val = item["_source"][source['field']]
    if val is not None:
        queue.append(val)
    else:
        continue
    if i < (window - 1):
        i += 1
    else:
        mean = sum(queue) / float(window)
        doc = {
            state['fields']['mean']: mean
        }
        res = es.index(index=state['index'], doc_type='_doc', body=doc)
state["last"] = last
state["values"] = list(queue)
# Request to store state
msg = {}
msg["type"] = "store"
msg["id"] = 1
msg["state"] = state
print("writing result")
if STANDALONE:
    run_response = ivis_core.HandleRunRequest(RunRequest(
        job_id="job1",
        request=json.dumps(msg)
    ), None)
    print(f"Response:\n{run_response.response}")
else:
    ret = os.write(fd, (json.dumps(msg) + '\n').encode())
    os.close(fd)
