"""
Contains samples of IVIS requests and responses used for testing the IVIS interface
"""
SAMPLE_JOB_ID = "job1"
SAMPLE_JOB_INTERVAL = 10
SAMPLE_JOB_PARAMETERS = '''
[
    {
      "id": "sigSet",
      "label": "Signal Set",
      "type": "signalSet"
    },
    {
      "id": "ts_signal",
      "label": "TS signal",
      "type": "signal",
      "signalSetRef": "sigSet"
    },
    {
      "id": "source_signal",
      "label": "Source signal",
      "type": "signal",
      "signalSetRef": "sigSet"
    },
    {
      "id": "window",
      "label": "Window",
      "type": "string"
    }
]
'''
'''
[
    {
      "id": "dataset",
      "label": "Dataset",
      "type": "string"
    }
]
'''
SAMPLE_JOB_CONFIG = '''
{
  "params": {
    "sigSet": "example_set",
    "ts_signal": "ts",
    "source_signal": "source",
    "window": 4
  },
  "entities": {
    "signalSets": {
      "example_set": {
        "index": "signal_set_1",
        "name": "Example set",
        "namespace": 1
      }
    },
    "signals": {
      "example_set": {
        "source": {
          "field": "s1",
          "name": "Source of values",
          "namespace": 1
        },
        "ts": {
          "field": "s2",
          "name": "Timestamp",
          "namespace": 1
        }
      }
    }
  },
  "state": null,
  "es": {
    "host": "localhost",
    "port": "9200"
  }
}
'''
SAMPLE_JOB_STATE = """
{
  "index": "signal_set_2",
  "type": "_doc",
  "fields": {
    "mean": "s3"
  },
  "last": 1.234,
  "values": [1, 2, 3, 4]
}
"""
"""
{"params": {"dataset": "ds1"}}
"""
SAMPLE_JOB_CREATE_SS_REQUEST = '''
{
  "type": "sets",
  "sigSet": {
      "cid" : "moving_average",
      "name" : "moving average" ,
      "namespace": 1,
      "description" : "moving average" ,
      "aggs": "0",
      "signals": [
        {
          "cid": "mean",
          "name": "mean",
          "description": "mean",
          "namespace": 1,
          "type": "raw_double",
          "indexed": false,
          "settings": {} 
        }       
      ]
  }
}
'''
SAMPLE_JOB_CREATE_SS_RESPONSE = '''
{
  "index": "signal_set_2",
  "type": "_doc",
  "fields": {
    "mean": "s3"
  }
}
'''
'''
{
 "id": 1,
 "type": "store",
 "state": {
   "index": "signal_set_2",
   "type": "_doc",
   "fields": {
     "created_signal": "s3"
   }
 }
}
'''
'''
{
    settings: { params: [], jsx: "'use strict';\n", scss: '' },
    name: 'tmp',
    elevated_access: 1,
    type: 'jsx',
    namespace: 1,
    description: '',
    originalHash: '7ea1949a41c2b0c3ddd392f5721234ccb6e879733f37b7e95738d98c5e7cd813',
    id: 1
}
'''


SAMPLE_JOB_CODE = '''
standalone = False

if standalone:
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


print("reading")
import sys
import os
import json

from collections import deque

# Get parameters and set up elasticsearch
print("reading")
if not standalone:
    fd = int(sys.argv[1])
    data = json.loads(sys.stdin.readline())
else:
    data = json.loads(SAMPLE_JOB_CONFIG)

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
    if standalone:
        run_response = ivis_core.HandleRunRequest(RunRequest(
            job_id="job1",
            request=json.dumps(msg)
        ), None)
        state = json.loads(run_response.response)
    else:
        ret = os.write(fd, (json.dumps(msg) + '\\n').encode())
        print(f"written {ret} chars")
        print("loading json from stdin")
        print(json.dumps(msg))
        state = json.loads(sys.stdin.readline())
    print("checking error")
    error = state.get('error')
    if error:
        sys.stderr.write(error + "\\n")
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
if standalone:
    run_response = ivis_core.HandleRunRequest(RunRequest(
        job_id="job1",
        request=json.dumps(msg)
    ), None)
    print(f"Response:\\n{run_response.response}")
else:
    ret = os.write(fd, (json.dumps(msg) + '\\n').encode())
    os.close(fd)
'''
