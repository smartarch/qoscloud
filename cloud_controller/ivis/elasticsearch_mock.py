from threading import RLock, Thread
from time import perf_counter, sleep
from typing import Dict, List


class Elasticsearch:

    def __init__(self):
        self.items: List[Dict[str, Dict]] = []
        self.means: List[Dict[str, float]] = []
        self.last_index = 0
        self.lock = RLock()
        self.generator_thread = Thread(target=self.get_new_items, args=())
        self.generator_thread.start()

    def get_new_items(self):
        with self.lock:
            previous_index = self.last_index
            self.last_index = len(self.items)
            return self.items[previous_index:self.last_index]

    def generator(self):
        last_value = 0
        while True:
            last_value += 1
            with self.lock:
                self.items.append(
                    {
                        "_source": {
                            "s1": last_value,
                            "s2": perf_counter()
                        }
                    }
                )
            sleep(2)

    def index(self, index, doc_type, body):
        self.means.append(body)


class helpers:



    @staticmethod
    def scan(es: Elasticsearch, preserve_order: bool, query, index: str) -> List[Dict[str, Dict]]:
        # if 'range' in query['query']:
        #     starting_time = query['query']['range']['s2']['gt']
        return es.get_new_items()

