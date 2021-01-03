import threading
from typing import Dict

from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection

from cloud_controller.middleware import MONGOS_PORT, SHARD_KEY


class MongoAgent:
    """
    Agent that allows managed compins to access their chunk of Mongo collection. All of its methods are essentially
    wrappers over the standard MongoDB Collection methods. These wrappers disallow the user to access the data that
    belong to other users. Only "safe" operations are allowed (i.e. those that do not violate the rules of using the
    Mongo service). Operations such as messing with indexes and sharding keys are not allowed.

    Operations supported by this agent:
        collection.delete_many()
        collection.delete_one()
        collection.find()
        collection.find_one()
        collection.find_one_and_delete()
        collection.find_one_and_replace()
        collection.find_one_and_update()
        collection.insert_many()
        collection.insert_one()
        collection.replace_one()
        collection.update_many()
        collection.update_one()
        collection.count_documents()
    """

    def __init__(self, mongos_ip: str, db_name, collection_name):
        """

        :param mongos_ip: The initial IP of mongos instance
        :param db_name: DB to use
        :param collection_name: Collection to use
        """
        self._mongos_ip = mongos_ip
        self._shard_key = None
        self._db_name = db_name
        self._collection_name = collection_name
        self._mongo_client: MongoClient = MongoClient(mongos_ip, MONGOS_PORT)
        self._collection: Collection = self._mongo_client[db_name][collection_name]
        self._lock = threading.RLock()

    def _set_mongos_ip(self, mongos_ip):
        """
        Changes Mongos IP. Reconnects to the Mongos instance at the new address and uses it for all the subsequent
        requests.
        :param mongos_ip: new Mongos IP to use
        """
        with self._lock:
            self._mongos_ip = mongos_ip
            self._mongo_client = MongoClient(mongos_ip, MONGOS_PORT)
            self._collection = self._mongo_client[self._db_name][self._collection_name]

    def _set_shard_key(self, key):
        """
        :param key: The key identifying this user. Determines which documents of the collection are accessible.
        """
        with self._lock:
            self._shard_key = key

    def _specialize_document(self, doc: Dict):
        """
        Ensures that the supplied document will belong to the user's chunk.
        """
        doc[SHARD_KEY] = self._shard_key
        return doc

    def delete_many(self, filter):
        filter = self._specialize_document(filter)
        with self._lock:
            return self._collection.delete_many(filter)

    def delete_one(self, filter):
        filter = self._specialize_document(filter)
        with self._lock:
            return self._collection.delete_one(filter)

    def find(self, filter, projection=None, sort=None):
        filter = self._specialize_document(filter)
        with self._lock:
            return self._collection.find(filter, projection=projection, sort=sort)

    def find_one(self, filter, projection=None, sort=None):
        filter = self._specialize_document(filter)
        with self._lock:
            return self._collection.find_one(filter, projection=projection, sort=sort)

    def find_one_and_delete(self, filter, projection=None, sort=None):
        filter = self._specialize_document(filter)
        with self._lock:
            return self._collection.find_one_and_delete(filter, projection=projection, sort=sort)

    def find_one_and_replace(self, filter, replacement, projection=None, sort=None, upsert=False,
                             return_document=ReturnDocument.BEFORE):
        filter = self._specialize_document(filter)
        replacement = self._specialize_document(replacement)
        with self._lock:
            return self._collection.find_one_and_replace(filter, replacement, projection=projection, sort=sort,
                                                        upsert=upsert, return_document=return_document)

    def find_one_and_update(self, filter, update, projection=None, sort=None, upsert=False,
                            return_document=ReturnDocument.BEFORE):
        filter = self._specialize_document(filter)
        with self._lock:
            return self._collection.find_one_and_update(filter, update, projection=projection, sort=sort,
                                                        upsert=upsert, return_document=return_document)

    def insert_many(self, documents, ordered=True):
        for i in range(len(documents)):
            documents[i] = self._specialize_document(documents[i])
        with self._lock:
            return self._collection.insert_many(documents, ordered=ordered)

    def insert_one(self, document):
        document = self._specialize_document(document)
        with self._lock:
            return self._collection.insert_one(document)

    def replace_one(self, filter, replacement, upsert=False):
        filter = self._specialize_document(filter)
        replacement = self._specialize_document(replacement)
        with self._lock:
            return self._collection.replace_one(filter, replacement, upsert=upsert)

    def update_many(self, filter, update):
        filter = self._specialize_document(filter)
        with self._lock:
            return self._collection.update_many(filter, update)

    def update_one(self, filter, update):
        filter = self._specialize_document(filter)
        with self._lock:
            return self._collection.update_one(filter, update)

    def count_documents(self, filter):
        filter = self._specialize_document(filter)
        with self._lock:
            return self._collection.count_documents(filter)