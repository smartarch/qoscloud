"""
Contains the MongoController class responsible for executing the tasks on MongoDB.
"""
import time

import logging
from queue import Queue
from typing import Dict, Callable, Iterable

import pymongo
import threading
from dataclasses import dataclass
from pymongo import MongoClient
from threading import RLock

from pymongo.collection import Collection

from cloud_controller import MAIN_MONGO_SHARD_NAME
from cloud_controller.middleware import MONGOS_PORT, SHARD_KEY

WAIT_BEFORE_RETRY = 1  # Seconds


@dataclass(frozen=True)
class MigrationArgs:
    collection: str
    shard: str
    shard_key: int


class MongoController:
    """
    Can be used to execute the tasks on MongoDB. Since most of the tasks on mongo DB cannot run in parallel and take
    noticeable time, all of the execution calls add the task to a queue and return. The tasks from the queue run
    sequentially in a separate process. In order to start this process start method should be called.
    """

    def __init__(self, ip, port=MONGOS_PORT):
        """
        :param ip: IP of the mongos instance to connect to
        :param port: port of the mongos instance to connect to
        """
        self._mongo_available = True
        self._mongo_client = MongoClient(ip, port, serverSelectionTimeoutMS=15000)
        # Test the connection to mongos. If mongos is not running at the specified address, will throw
        # pymongo.errors.ConnectionFailure
        try:
            self._mongo_client.admin.command('ismaster')
        except pymongo.errors.ConnectionFailure:
            logging.info('MongoDB is not accesible. All operations with Mongo will not work.')
            self._mongo_available = False
        self._queue: Queue = Queue()
        self._chunks: Dict[str, Dict[int, str]] = {}
        self._lock = RLock()

    def start(self) -> None:
        """
        Starts the thread that executes the MongoDB tasks from the queue of this instance.
        """
        if not self._mongo_available:
            return
        operations_thread = threading.Thread(target=self._run, args=())
        operations_thread.setDaemon(True)
        operations_thread.start()

    @staticmethod
    def _try_until_done(task: Callable) -> None:
        while True:
            try:
                task()
                break
            except pymongo.errors.OperationFailure:
                time.sleep(WAIT_BEFORE_RETRY)

    def _run(self) -> None:
        """
        Executes the tasks from the queue.
        """
        while True:
            args: MigrationArgs = self._queue.get(block=True)
            with self._lock:
                if args.collection in self._chunks:
                    if args.shard_key not in self._chunks[args.collection]:
                        self._split_chunk(args.collection, args.shard_key)
                    self._move_chunk(args)

    def _split_chunk(self, collection_name: str, key: int):
        """
        Splits the chunk of the collection at the given value of the shard key.
        :param collection_name: Collection to split
        :param key: Value of the shard key
        """
        def split_command():
            self._mongo_client.admin.command('split', collection_name, middle={SHARD_KEY: key})
        self._try_until_done(split_command)
        self._chunks[collection_name][key] = MAIN_MONGO_SHARD_NAME
        logging.info(f"MongoAgent: Split chunk of {collection_name} at {key}")

    def _move_chunk(self, args: MigrationArgs) -> None:
        """
        Moves the chunk to a given shard of MongoDB
        :param args: MigrationArgs for the chunk
        """
        def move_command():
            self._mongo_client.admin.command("moveChunk", args.collection, find={SHARD_KEY: args.shard_key},
                                             to=args.shard, _secondaryThrottle=False, _waitForDelete=True)
        self._try_until_done(move_command)
        self._chunks[args.collection][args.shard_key] = args.shard
        logging.info(f"MongoAgent: Moved chunk  {args.shard_key} of collection {args.collection} to {args.shard}")

    def shard_collection(self, db_name: str, collection_name: str) -> None:

        def shard_command():
            self._mongo_client.admin.command('enableSharding', db_name)
            self._mongo_client.admin.command('shardCollection', full_collection_name, key={SHARD_KEY: 1}, unique=False)

        if not self._mongo_available:
            return
        db = self._mongo_client[db_name]
        collection = db[collection_name]
        full_collection_name = db_name + '.' + collection_name
        self._try_until_done(shard_command)
        self._chunks[full_collection_name] = {}

    def move_chunk(self, db_name: str, collection_name: str, shard_key: int, shard_id: str) -> None:
        """
        Moves a specified chunk of a specified collection to a specified shard. If the value of the shard key does not
        belong to a separate chunk, splits the chunk at that value first. This task is executed asynchronously.
        :param db_name: Database of the chunk
        :param collection_name: Collection of the chunk
        :param shard_key: Value of the shard key of the chunk
        :param shard_id: ID of the shard where the chunk has to be moved
        """
        if not self._mongo_available:
            return
        full_collection_name = db_name + '.' + collection_name
        args = MigrationArgs(full_collection_name, shard_id, shard_key)
        self._queue.put(args)

    def drop_database(self, db_name: str, collections: Iterable[str]) -> None:
        """
        Drops a database.
        :param db_name: Database to drop.
        :param collections: Collections that were created in this database.
        """
        if not self._mongo_available:
            return
        with self._lock:
            for collection in collections:
                del self._chunks[f'{db_name}.{collection}']
            self._mongo_client.drop_database(db_name)

    def add_document(self, db_name, collection_name, doc) -> None:
        """
        Adds a document to a collection of MongoDB
        :param db_name: Database to add the document to
        :param collection_name: Collection to add the document to
        :param doc: Document to add
        """
        if not self._mongo_available:
            return
        collection: Collection = self._mongo_client[db_name][collection_name]
        collection.insert_one(document=doc)

    def delete_document(self, db_name, collection_name, doc) -> None:
        """
        Deletes a document from a collection of MongoDB
        :param db_name: Database to delete the document from
        :param collection_name: Collection to delete the document from
        :param doc: Document to delete
        """
        if not self._mongo_available:
            return
        collection: Collection = self._mongo_client[db_name][collection_name]
        collection.delete_one(doc)
