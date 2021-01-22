"""
This module contains the ClusterCleaner class responsible for deleting all the
"""
import logging

import kubernetes
import pymongo
from kubernetes import client, config
from pymongo import MongoClient
from pymongo.collection import Collection

from cloud_controller import SYSTEM_DATABASE_NAME, APPS_COLLECTION_NAME, PRODUCTION_KUBECONFIG
from cloud_controller.middleware import MONGOS_PORT


class ClusterCleaner:
    """
    Can perform Kubernetes cluster and MongoDB cleanup with cleanup method. Uses the records that the framework puts
    into the MongoDB to determine which items to remove. Thus, it will not delete any namespaces or databases that were
    not previously created by the framework.
    """

    def __init__(self, mongos_ip: str, kubeconfig_file=PRODUCTION_KUBECONFIG):
        """
        :param mongos_ip: IP of mongos instance to connect to.
        """
        config.load_kube_config(config_file=kubeconfig_file)
        self._mongo_client = MongoClient(mongos_ip, MONGOS_PORT, serverSelectionTimeoutMS=15000)
        self._apps_collection: Collection = self._mongo_client[SYSTEM_DATABASE_NAME][APPS_COLLECTION_NAME]
        self._core_api = client.CoreV1Api()

    def cleanup(self) -> None:
        """
        Performs the cluster and DB cleanup based on the records found in the MongoDB.
        Deletes all the namespaces and the database records that were left after the previous run of the framework.
        """
        try:
            self._mongo_client.admin.command('ismaster')
        except pymongo.errors.ConnectionFailure:
            logging.info("Mongo DB is not available. Cleanup is not possible")
            return

        for doc in self._apps_collection.find():
            app_name = doc['name']
            self._drop_database(app_name)
            self._delete_namespace(app_name)
            self._apps_collection.delete_one(doc)

    def _drop_database(self, app_name: str) -> None:
        """
        Drops the database with the specified name from the MongoDB.
        :param app_name: A name of the database to drop.
        """
        self._mongo_client.drop_database(app_name)
        logging.info(f'Database {app_name} dropped')

    def _delete_namespace(self, app_name: str) -> None:
        """
        Deletes the specified namespace from the Kubernetes cluster.
        :param app_name: A name of namespace to delete.
        """
        try:
            api_response = self._core_api.delete_namespace(
                name=app_name,
                body=client.V1DeleteOptions(),
                propagation_policy='Foreground',
                grace_period_seconds=0
            )
            logging.info(f'Namespace {app_name} deleted from cluster. status={api_response}')
        except kubernetes.client.rest.ApiException as e:
            if e.status == 404:
                logging.info(f'Namespace {app_name} does not exist')
            elif e.status == 409:
                logging.info(f'Namespace {app_name} is already being deleted')
            else:
                raise
