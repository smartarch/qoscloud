import logging
from typing import List

from cloud_controller import SYSTEM_DATABASE_NAME, APPS_COLLECTION_NAME
from cloud_controller.knowledge.knowledge import Knowledge
from cloud_controller.task_executor.execution_context import StatefulnessControllerExecutionContext
from cloud_controller.tasks.preconditions import namespace_active, application_deployed
from cloud_controller.tasks.task import Task


class ShardCollectionTask(Task):
    """
    Creates and shards a MongoDB collection.
    """
    def __init__(self, app_name: str, database: str, collection: str):
        self._database = database
        self._collection = collection
        super(ShardCollectionTask, self).__init__(
            task_id=self.generate_id()
        )
        self.add_precondition(application_deployed, (app_name,))

    def execute(self, context: StatefulnessControllerExecutionContext) -> bool:
        context.mongo_controller.shard_collection(
            self._database,
            self._collection
        )
        logging.info(f"Collection {self._database}.{self._collection} sharded")
        return True

    def generate_id(self) -> str:
        return f"{self.__class__.__name__}_{self._database}_{self._collection}"

    def update_model(self, knowledge: Knowledge) -> None:
        if self._database in knowledge.applications:
            app = knowledge.applications[self._database]
            if self._collection in app.components:
                app.components[self._collection].collection_sharded = True


class MoveChunkTask(Task):
    """
    See documentation to MongoController.move_chunk
    """
    def __init__(self, database: str, collection: str, key: int, shard: str):
        self._database = database
        self._collection = collection
        self._key = key
        self._shard = shard
        super(MoveChunkTask, self).__init__(
            task_id=self.generate_id()
        )

    def execute(self, context: StatefulnessControllerExecutionContext) -> bool:
        context.mongo_controller.move_chunk(
            self._database,
            self._collection,
            self._key,
            self._shard
        )
        logging.info(f"Chunk {self._key} of collection {self._database}.{self._collection} "
                     f"moved to shard {self._shard}")
        return True

    def generate_id(self) -> str:
        return f"{self.__class__.__name__}_{self._database}_{self._collection}_{self._key}_{self._shard}"


class DropDatabaseTask(Task):

    def __init__(self, database: str, collections: List[str]):
        """
        Drops a database.
        :param database: Database to drop.
        :param collections: Collections that were created in this database.
        """
        self._database = database
        self._collections = collections
        super(DropDatabaseTask, self).__init__(
            task_id=self.generate_id()
        )

    def execute(self, context: StatefulnessControllerExecutionContext) -> bool:
        context.mongo_controller.drop_database(
            self._database,
            self._collections
        )
        logging.info(f"Database {self._database} dropped")
        return True

    def generate_id(self) -> str:
        return f"{self.__class__.__name__}_{self._database}"

    def update_model(self, knowledge: Knowledge) -> None:
        if self._database in knowledge.applications:
            knowledge.applications[self._database].db_dropped = True


class AddAppRecordTask(Task):
    """
    Adds a record about the application to MongoDB. These records are used by the framework to check which apps are
    present in the cloud after a framework restart.
    """
    def __init__(self, app_name: str):
        self._app_name = app_name
        super(AddAppRecordTask, self).__init__(
            task_id=self.generate_id()
        )

    def execute(self, context: StatefulnessControllerExecutionContext) -> bool:
        context.mongo_controller.add_document(SYSTEM_DATABASE_NAME, APPS_COLLECTION_NAME, {
            "type": "app",
            "name": self._app_name
        })
        logging.info(f"Record for app {self._app_name} added")
        return True

    def update_model(self, knowledge: Knowledge) -> None:
        knowledge.actual_state.add_application(knowledge.applications[self._app_name])

    def generate_id(self) -> str:
        return f"{self.__class__.__name__}_{self._app_name}"


class DeleteAppRecordTask(Task):
    """
    Deletes a record about the application from MongoDB.
    """
    def __init__(self, app_name: str):
        self._app_name = app_name
        super(DeleteAppRecordTask, self).__init__(
            task_id=self.generate_id()
        )
        # self.add_precondition(lambda _, x: not namespace_active(_, x), (self._app_name,))

    def execute(self, context: StatefulnessControllerExecutionContext) -> bool:
        context.mongo_controller.delete_document(SYSTEM_DATABASE_NAME, APPS_COLLECTION_NAME, {
            "type": "app",
            "name": self._app_name
        })
        logging.info(f"Record for app {self._app_name} deleted")
        return True

    def update_model(self, knowledge: Knowledge) -> None:
        knowledge.actual_state.remove_application(self._app_name)

    def generate_id(self) -> str:
        return f"{self.__class__.__name__}_{self._app_name}"


