import pymongo as pymongo


class Mongodb_Connect:

    def __init__(self, host=None, database=None, table=None):

        self._db_name = database
        self._db_table = table
        self._db_host = host
        self._mongo_link = 'mongodb://{}:27017/'.format(self._db_host)
        self._connection = {}
        self._connection = self.__mongodb_connect(mongo_link=self._mongo_link)
        self._data = {}

    def __mongodb_connect(self, mongo_link=None):

        client = None
        try:

            client = pymongo.MongoClient(mongo_link, serverSelectionTimeoutMS=1)

        except pymongo.errors.ServerSelectionTimeoutError as err:

            print(err)

        db = client[self._db_name]
        return {"db": db, "client": client}

    def select(self, tablename=None):

        if tablename is not None:
            table = self._connection["db"][tablename]
            self._data = table.find()

        return self._data

    def insert(self, table_data=None):

        if table_data["TABLE"] is not None:
            table = self._connection["db"][table_data["TABLE"]]
            self._data = table.insert_one(table_data["DATA"])

        return self._data

    def delete(self, table=None):

        table = self._connection["db"][table]
        self._data = table.delete_many({})

        return self._data.deleted_count

    def close(self, ):

        self._connection["client"].close()
        return "CLOSED"
