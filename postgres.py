import pg8000 as pg8000


# import psycopg2 as pg8000


class Postgres_Connect:

    def __init__(self, host=None, database=None, user=None, port=None, password=None):
        self._db_name = database
        self._db_user = user
        self._db_host = host
        self._db_port = port
        self._db_pass = password
        self._result = {}

        print(self._db_host, self._db_user, self._db_pass, self._db_port, self._db_name)
        self._result = self.postgres_connect(host=self._db_host, user=self._db_user, password=self._db_pass,
                                             port=self._db_port, database=self._db_name)

        self.__data = None

    @staticmethod
    def postgres_connect(host=None, user=None, password=None, port=None, database=None):
        connection = pg8000.connect(host=host, user=user, password=password, port=port, database=database)
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()

        return {"cursor": cursor, "connection": connection}

    def select(self, table=None):
        postgres_select_query = '''select * from public.%s''' % (table)
        self._result["cursor"].execute(postgres_select_query)

        return self._result["cursor"].fetchall()

    def insert(self, data=None):
        col = list(data["data"].keys())
        val = list(data["data"].values())
        query_placeholders = ','.join(['%s'] * len(val))
        query_columns = ', '.join(col)
        insert_query = ''' INSERT INTO public.%s(%s) VALUES(%s) ''' % (data["table"], query_columns, query_placeholders)
        insert_query = insert_query
        print(insert_query)
        self._result["cursor"].execute(insert_query, val)
        self._result["connection"].commit()

    def close(self, ):
        self._result["cursor"].close()
        self._result["connection"].close()

        return "CLOSED"
