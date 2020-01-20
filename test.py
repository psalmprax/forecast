import pg8000

HOST="35.156.104.103"
#HOST=35.156.224.178
USER="postgres"
PASSWORD="secret"
PORT=5432
DATABASE="analytics_v2"


connection = pg8000.connect(host=HOST, user=USER, password=PASSWORD, port=PORT, database=DATABASE)
cursor = connection.cursor()
cursor.execute("SELECT version();")

print(cursor.fetchall())