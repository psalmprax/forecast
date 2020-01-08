import pandas as pd

from postgres import Postgres_Connect

host = "35.156.224.178"
user = "postgres"
password = "secret"
port = 5432
database = "analytics_v2"

vehicle_data = Postgres_Connect(host=host, user=user, password=password, port=port, database=database)


vehicle = pd.DataFrame(vehicle_data.select("requests"))

vec = vehicle.iloc[:, [0, 1, 6]]


print(vec)

