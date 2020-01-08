import pandas as pd
from pandas.io.json import json_normalize

from mongodb import Mongodb_Connect
from postgres import Postgres_Connect


class Data_prep_pipeline:

    def __init__(self, host=None, user=None, password=None, port=None, database=None, tables=None):

        self._data = self.data_prep(host=host, user=user, password=password, port=port, database=database,
                                    tables=tables)

    @staticmethod
    def data_prep(host=None, user=None, password=None, port=None, database=None, tables=None):

        print(host, user, password, port, database, tables)

        vehicle_data = Postgres_Connect(host=host, user=user, password=password, port=port, database=database)

        vehicle = pd.DataFrame(vehicle_data.select(tables["VEHICLES"]))
        component_type_vehicle = pd.DataFrame(vehicle_data.select(tables["COMPONENT_TYPE_VEHICLE"]))

        vec, com = vehicle.iloc[:, [0, 1, 6]], component_type_vehicle.iloc[:, [0, 1, 2]]
        vec.columns, com.columns = ["vehicle_id", "slug", "user_id"], ["component_type_vehicle_id", "component_type_id",
                                                                       "vehicle_id"]
        vehicle_components = pd.merge(vec, com, on=["vehicle_id"], how='inner')

        check = Mongodb_Connect(host=host, database=database, table=tables["COMPONENT_DAMAGES"])

        components_damages = json_normalize(list(check.select(tables["COMPONENT_DAMAGES"])))

        columns_todelete = [x for x in components_damages.columns.to_list() if
                            str(x).endswith("lower_damage") | str(x).endswith("upper_damage")]
        components_damages.drop(columns_todelete, axis=1, inplace=True)
        components_damages_TRIM = components_damages

        row_expansion = pd.DataFrame.from_records(components_damages_TRIM.damages_types.tolist()).stack().reset_index(
            level=1, drop=True).rename('damages_types')

        components_damages_TRIM = components_damages_TRIM.drop('damages_types', axis=1).join(row_expansion).reset_index(drop=True)[
            components_damages.columns.to_list()]  # .head(5)

        result = pd.merge(vehicle_components, components_damages_TRIM, on=["component_type_vehicle_id"], how='inner')

        return [result, vehicle_data, components_damages]

    def result(self, ):

        return self._data
