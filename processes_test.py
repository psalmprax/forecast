import ast
import datetime
import os
import signal

import pandas as pd
from data_pipeline import Data_prep_pipeline
from environs import Env
from forecast import Forecast
from mongodb import Mongodb_Connect
from pandas.io.json import json_normalize


def sigterm_handler():
    # Raises SystemExit(0):
    sys.exit(0)


class ForecastPipelineProcess():

    def __init__(self):
        self.env = Env()
        self.env.read_env()

        HOST = str(os.environ["HOST"])
        USER = str(os.environ["USER"])
        PASSWORD = str(os.environ["PASSWORD"])
        PORT = int(os.environ["PORT"])
        DATABASE = str(os.environ["DATABASE"])
        BENCHMARK = int(os.environ["BENCHMARK"])

        COMPREDICT_AI_CORE_KEY = str(os.environ["COMPREDICT_AI_CORE_KEY"])
        COMPREDICT_AI_CORE_FAIL_ON_ERROR = ast.literal_eval(str(os.environ["COMPREDICT_AI_CORE_FAIL_ON_ERROR"]))
        COMPREDICT_AI_CORE_PPK = os.environ["COMPREDICT_AI_CORE_PPK"]
        COMPREDICT_AI_CORE_PASSPHRASE = os.environ["COMPREDICT_AI_CORE_PASSPHRASE"]
        COMPREDICT_AI_CORE_BASE_URL = str(os.environ["COMPREDICT_AI_CORE_BASE_URL"])
        COMPREDICT_AI_CORE_CALLBACK_URL = str(os.environ["COMPREDICT_AI_CORE_CALLBACK"])

        self.api_config = {"COMPREDICT_AI_CORE_KEY": COMPREDICT_AI_CORE_KEY,
                           "COMPREDICT_AI_CORE_FAIL_ON_ERROR": COMPREDICT_AI_CORE_FAIL_ON_ERROR,
                           "COMPREDICT_AI_CORE_PPK": COMPREDICT_AI_CORE_PPK,
                           "COMPREDICT_AI_CORE_PASSPHRASE": COMPREDICT_AI_CORE_PASSPHRASE,
                           "COMPREDICT_AI_CORE_BASE_URL": COMPREDICT_AI_CORE_BASE_URL,
                           "COMPREDICT_AI_CORE_CALLBACK_URL": COMPREDICT_AI_CORE_CALLBACK_URL}

        self.check = self.data_mongodb = self.request_table = self.data = self.forecasting_table_data = self.result = \
            self.forcaster = self.callback_param = {}

        self.tables = {"FORECASTING": "forecasting", "REQUEST": "requests"}
        self.table_names = dict(COMPONENT_DAMAGES="component_damages", VEHICLES="vehicles",
                                COMPONENT_TYPE_VEHICLE="component_type_vehicle")

        self.request_count, self.success_count, self.temp = 0, 0, []

    def process(self):

        HOST = "35.156.104.103"
        USER = "postgres"
        PASSWORD = "secret"
        PORT = 5432
        DATABASE = "analytics_v2"
        BENCHMARK = 1000

        result = Data_prep_pipeline(host=HOST, user=USER, password=PASSWORD,
                                    port=PORT, database=DATABASE, tables=self.table_names)

        results = result.result()

        results[0].rename(columns={'_id': 'idX'}, inplace=True)
        results[0]['idX'] = results[0]['idX'].astype('str')

        check = Mongodb_Connect(host=self.env('HOST'), database=self.env('DATABASE'))

        try:
            forcaster = check.select(self.tables["FORECASTING"])
            if forcaster != {}:
                for document in forcaster:
                    self.temp.append(document)

                forcasting = json_normalize(self.temp)
                forcasting.rename(columns={'damage_id': 'idX', "damage_type_id": "damages_types"}, inplace=True)
                forcasting['idX'] = forcasting['idX'].astype('str')

                for x, row in forcasting.iterrows():
                    forcasting.at[x, 'latest_trip_mileage'] = row["results.mileage"][0]
                    forcasting.at[x, 'update_date_at'] = eval(str(row["update_date_at"]).strip())
                for x, row in results[0].iterrows():
                    results[0].at[x, 'Max_Km'] = row["mileages"][-1]

                data = pd.merge(results[0], forcasting, on=['idX', 'damages_types'], how='left')
                data.drop(columns="updated_at_y", inplace=True)
                data.rename(columns={'updated_at_x': 'updated_at'}, inplace=True)

                data['update_date_at'].fillna('1900-01-01 00:00:00', inplace=True)
                for column in data.keys()[9:23]:
                    isnull = data[column].isnull()
                    data.loc[isnull, [column]] = [[[0]] * isnull.sum()]

                data.fillna(0, inplace=True)

                data['update_date_at'] = pd.to_datetime(data['update_date_at'])

                data = data[(data['Max_Km'] - data['latest_trip_mileage']) > BENCHMARK]  # int(self.env('BENCHMARK'))]
                data = data[(data['updated_at'] - data['update_date_at']).dt.total_seconds() / 3600 > 0]
                data.sort_values(by=['updated_at'], inplace=True, ascending=False)


        except:

            data = results[0]
            data.drop(columns="updated_at_y", inplace=True)
            data.rename(columns={'updated_at_x': 'updated_at'}, inplace=True)
            for column in data.keys()[9:23]:
                isnull = data[column].isnull()
                data.loc[isnull, [column]] = [[[0]] * isnull.sum()]
            data.fillna(0, inplace=True)

        return [check, data, results[1]]

    def forecast(self):
        count = 0
        processdata = self.process()

        check, data, postgresdb = processdata

        forecast = dict()
        data_forecast = Forecast(self.api_config)

        if not data.empty:
            TIMEOUT = int(os.environ["TIMEOUT"])
            signal.signal(signal.SIGTERM, sigterm_handler)
            for index, row in data.iterrows():
                if context.get_remaining_time_in_millis() < TIMEOUT:
                    signal.alarm(0)
                forecast["data"] = row
                data_forecast.forecast_checker(forecast=forecast)

                data_mongodb = data_forecast.results()

                if data_mongodb is not None:
                    self.success_count += 1
                    print("success_count: ", self.success_count)
                else:
                    print("failed: ", data_mongodb["callback_param"])
                    continue

                request_table = dict(job_id=data_mongodb["reference_id"], status=data_mongodb["status"],
                                     success=data_mongodb["success"], notify=False, user_id=row["user_id"],
                                     created_at=datetime.datetime.now(), algorithm="forecasting")

                request = dict(table=self.tables["REQUEST"], data=request_table)
                print(request)
                # postgresdb.insert(data=request)
                self.request_count += 1
                print("request_count: ", self.request_count)
                callback_param = data_mongodb["callback_param"]

                print("FORECAST/REQUEST DATA INJECTION FOR SINGLE RECORD FIRST TIME")
                if count == 25:
                    break
                count += 1

        else:

            print("NO CHANGES FOR FORCASTING")

        print("callback_param: ", callback_param)

        check.close()
        postgresdb.close()

        # return data
        return postgresdb

    def sigterm_handler(self) :
        # Raises SystemExit(0):
        sys.exit(0)