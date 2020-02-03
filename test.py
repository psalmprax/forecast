import datetime
import multiprocessing
import os
import signal
import time
from multiprocessing import Process, Pool
from typing import Dict, Any

import pandas as pd
from mongodb import Mongodb_Connect

from data_pipeline import Data_prep_pipeline
from environs import Env

from forecast import Forecast

from processes_copy import ForecastPipelineProcess
import numpy    as np

"""
params = dict(BENCHMARK=int(os.environ["BENCHMARK"]),
              COMPREDICT_AI_CORE_BASE_URL=str(os.environ["COMPREDICT_AI_CORE_BASE_URL"]),
              COMPREDICT_AI_CORE_CALLBACK=str(os.environ["COMPREDICT_AI_CORE_CALLBACK"]),
              COMPREDICT_AI_CORE_FAIL_ON_ERROR=ast.literal_eval(str(os.environ["COMPREDICT_AI_CORE_FAIL_ON_ERROR"])),
              COMPREDICT_AI_CORE_KEY=str(os.environ["COMPREDICT_AI_CORE_KEY"]),
              COMPREDICT_AI_CORE_PASSPHRASE=os.environ["COMPREDICT_AI_CORE_PASSPHRASE"],
              COMPREDICT_AI_CORE_PPK=os.environ["COMPREDICT_AI_CORE_PPK"],
              DATABASE=None,
              HOST=str(os.environ["HOST"]),
              PASSWORD=str(os.environ["PASSWORD"]),
              PORT=int(os.environ["PORT"]),
              TIMEOUT=int(os.environ["TIMEOUT"]),
              USER=str(os.environ["USER"]))
"""
params = dict(BENCHMARK=None,
              COMPREDICT_AI_CORE_BASE_URL=None,
              COMPREDICT_AI_CORE_CALLBACK=None,
              COMPREDICT_AI_CORE_FAIL_ON_ERROR=None,
              COMPREDICT_AI_CORE_KEY=None,
              COMPREDICT_AI_CORE_PASSPHRASE=None,
              COMPREDICT_AI_CORE_PPK=None,
              DATABASE=None,
              HOST=None,
              PASSWORD=None,
              PORT=None,
              TIMEOUT=None,
              USER=None)


def forecast(self, process=None, check=None, postgresdb=None):
    # signal.signal(signal.SIGALRM, self.process)
    env = Env()
    env.read_env()
    table_names = dict(COMPONENT_DAMAGES="component_damages", VEHICLES="vehicles",
                       COMPONENT_TYPE_VEHICLE="component_type_vehicle")

    postgresdb = Data_prep_pipeline(host=env('HOST'), user=env('USER'), password=env('PASSWORD'),
                                    port=int(env('PORT')), database=env('DATABASE'), tables=table_names)

    check = Mongodb_Connect(host=self.env('HOST'), database=self.env('DATABASE'))
    request_count = success_count = count = 0
    tables = {"FORECASTING": "forecasting", "REQUEST": "requests"}
    # processdata = self.process()
    processdata = process
    data = processdata

    forecast = dict()
    data_forecast = Forecast(self.api_config)

    if not data.empty:

        for index, row in data.iterrows():

            forecast["data"] = row
            data_forecast.forecast_checker(forecast=forecast)

            data_mongodb = data_forecast.results()

            if data_mongodb is not None:
                success_count += 1
                print("success_count: ", success_count)
            else:
                print("failed: ", data_mongodb["callback_param"])
                continue

            request_table = dict(job_id=data_mongodb["reference_id"], status=data_mongodb["status"],
                                 success=data_mongodb["success"], notify=False, user_id=row["user_id"],
                                 created_at=datetime.datetime.now(), algorithm="forecasting")

            request = dict(table=tables["REQUEST"], data=request_table)
            print(request)
            # postgresdb.insert(data=request)
            request_count += 1
            print("request_count: ", self.request_count)
            callback_param = data_mongodb["callback_param"]

            print("FORECAST/REQUEST DATA INJECTION FOR SINGLE RECORD FIRST TIME")
            if count == 2:
                break
            count += 1

    else:

        print("NO CHANGES FOR FORCASTING")

    print("callback_param: ", callback_param)

    check.close()
    postgresdb.close()

    # return data
    return postgresdb


def lambda_handlerr():
    # Setup alarm for remaining runtime minus a second
    # signal.alarm(int(context.get_remaining_time_in_millis() / 1000) - 1)

    test = ForecastPipelineProcess()  # params=params)

    balance = test.process()[1]  # multiprocessing.Value('i', test.process())
    check = test.process()[0]
    postgresdb = test.process()[2]
    df_split = np.array_split(balance, 10)
    if __name__ == '__main__':
        pool = Pool(4)
        pool.map(forecast, df_split)
        pool.close()
        pool.terminate()
        pool.join()

    return df_split


print(lambda_handlerr())
