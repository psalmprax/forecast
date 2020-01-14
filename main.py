import datetime

import pandas as pd
from environs import Env
from pandas.io.json import json_normalize

from data_pipeline import Data_prep_pipeline
from forecast import Forecast
from mongodb import Mongodb_Connect


def lambda_handler():
    # Setup alarm for remaining runtime minus a second
    # signal.alarm(int(context.get_remaining_time_in_millis() / 1000) - 1)

    env = Env()
    env.read_env()

    api_config = {"COMPREDICT_AI_CORE_KEY": env("COMPREDICT_AI_CORE_KEY"),
                  "COMPREDICT_AI_CORE_FAIL_ON_ERROR": env("COMPREDICT_AI_CORE_FAIL_ON_ERROR", False),
                  "COMPREDICT_AI_CORE_PPK": env("COMPREDICT_AI_CORE_PPK", None),
                  "COMPREDICT_AI_CORE_PASSPHRASE": env("COMPREDICT_AI_CORE_PASSPHRASE", ""),
                  "COMPREDICT_AI_CORE_BASE_URL": env("COMPREDICT_AI_CORE_BASE_URL"),
                  "COMPREDICT_AI_CORE_CALLBACK_URL": env("COMPREDICT_AI_CORE_CALLBACK_URL")}

    check, data_mongodb, request_table, data, forecasting_table_data, result, forcaster = {}, {}, {}, {}, {}, {}, {}

    tables = {"FORECASTING": "forecasting", "REQUEST": "requests"}
    table_names = dict(COMPONENT_DAMAGES="component_damages", VEHICLES="vehicles",
                       COMPONENT_TYPE_VEHICLE="component_type_vehicle")

    request_count, success_count, callback_param, temp = 0, 0, {}, []

    result = Data_prep_pipeline(host=env('HOST'), user=env('USER'), password=env('PASSWORD'), port=int(env('PORT')),
                                database=env('DATABASE'),
                                tables=table_names)

    results = result.result()

    results[0].rename(columns={'_id': 'idX'}, inplace=True)
    results[0]['idX'] = results[0]['idX'].astype('str')

    check = Mongodb_Connect(host=env('HOST'), database=env('DATABASE'), table=tables["FORECASTING"])

    try:
        forcaster = check.select(tables["FORECASTING"])
        if forcaster != {}:
            for document in forcaster:
                temp.append(document)

            forcasting = json_normalize(temp)
            forcasting.rename(columns={'damage_id': 'idX', "damage_type_id": "damages_types"}, inplace=True)
            forcasting['idX'] = forcasting['idX'].astype('str')

            for x, row in forcasting.iterrows():
                forcasting.at[x, 'latest_trip_mileage'] = row["results.mileage"][0]
                forcasting.at[x, 'update_date_at'] = eval(str(row["update_date_at"]).strip())
            for x, row in results[0].iterrows():
                results[0].at[x, 'Max_Km'] = row["mileages"][-1]

            data = pd.merge(results[0], forcasting, on=['idX', 'damages_types'], how='left')

            data['update_date_at'].fillna('1900-01-01 00:00:00', inplace=True)

            data.fillna(0, inplace=True)
            data['update_date_at'] = pd.to_datetime(data['update_date_at'])

            data = data[(data['Max_Km'] - data['latest_trip_mileage']) > int(env('BENCHMARK'))]
            data = data[(data['updated_at'] - data['update_date_at']).dt.total_seconds() / 3600 > 0]

    except:

        data = results[0]

    postgresdb = results[1]

    forecast = dict()
    data_forecast = Forecast(api_config)

    if not data.empty:

        for index, row2 in data.iterrows():

            forecast["data"] = row2
            data_forecast.forecast_checker(forecast=forecast)

            data_mongodb = data_forecast.results()

            if data_mongodb is not None:
                success_count += 1
                print("success_count: ", success_count)
            else:
                print("failed: ", callback_param)
                continue

            request_table = dict(job_id=data_mongodb["reference_id"], status=data_mongodb["status"],
                                 success=data_mongodb["success"], notify=False, user_id=row2["user_id"],
                                 created_at=datetime.datetime.now(), algorithm="forecasting")

            request = dict(table=tables["REQUEST"], data=request_table)
            print(request)
            postgresdb.insert(data=request)
            request_count += 1
            print("request_count: ", request_count)
            callback_param = None

            print("FORECAST/REQUEST DATA INJECTION FOR SINGLE RECORD FIRST TIME")

    else:

        print("NO CHANGES FOR FORCASTING")

    print("callback_param: ", callback_param)

    check.close()
    postgresdb.close()

    return data


print(lambda_handler())
