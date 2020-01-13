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

    print(api_config)

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

    print(results[0])

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
            for x, row in results[0].iterrows():
                results[0].at[x, 'Max_Km'] = row["mileages"][-1]

            data = pd.merge(results[0], forcasting, on=['idX', 'damages_types'], how='left')

            data.fillna(0, inplace=True)

            # data = data[(data['Max_Km'] - data['latest_trip_mileage']) > env('BENCHMARK')]
            data = data[(data['Max_Km'] - data['latest_trip_mileage']) > env('BENCHMARK') & (
                        data['udpated_at'] > data['udpate_date_at'])]

    except:

        data = results[0]

    postgresdb = results[1]

    forecast = dict()
    data_forecast = Forecast(api_config)

    count = 0

    if not data.empty:

        for index, row2 in data.iterrows():

            forecast["data"] = row2
            # forecast["api_config"] = api_config
            data_forecast.forecast_checker(forecast=forecast)

            data_mongodb = data_forecast.results()

            if data_mongodb is not None:
                success_count += 1
                print("success_count: ", success_count)
            else:
                print("failed: ", callback_param)
                continue

            request_table["job_id"] = data_mongodb["reference_id"]
            request_table["status"] = data_mongodb["status"]
            request_table["success"] = data_mongodb["success"]
            request_table["notify"] = False
            request_table["errors"] = 0  # todo this should be removed in production
            request_table["user_id"] = row2["user_id"]
            request_table["created_at"] = datetime.datetime.now()
            request_table["algorithm"] = "forecasting"

            request = dict(table=tables["REQUEST"], data=request_table)
            print(request)
            postgresdb.insert(data=request)
            request_count += 1
            print("request_count: ", request_count)
            callback_param = None

            print("FORECAST/REQUEST DATA INJECTION FOR SINGLE RECORD FIRST TIME")

            # table_data = dict(TABLE=tables["FORECASTING"], DATA=data_mongodb)
            # check.insert(table_data=table_data)

            count += 1
            if count == 2:
                break


    else:

        print("NO CHANGES FOR FORCASTING")

    print("callback_param: ", callback_param)

    check.close()
    postgresdb.close()

    return data


# if __name__ == 'main':
print(lambda_handler())
