import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import pandas as pd
import datetime
import ast
from forecast_pipeline import *

def lambda_handler(event, context):

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

    api_config = {"COMPREDICT_AI_CORE_KEY":COMPREDICT_AI_CORE_KEY, "COMPREDICT_AI_CORE_FAIL_ON_ERROR":COMPREDICT_AI_CORE_FAIL_ON_ERROR,"COMPREDICT_AI_CORE_PPK":COMPREDICT_AI_CORE_PPK, \
                 "COMPREDICT_AI_CORE_PASSPHRASE":COMPREDICT_AI_CORE_PASSPHRASE, "COMPREDICT_AI_CORE_BASE_URL":COMPREDICT_AI_CORE_BASE_URL}
    check,data_mongodb,request_table,data,forecasting_table_data,result = {},{},{},{},{},{}
    count = 0

    tables = {"FORECASTING":"forecasting","REQUEST":"requests"}
    table_names = dict(COMPONENT_DAMAGES="component_damages", VEHICLES="vehicles", COMPONENT_TYPE_VEHICLE="component_type_vehicle")
    temp = []
    try:
        result = Data_prep_pipeline(host=HOST,user=USER,password=PASSWORD,port=PORT,database=DATABASE, tables=table_names)
        print(result)
        results = result.pipeline
        results[0].rename(columns={'_id':'idX'}, inplace=True)
        results[0]['idX'] = results[0]['idX'].astype('str')

        #Getting the focasting collections from Mongodb
        check = Mongodb_Connect(host=HOST, database=DATABASE,table=tables["FORECASTING"])
        check.select = tables["FORECASTING"]

        for document in check.select:
            temp.append(document)

        forcasting = json_normalize(temp)
        forcasting.rename(columns={'damage_id':'idX',"damage_type_id":"damages_types"}, inplace=True)
        forcasting['idX'] = forcasting['idX'].astype('str')

        data = pd.merge(results[0], forcasting, on=['idX','damages_types'], how='left')

        data.fillna(0, inplace=True)

        data = data[(data['mileages'].tolist()[0][-1] - data['latest_trip_mileage'])>BENCHMARK]
   
    except:

        results = result.pipeline
        results[0].rename(columns={'_id':'idX'}, inplace=True)
        results[0]['idX'] = results[0]['idX'].astype('str')

        data = results[0]
        
        
    check.select = tables["FORECASTING"]
    focast_check = json_normalize(list(check.select), max_level=20)
    postgresdb = results[1]
    
    if data.empty==False: 
        for index, row2 in data.iterrows():
            
            data_mongodb = Forecast(data=row2,api_config=api_config)
            request_table["job_id"]=data_mongodb.forecast["reference_id"]
            request_table["status"]=data_mongodb.forecast["status"]
            request_table["success"]=data_mongodb.forecast["success"]
            request_table["notify"]=False
            request_table["user_id"]=row2["user_id"]
            request_table["created_at"]=datetime.datetime.now()
            request_table["algorithm"]="forecasting"

            request = dict(table=tables["REQUEST"],data=request_table) 
            postgresdb.insert=request
            print("FORECAST/REQUEST DATA INJECTION FOR SINGLE RECORD FIRST TIME")
            
    else:

        print("NO CHANGES FOR FORCASTING")


    check.close
    postgresdb.close