import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from forecast_pipeline import *
import pandas as pd
import datetime


def lambda_handler(event, context):

    env = Env()
    env.read_env("var.env", recurse=False)

    HOST = env("HOST")#"35.156.104.103" #str(os.environ["host"])
    USER = env("USER") #str(os.environ["user"])
    PASS = env("PASS") #str(os.environ["password"])
    PORT = env.int("PORT") #int(os.environ["port"])
    DATABASE = env("DATABASE") #str(os.environ["database"])
    BENCHMARK = env.int("BENCHMARK") #"forecasting"#str(os.environ["benchmark"])
    TABLE_1= env("TABLE_1")#"forecasting"#str(os.environ["table"])
    TABLE_2= env("TABLE_2")#"forecasting"#str(os.environ["table"])
    TABLE_3= env("TABLE_3")#"forecasting"#str(os.environ["table"])
    TABLE_4= env("TABLE_4")#"forecasting"#str(os.environ["table"])
    TABLE_5= env("TABLE_5")

    #components_damages,components_damages_TRIM,vehicle_components, \
    check,data_mongodb,request_table,data,forecasting_table_data,result,table_update = {},{},{},{},{},{},{}
    count = 0

    tables = {"FORECASTING":TABLE_2,"REQUEST":TABLE_5}
    table_names = {"COMPONENT_DAMAGES":TABLE_1,"VEHICLES":TABLE_3,"COMPONENT_TYPE_VEHICLE":TABLE_4}
    temp = []
    
    try:
        
        # getting the join of COMPONENT_DAMAGES collections ,VEHICLES and COMPONENT_TYPE_VEHICLE tables and their damages_types
        result = Data_prep_pipeline(host=HOST,user=USER,password=PASS,port=PORT,database=DATABASE, tables=table_names)
        results = result.pipeline
        results[0].rename(columns={'_id':'idX'}, inplace=True)
        results[0]['idX'] = results[0]['idX'].astype('str')

        #Getting the FORECASTING collections from Mongodb
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
        #print(data)

    except:
    
        results = result.pipeline
        results[0].rename(columns={'_id':'idX'}, inplace=True)
        results[0]['idX'] = results[0]['idX'].astype('str')

        data = results[0]
        print("FIRST TIME FORECASTING PROCESS RUN")    

    
    check.select = tables["FORECASTING"]
    focast_check = json_normalize(list(check.select), max_level=20)
    postgresdb = results[1]  
    postgresdb.select = tables["REQUEST"]

    for index, row2 in data.iterrows():
    
        data_mongodb = Forecast(data=row2)

        request_table["job_id"]=data_mongodb.forecast["reference_id"]
        request_table["status"]=data_mongodb.forecast["status"]
        request_table["success"]=data_mongodb.forecast["success"]
        request_table["notify"]=False
        request_table["user_id"]=row2["user_id"]
        request_table["created_at"]=datetime.datetime.now()
        request_table["algorithm"]="forecasting"

        forecasting_table_data["damage_type_id"]=data_mongodb.forecast["damage_type_id"]
        forecasting_table_data["damage_id"]=str(tuple(data_mongodb.forecast.values())[5])
        forecasting_table_data["latest_trip_mileage"]=data_mongodb.forecast["latest_trip_mileage"]
        forecasting_table_data["results"]=data_mongodb.forecast["results"]

        request = dict(table=tables["REQUEST"],data=request_table) 

        table_update = {"FORECASTING_TABLE":tables["FORECASTING"],"FORECASTING_DATA":forecasting_table_data}
        
        try:
            if len(focast_check[focast_check["damage_id"]==forecasting_table_data["damage_id"]])>=1:
                focast_check_temp = focast_check[focast_check["damage_id"]==forecasting_table_data["damage_id"]]
                
                if len(focast_check_temp[focast_check_temp["damage_type_id"]==forecasting_table_data["damage_type_id"]])>=1:
                
                    if len(focast_check_temp[focast_check_temp["latest_trip_mileage"]==forecasting_table_data["latest_trip_mileage"]])>=1:
                    
                        print("this does not need update")
                        
                    else: 
                        
                        print("this does need update/FORECAST/REQUEST DATA INJECTION")
                        postgresdb.insert=request
                        
                else:
                
                    check.insert = table_update
                    print("FORECAST/REQUEST DATA INJECTION FOR EXISTING damage_id BUT NO damage_type_id FOUND SINGLE RECORD FIRST TIME")
                    postgresdb.insert=request
                    
            else:
                
                check.insert = table_update
                print("FORECAST/REQUEST DATA INJECTION FOR SINGLE RECORD FIRST TIME")
                postgresdb.insert=request
        
        except:
            check.insert = table_update
            print("FIRST FORECAST/REQUEST DATA INJECTION FOR ALL RECORDS FROM THE BEGINING")
            postgresdb.insert=request

        forecasting_table_data = {}
        count += 1
        if count == 1:
            break
            
    check.close
    postgresdb.close