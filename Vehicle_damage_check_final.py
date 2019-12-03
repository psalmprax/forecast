#C:\Users\COMPREDICT\Downloads\lambdafunction\Lib\site-packages
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from compredict.client import api
from compredict.resources import resources
from time import sleep
from environs import Env
from sys import exit
from pandas.io.json import json_normalize
import json
import os
import json
import pg8000
import pymongo
import pprint
import pandas as pd
import time
from bson import ObjectId

def lambda_handler(event, context):
    # TODO implement
    HOST = str(os.environ["host"])
    USER = str(os.environ["user"])
    PASS = str(os.environ["password"])
    PORT = int(os.environ["port"])
    DATABASE = str(os.environ["database"])
    TABLE= str(os.environ["table"])
    BENCHMARK = int(os.environ["benchmark"])
    COUNT = 0
    #delete_status = mongodb_connect(host=HOST, database=DATABASE,table=TABLE,command="delete all")
    
    vehicle_trip = postgres_connect(host=HOST,user=USER,password=PASS,port=PORT,database=DATABASE,sql_code=3)
    vehicle_trip = pd.DataFrame(vehicle_trip)
    vehicle_trip.columns = ["vehicle_id","vehicle_name","component_type_id","component_type_name", \
                            "component_damage_type_id","component_damage_value","max_trip_mileage"]
    vehicle_trip_trim = vehicle_trip.iloc[:,[0,2,4,6]]
    damage_table = mongodb_connect(host=HOST, database=DATABASE,table=TABLE)
    damage_table = json_normalize(damage_table, max_level=20)
    
    try:
    
        damage_table.sort_values(by=["vechicle_id","component_type_id","damage_type_id"], inplace=True,ascending=True)
        damage_table_trim = damage_table.iloc[:,[2,3,5,6,7]]
        
        data = vehicle_trip_trim.merge(damage_table_trim, how='left', left_on=['vehicle_id','component_type_id','component_damage_type_id'], \
                                       right_on=['vechicle_id','component_type_id','damage_type_id'])
        data.fillna(0, inplace=True)
        data = data.loc[(data['max_trip_mileage']-data['latest_trip_mileage'])>BENCHMARK]
        print(damage_table_trim)

    except:
        data = vehicle_trip_trim   
    
    vehicle_trip_FULL = postgres_connect(host=HOST,user=USER,password=PASS,port=PORT,database=DATABASE,sql_code=4)
    vehicle_trip_FULL = pd.DataFrame(vehicle_trip_FULL).iloc[:,[0,2,4,6,7]]
    vehicle_trip_FULL.columns = ["vehicle_id","component_type_id","component_damage_type_id","component_damage_value", \
                                 "max_trip_mileage"]

    damage =[]
    km =[]
    for index, row2 in data.iterrows():
        forecast  = vehicle_trip_FULL.loc[(vehicle_trip_FULL['vehicle_id']==int(row2["vehicle_id"]))& \
                                          (vehicle_trip_FULL['component_type_id']==int(row2["component_type_id"]))& \
                                          (vehicle_trip_FULL['component_damage_type_id']==int(row2["component_damage_type_id"]))]
        
        data_mongodb = forecast_checker(data=forecast)
        insert_status = mongodb_connect(host=HOST, database=DATABASE,table=TABLE,command="insert",data=data_mongodb)
        if COUNT == 10:
            break
        COUNT += 1
    
    
    return "90 new forcasting done!!!!!" #damage_table#
    

def postgres_connect(host=None,user=None,password=None,port=None,database=None, sql_code=None):
    connection = pg8000.connect(host=host,user=user,password=password,port=port,database=database)
    
    # Print PostgreSQL Connection properties
    cursor = connection.cursor()
    
    # Print PostgreSQL version
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")
    
    print("Selecting rows from mobile table using cursor.fetchall")
    
    #postgreSQL_select_Query = sql_code
    postgreSQL_select_Query = postgressqls(sql_code) if postgressqls(sql_code) else "SELECT version();"
    cursor.execute(postgreSQL_select_Query)
    rows = cursor.fetchall()
    
    return rows
        

def mongodb_connect(host=None, database=None, table=None, command=None,data=None,search=None):
    
    mongo_link = 'mongodb://{}:27017/'.format(host)
    db_name = database
    db_table = table
    db_host  = host

    mongo_link = 'mongodb://{}:27017/'.format(db_host)
    try:
        client = pymongo.MongoClient(mongo_link,serverSelectionTimeoutMS=1)

        #print(client.server_info())
    except pymongo.errors.ServerSelectionTimeoutError as err:
        # do whatever you need
        print(err)
    
    # Get the sampleDB database
    db = client[db_name]
    
    #print(type(db.forecasting.find()))
    users = db[db_table]
    
    #user = users.find({"vehicle_id":vehcile_id})
    if command==None and search==None :
        user = users.find()
        return list(user)
    elif command=="insert":
        user = users.insert_one(data)
        return user.inserted_id
    elif command=="delete all":
        user = users.delete_many({})
        return user.deleted_count
    return None
    
def postgressqls(select):
    if select == 3:
        return "select \
            DMC.vehicle_id as vehicle_id, \
            DMC.vehicle_name as vehicle_name, \
            DMC.component_type_id as component_type_id, \
            DMC.component_type_name as component_type_name, \
            DMC.component_damage_type_id as component_damage_type_id, \
            DMC.component_damage_type_name as component_damage_type_name, \
            max(DMC.trip_mileage ) as max_trip_mileage \
            from (select \
            vehicles.id as vehicle_id, \
            vehicles.name as vehicle_name, \
            trips.id as trip_id, \
            trips.mileage as trip_mileage, \
            component_types.id as component_type_id, \
            component_types.name as component_type_name, \
            component_values.id as component_values_id, \
            component_damage_types.id as component_damage_type_id, \
            component_damage_types.name as component_damage_type_name \
            from vehicles \
            join trips on trips.vehicle_id = vehicles.id \
            join component_values on component_values.trip_id = trips.id \
            join component_types on component_values.component_type_id = component_types.id \
            join component_damage_values on component_damage_values.component_value_id = component_values.id \
            join component_damage_types on component_damage_values.component_damage_type_id = component_damage_types.id \
            order by vehicles.name,component_types.id,trips.id,trips.mileage) DMC \
            group by 1, 2, 3, 4, 5, 6 \
            order by 2"
    elif select == 4:
        return "select \
            DMC.vehicle_id as vehicle_id, \
            DMC.vehicle_name as vehicle_name, \
            DMC.component_type_id as component_type_id, \
            DMC.component_type_name as component_type_name, \
            DMC.component_damage_type_id as component_damage_type_id, \
            DMC.component_damage_type_name as component_damage_type_name, \
            DMC.component_damage_value as component_damage_value, \
            max(DMC.trip_mileage ) as max_trip_mileage \
            from (select \
            vehicles.id as vehicle_id, \
            vehicles.name as vehicle_name, \
            trips.id as trip_id, \
            trips.mileage as trip_mileage, \
            component_types.id as component_type_id, \
            component_types.name as component_type_name, \
            component_values.id as component_values_id, \
            component_damage_types.id as component_damage_type_id, \
            component_damage_types.name as component_damage_type_name, \
            component_damage_values.damage as component_damage_value \
            from vehicles \
            join trips on trips.vehicle_id = vehicles.id \
            join component_values on component_values.trip_id = trips.id \
            join component_types on component_values.component_type_id = component_types.id \
            join component_damage_values on component_damage_values.component_value_id = component_values.id \
            join component_damage_types on component_damage_values.component_damage_type_id = component_damage_types.id \
            order by vehicles.name,component_types.id,trips.id,trips.mileage) DMC \
            group by 1, 2, 3, 4, 5, 6, 7 \
            order by 2"    
    return


def algorithm(data):
    # TODO implement
    env = Env()
    env.read_env()

    token = env("COMPREDICT_AI_CORE_KEY", "3bd00ee3265f6c06b60344802ab85cbaf20426b2")
    callback_url = env("COMPREDICT_AI_CORE_CALLBACK", None)
    fail_on_error = env("COMPREDICT_AI_CORE_FAIL_ON_ERROR", False)
    ppk = env("COMPREDICT_AI_CORE_PPK", None)
    passphrase = env("COMPREDICT_AI_CORE_PASSPHRASE", "")
    base_url = env("COMPREDICT_AI_CORE_BASE_URL", "https://b.aic.compredict.de/api/v{}")
    
    client = api.get_instance(token=token, callback_url=callback_url, ppk=ppk, passphrase=passphrase,base_url=base_url)
    client.fail_on_error(option=fail_on_error)

        # get a graph
        # algorithm = client.get_algorithm("auto-parameterization")
        # graph = algorithm.get_detailed_graph()
        # new_file = open('auto-parameterization-graph.png', 'wb')
        # shutil.copyfileobj(graph, new_file)
        # graph.close()


    algorithms = client.get_algorithms()

    # Check if the user has algorithms to predict
    if len(algorithms) == 0:
        print("No algorithms to proceed!")
        exit()

    #algorithm = algorithms["base-damage-forecaster"]
    algorithm = algorithms[0]

    #print(data)
   
    tmp = algorithm.get_detailed_template()
    tmp.close()  # It is tmp file. close the file to remove it.

    results = algorithm.run(data, evaluate=False, encrypt=False)

    if isinstance(results, resources.Task):
        print(results.job_id)

    return results

            
def forecast_checker(key=None,data=None):
    damages = list(data["component_damage_value"])
    km = list(data["max_trip_mileage"])
    
    forecasting = {
                        "data": {
                            "damage":damages,
                            "distance":km,
                        }
                  }
    
    try: 
        forecasting = json.dumps(forecasting, indent=4, separators=(',', ': '))
        forecasting = json.loads(forecasting)
        print ("Is valid json? true") 
    except ValueError as e: 
        print ("Is valid json? false") 
    
    vechicle_id,component_type_id,component_damage_type_id = data["vehicle_id"],data["component_type_id"],data["component_damage_type_id"]
    result = algorithm(forecasting)
    
    mydata = {"reference_id":result.job_id, 
              "status": result.status, 
              #"result": result.results, 
              "damage_type_id": int(component_damage_type_id.unique()[0]), 
              "success": result.success, 
              "latest_trip_mileage": km[-1], 
              "component_type_id": int(component_type_id.unique()[0]), 
              "vechicle_id": int(vechicle_id.unique()[0]) }
    return mydata
