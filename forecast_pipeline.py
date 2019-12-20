#https://stackoverflow.com/questions/714063/importing-modules-from-parent-folder
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



#Mongodb_Connect class has 4 functions decorated with @property select,insert,delete,close
class Mongodb_Connect:
    
    # Mongodb_Connect constructor
    def __init__(self,host=None, database=None, table=None, data=None):
        
        self._db_name = database
        self._db_table = table
        self._db_host = host
        self._mongo_link = 'mongodb://{}:27017/'.format(self._db_host)
        self._connection = {}
        self._connection = self.__mongodb_connect(host=self._db_host, database=self._db_name,table=self._db_table)
        self._data={}
    
    # initialize connection to mongodb. Not accessible to the public
    def __mongodb_connect(self, host=None, database=None, table=None, data=None):
        
        client = None
        try:
            client = pymongo.MongoClient(self._mongo_link,serverSelectionTimeoutMS=1)

        #print(client.server_info())
        except pymongo.errors.ServerSelectionTimeoutError as err:
            
        # print error from the database connection
            print(err)
        
        # Get the sampleDB database
        db = client[self._db_name]
        return {"db":db,"client":client}
    
    #select * from tablename. Not accessible to the public
    #accessible through it's property interface
    def __setter_select(self,tablename=None):
        
        #user = users.find({"vehicle_id":vehcile_id})
        if tablename != None: 
            table = self._connection["db"][tablename]
            self._data = table.find()
    
    #get the result of select * from tablename. Not accessible to the public
    #accessible through it's property interface      
    def __getter_select(self,):
        
        return self._data
    
    #input data into the table = table_data = {"FORECASTING_TABLE":a table, "FORECASTING_DATA": data to be input}. Not accessible to the public
    #accessible through it's property interface
    def __setter_insert(self,table_data=None):
        
        #user = users.find({"vehicle_id":vehcile_id})
        if table_data["FORECASTING_TABLE"] != None: 
            table = self._connection["db"][table_data["FORECASTING_TABLE"]]
            self._data = table.insert_one(table_data["FORECASTING_DATA"])
            
    #get the insert_id
    #accessible through it's property interface        
    def __getter_insert(self,):
        
        return self._data.inserted_id
    
    #delete all the record. need dynamic delete in the future
    #accessible through it's property interface
    def __delete(self,table=None):
        
        table = self._connection["db"][table]
        self._data = table.delete_many({})
        return self._data
    
    #close mongodb connection
    #accessible through it's property interface
    def __close(self,):
        
        self._connection["client"].close()
        return "CLOSED"
    
    #accessible interface for select, insert, delete and close functions      
    select = property(__getter_select,__setter_select,"I'm the 'Mongodb' select property.")
    insert = property(__getter_insert,__setter_insert,"I'm the 'Mongodb' insert property.")
    delete  = property(fget=None, fset=__delete, fdel=None,doc="I'm the 'Mongodb' insert property.")
    close  = property(fget=__close, fset=None, fdel=None,doc="I'm the 'Mongodb' insert property.")
    
   

   
class Forecast:

    def __init__(self,):#data=None, api_config= None):
    
        self.__result = None
    
    #sends damage data to the AICORE for forcasting    
    def __algorithm(self, data, callback=None,api_config=None):
        
        # TODO implement
        callback_url = api_config["COMPREDICT_AI_CORE_CALLBACK_URL"]
        
        client = api.get_instance(token=api_config["COMPREDICT_AI_CORE_KEY"], callback_url=callback_url, url=api_config["COMPREDICT_AI_CORE_BASE_URL"])
        client.fail_on_error(option=api_config["COMPREDICT_AI_CORE_FAIL_ON_ERROR"])

            # get a graph
            # algorithm = client.get_algorithm("auto-parameterization")
            # graph = algorithm.get_detailed_graph()
            # new_file = open('auto-parameterization-graph.png', 'wb')
            # shutil.copyfileobj(graph, new_file)
            # graph.close()


        algorithm = client.get_algorithm("base-damage-forecaster")
        results = algorithm.run(data, evaluate=False, encrypt=False, callback_param=callback)
        
        if isinstance(results, resources.Task):
            print(results.job_id)
            
        return results
        
        
    # forecasting function that returns the mydata to the caller
    # data is a series that contains Key-Value data for damages associated to each component for each car
    def __forecast_checker(self,forecast=None):#data=None, api_config=None):
        
        subscript = "damages.%s.damage"%(str(int(forecast["data"]["damages_types"]))) # get the damages_type and use the integer as automatic column for damages.#.damage column to select
        damages = list(forecast["data"][subscript]) # (damages.#.damage) column to select in list 
        km = list(forecast["data"]["mileages"]) # mileage in list format
        callback_param = dict(damage_id=forecast["data"]["idX"] ,damage_type_id=int(forecast["data"]["damages_types"])) # callback 
        
        forecasting ={
                        "data": {
                            "damage": damages,
                            "distance": km
                        }
                        
                    }
        try: 
            forecasting = json.dumps(forecasting, sort_keys=True, indent=4, separators=(',', ': '))
            forecasting = json.loads(forecasting, strict=False)
            
        except ValueError as e: 
        
            print ("This is not a right json format") 
        
        # the algorithm called to forecast
        result = self.__algorithm(forecasting,callback_param,forecast["api_config"])
        
        # mixture of result from AICORE and damage component vehicle data    
        mydata = {"reference_id":result.job_id,
                  "status":"Pending",
                  "success":False}
        
        self.__result = mydata    
        #return mydata
    
    # prediction result call through the @property decorator
    def __results(self,):
    
        return self.__result
    
    forecast = property(fget=__results,fset=__forecast_checker,fdel=None,doc="I'm the 'forecast' property.")
    
    
    

class Postgres_Connect:

    def __init__(self,host=None, database=None, user=None, port=None,password=None):
    
        self._db_name = database
        self._db_user = user
        self._db_host = host
        self._db_port = port
        self._db_pass = password
        self._result = {}
        self._result = self.__postgres_connect(host=self._db_host,user=self._db_user,password=self._db_pass,port=self._db_port,database=self._db_name)
        self.__data = None
        
    def __postgres_connect(self,host=None,user=None,password=None,port=None,database=None):
        
        connection =  pg8000.connect(host=host,user=user,password=password,port=port,database=database)

        # PostgreSQL Connection properties
        cursor = connection.cursor()

        # PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        
        #return [cursor,connection]
        return {"cursor":cursor,"connection":connection}
        
    def __select(self,table=None):
        
        postgreSQL_select_Query = '''select * from public.%s'''%(table)
        self._result["cursor"].execute(postgreSQL_select_Query)
        self.__data = rows = self._result["cursor"].fetchall()
        return rows
        
    def __data(self,):
    
        return self.__data
        
    def __insert(self,data=None):
    
        col = list(data["data"].keys())
        val=list(data["data"].values())
        query_placeholders = ','.join(['%s'] * len(val))
        query_columns = ', '.join(col)

        insert_query = ''' INSERT INTO public.%s(%s) VALUES(%s) ''' %(data["table"],query_columns, query_placeholders)
        insert_query = insert_query
        self._result["cursor"].execute(insert_query,val)
        self._result["connection"].commit()
        
    def __close(self,):
        
        self._result["cursor"].close()
        self._result["connection"].close()
        return "CLOSED"
        

    select = property(fset=__select,fget=__data,fdel=None,doc="I'm the 'Postgres_Connect insert' property.")
    insert = property(fset=__insert,fget=None,fdel=None,doc="I'm the 'Postgres_Connect insert' property.")
    close = property(fset=None,fget=__close,fdel=None,doc="I'm the 'Postgres_Connect close' property.")



class Data_prep_pipeline:

    def __init__(self,host=None,user=None,password=None,port=None, database=None, tables=None):
        
        self._data = self.__data_prep(host=host,user=user,password=password,port=port,database=database,tables=tables)
        
    def __data_prep(self,host=None,user=None,password=None,port=None, database=None, tables=None):
        
        vehicle_data = Postgres_Connect(host=host,user=user,password=password,port=port,database=database)
        vehicle_data.select=tables["VEHICLES"]
        vehicle = pd.DataFrame(vehicle_data.select)
        vehicle_data.select=tables["COMPONENT_TYPE_VEHICLE"]
        component_type_vehicle = pd.DataFrame(vehicle_data.select)
        
        vec,com = vehicle.iloc[:,[0,1,6]],component_type_vehicle.iloc[:,[0,1,2]]
        vec.columns,com.columns = ["vehicle_id","slug","user_id"], ["component_type_vehicle_id","component_type_id","vehicle_id"]
        vehicle_components = pd.merge(vec, com, on=["vehicle_id"], how='inner')
        
        check = Mongodb_Connect(host=host, database=database,table=tables["COMPONENT_DAMAGES"])
        check.select=tables["COMPONENT_DAMAGES"]
        components_damages = json_normalize(list(check.select), max_level=20)
        
        columns_todelete = [x for x in components_damages.columns.to_list() if str(x).endswith("lower_damage")|str(x).endswith("upper_damage")]
        components_damages.drop(columns_todelete, axis=1, inplace=True)
        components_damages_TRIM = components_damages

        row_expansion=pd.DataFrame.from_records(components_damages_TRIM.damages_types.tolist()).stack().reset_index(level=1, drop=True).rename('damages_types')
                                                                                                                 
        components_damages_TRIM = components_damages_TRIM.drop('damages_types', axis=1).join(row_expansion).reset_index(drop=True)[components_damages.columns.to_list()]#.head(5)

        result =  pd.merge(vehicle_components, components_damages_TRIM, on=["component_type_vehicle_id"], how='inner')     
        return [result,vehicle_data,components_damages]
        
    def __result(self,):
        
        return self._data
    
    pipeline = property(__result,"I'm the 'Data_prep_pipeline' property.")
    
    
    
  