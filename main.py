import os

from processes import ForecastPipelineProcess


def lambda_handler(event, context):
    HOST = str(os.environ["HOST"])
    USER = str(os.environ["USER"])
    PASSWORD = str(os.environ["PASSWORD"])
    PORT = int(os.environ["PORT"])
    DATABASE = str(os.environ["DATABASE"])

    forecastpipeline = ForecastPipelineProcess()
    try:
        connection = forecastpipeline.dbconnection(HOST=HOST, USER=USER, PASSWORD=PASSWORD, PORT=PORT,
                                                   DATABASE=DATABASE)
    except:
        print("Database Connection Error")

    try:
        data = forecastpipeline.process(connection=connection)
        forecastpipeline.forecast(context=context, processdata=data)
    except Exception as e:
        print(e)
    finally:
        connection[0].close()
        connection[1].result()[1].close()
