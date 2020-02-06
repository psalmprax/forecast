import os

from processes import ForecastPipelineProcess


def lambda_handler(event, context):
    HOST = str(os.environ["HOST"])
    USER = str(os.environ["USER"])
    PASSWORD = str(os.environ["PASSWORD"])
    PORT = int(os.environ["PORT"])
    DATABASE = str(os.environ["DATABASE"])

    # HOST = "35.156.104.103"
    # USER = "postgres"
    # PASSWORD = "secret"
    # PORT = 5432
    # DATABASE = "analytics_v2"

    forecastpipeline = ForecastPipelineProcess()
    try:
        connection = forecastpipeline.dbconnection(HOST=HOST, USER=USER, PASSWORD=PASSWORD, PORT=PORT,
                                                   DATABASE=DATABASE, condition=5)

    except Exception as ex:
        print(ex)
    try:
        data = forecastpipeline.process(connection=connection)

        forecastpipeline.forecast(context=context, processdata=data)
    except Exception as e:
        print(e)
    finally:
        connection[0].close()
        connection[1].result()[1].close()
