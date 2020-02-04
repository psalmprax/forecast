from processes_test import ForecastPipelineProcess


def lambda_handler(event=None, context=None):
    HOST = "35.156.104.103"
    USER = "postgres"
    PASSWORD = "secret"
    PORT = 5432
    DATABASE = "analytics_v2"

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

# print(lambda_handler())
