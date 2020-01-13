import json

from compredict.client import api
from compredict.resources import resources


class Forecast:

    def __init__(self, api_config=None):  # data=None, api_config= None):

        self.__result = None
        self.__client = api.get_instance(token=api_config["COMPREDICT_AI_CORE_KEY"],
                                         callback_url=api_config["COMPREDICT_AI_CORE_CALLBACK_URL"],
                                         url=api_config["COMPREDICT_AI_CORE_BASE_URL"])
        self.__client.fail_on_error(option=api_config["COMPREDICT_AI_CORE_FAIL_ON_ERROR"])

    def __algorithm(self, data, callback=None):  # ,api_config=None):

        print(callback, "This is where the problem comes from*******************************")
        algorithm = self.__client.get_algorithm("base-damage-forecaster")
        results = algorithm.run(data, evaluate=False, encrypt=False, callback_param=callback)

        # if results is False:
        #     print(results.last_error)
        #
        # if isinstance(results, resources.Task):
        #     print(results.job_id)
        if isinstance(results, resources.Task):
            print(results.job_id)
        #     while results.status != results.STATUS_FINISHED:
        #         print("task is not done yet.. waiting...")
        #         sleep(15)
        #         results.update()
        #
        #     if results.success is True:
        #         print(results.predictions)
        #     else:
        #         print(results.error)
        #
        # else:
        #     print(results.predictions)

        return results

    def forecast_checker(self, forecast=None):  # data=None, api_config=None):

        subscript = "damages.%s.damage" % (str(int(forecast["data"]["damages_types"])))
        damages = list(forecast["data"][subscript])
        km = list(forecast["data"]["mileages"])
        callback_param = dict(damage_id=forecast["data"]["idX"], damage_type_id=int(forecast["data"]["damages_types"]),
                              update_date_at=json.dumps(forecast["data"]["updated_at"], indent=4, sort_keys=True, default=str))

        forecasting = {
            'data': {
                'damage': damages,
                'distance': km
            }

        }

        result = self.__algorithm(forecasting, callback_param)  # ,forecast["api_config"])

        mydata = {"reference_id": result.job_id,
                  "status": "Pending",
                  "success": False}

        self.__result = mydata

    def results(self, ):
        return self.__result
