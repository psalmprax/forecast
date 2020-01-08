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

        # return results

        return results

    def forecast_checker(self, forecast=None):  # data=None, api_config=None):

        subscript = "damages.%s.damage" % (str(int(forecast["data"]["damages_types"])))
        damages = list(forecast["data"][subscript])
        km = list(forecast["data"]["mileages"])
        callback_param = dict(damage_id=forecast["data"]["idX"], damage_type_id=int(forecast["data"]["damages_types"]))

        forecasting = {
            "data": {
                "damage": damages,
                "distance": km
            }

        }

        try:
            # forecasting = json.dumps(forecasting, sort_keys=True, indent=4, separators=(',', ': '))
            # forecasting = json.loads(forecasting, strict=False)
            print(forecasting)

            forecasting = json.dumps(forecasting, sort_keys=True, indent=4, separators=(',', ': '))
            print(forecasting)
            # forecasting = json.loads(forecasting, strict=True)
            # print(forecasting)

        except ValueError as e:
            print("This is not a right json format")

        result = self.__algorithm(forecasting, callback_param)  # ,forecast["api_config"])

        mydata = {"reference_id": result.job_id,
                  "status": "Pending",
                  "success": False}

        self.__result = mydata

    def results(self, ):
        return self.__result
