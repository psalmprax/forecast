from compredict.client import api
from compredict.resources import resources
from time import sleep
from environs import Env
from sys import exit
import shutil
import json
import sys

env = Env()
env.read_env()

token = env("COMPREDICT_AI_CORE_KEY")
callback_url = env("COMPREDICT_AI_CORE_CALLBACK", None)
fail_on_error = env("COMPREDICT_AI_CORE_FAIL_ON_ERROR", False)
ppk = env("COMPREDICT_AI_CORE_PPK", None)
passphrase = env("COMPREDICT_AI_CORE_PASSPHRASE", "")

client = api.get_instance(token="d6621e57cc9a79e5557b16e7af6ed5ff7c35678d", callback_url=callback_url, ppk=ppk, passphrase=passphrase,
                          url="https://aic.compredict.de/api/v1")
client.fail_on_error(option=False)

# get a graph
# algorithm = client.get_algorithm("auto-parameterization")
# graph = algorithm.get_detailed_graph()
# new_file = open('auto-parameterization-graph.png', 'wb')
# shutil.copyfileobj(graph, new_file)
# graph.close()

with open("base-damage-forecaster.json", "r") as f:
    data = json.load(f)

# encrypted_msg = client.RSA_encrypt(json.dumps(data))
# result = client.RSA_decrypt(encrypted_msg)

algorithm = client.get_algorithm("base-damage-forecaster")
callback_param = dict(damage_id=5, damage_type=1)
results = algorithm.run(data, evaluate=False, encrypt=False, callback_param=callback_param)

# algorithms = client.get_algorithms()
#
# # Check if the user has algorithms to predict
# if len(algorithms) == 0:
#     print("No algorithms to proceed!")
#     exit()
#
# algorithm = algorithms[0]
#
# tmp = algorithm.get_detailed_template()
# tmp.close()  # It is tmp file. close the file to remove it.
#
# data = dict()  # data for predictions
#
# results = algorithm.run(data, evaluate=False, encrypt=True)
#
print(results)
if results is False:
    print("failed to predict with error", client.last_error)
elif isinstance(results, resources.Task):
    print(results.job_id)

    while results.status != results.STATUS_FINISHED:
        print("task is not done yet.. waiting...")
        sleep(15)
        results.update()

    if results.success is True:
        print(results.predictions)
    else:
        print(results.error)
else:
    print(results.results)


