import json
import sys
from time import sleep

from environs import Env

from compredict.client import api
from compredict.resources import resources

env = Env()
env.read_env()

token = env("COMPREDICT_AI_CORE_KEY")
callback_url = env("COMPREDICT_AI_CORE_CALLBACK", None)
fail_on_error = env("COMPREDICT_AI_CORE_FAIL_ON_ERROR", False)
ppk = env("COMPREDICT_AI_CORE_PPK", None)
passphrase = env("COMPREDICT_AI_CORE_PASSPHRASE", "")

client = api.get_instance(token="a0ebfefcb8d21ce4fba0b18a645c41c770ae2673", callback_url=callback_url, ppk=ppk,
                          passphrase=passphrase,
                          url="http://localhost:8800/api/v1")
client.fail_on_error(option=False)

# get a graph
# algorithm = client.get_algorithm("auto-parameterization")
# graph = algorithm.get_detailed_graph()
# new_file = open('auto-parameterization-graph.png', 'wb')
# shutil.copyfileobj(graph, new_file)
# graph.close()

with open("base-damage-forecaster.json", "r") as f:
    # with open("test_observer.parquet", "rb") as f:
    data_raw = f.read()
    data = json.loads(data_raw)

# encrypted_msg = client.RSA_encrypt(data_raw)
# decrypred_msg = client.RSA_decrypt(encrypted_msg, to_bytes=False)
# with open("test_observer_decrypted.parquet", "wb") as f:
#     f.write(decrypred_msg.decode)

# import pandas as pd
# df = pd.read_parquet("test_observer_decrypted.parquet")
# print(df.head)
# result = client.RSA_decrypt(encrypted_msg)


algorithm = client.get_algorithm("base-damage-forecaster")
if algorithm is False:
    print(client.last_error)
    sys.exit()
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
