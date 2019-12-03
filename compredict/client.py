import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from .singleton import Singleton
from .connection import Connection
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from compredict.resources import resources
from json import dumps as json_dump
import base64


@Singleton
class api:

    def __init__(self, token=None, callback_url=None, ppk=None, passphrase=None, base_url=None):
        """
        COMPREDICT's AI Core Client that will provide an interface for communication. This class is singleton.

        :param token: API Key used for authorization.
        :param callback_url: URL for sending the results of long processes.
        :param ppk: Path to private key for decrypted requests responses (optional, only valid if public key is given \
        in dashboard)
        :param passphrase: Password to the private key.
        """
        if token is not None and len(token) != 40:
            raise Exception("API Key is not in valid format!")

        self.__set_base_url(base_url=base_url)

        self.callback_url = callback_url
        self.connection = Connection(api.BASE_URL.format(api.API_VERSION), token=token)
        self.rsa_key = None
        if ppk is not None:
            self.set_ppk(ppk, passphrase)

    def fail_on_error(self, option=True):
        """
        Ability to choose whether to raise exception on receiving error or return false.

        :param option: Boolean, True is to raise exception otherwise return false on error.
        :return: None
        """
        self.connection.fail_on_error = option

    def set_ppk(self, ppk, passphrase=''):
        """
        Load the private key from the path and set the correct padding scheme.

        :param ppk: path to private key
        :param passphrase: password of the private key if any
        :return: None
        """
        with open(ppk) as f:
            self.rsa_key = RSA.importKey(f.read(), passphrase=passphrase)
            self.rsa_key = PKCS1_OAEP.new(self.rsa_key)
        pass

    def verify_peer(self, option):
        """
        Prompt SSL connection

        :param option: Boolean True/False
        :return:
        """
        self.connection.ssl = option

    @property
    def last_error(self):
        return self.connection.last_error

    def __map_resource(self, resource, a_object):
        """
        Map the result to the correct resource

        :param resource: String name to the resource
        :param a_object: The values returned from the request.
        :return: New class of the resources with the response values.
        """
        if a_object is False:
            return a_object
        try:
            model_class = getattr(resources, resource)
            instance = model_class(**a_object)
        except(AttributeError, ModuleNotFoundError):
            raise ImportError("Resource {} was not found".format(resource))
        return instance

    def __map_collection(self, resource, objects):
        """
        Create a list of resources if the results returns a list

        :param resource: String name to the resource
        :param objects: The list of values returned from the request.
        :return: List of instances of the given resource
        """
        if objects is False:
            return objects

        try:
            instances = list()
            for obj in objects:
                model_class = getattr(resources, resource)
                instances.append(model_class(**obj))
        except(AttributeError, ModuleNotFoundError):
            raise ImportError("Resource {} was not found".format(resource))
        return instances

    def get_algorithms(self):
        """
        Returns the collection of algorithms

        :return: list of algorithms
        """
        response = self.connection.GET('/algorithms')
        return self.__map_collection('Algorithm', response)

    def get_algorithm(self, algorithm_id):
        """
        Get the information of the given algorithm id

        :param algorithm_id: String identifier of the algorithm
        :return: Algorithm resource
        """
        response = self.connection.GET('/algorithms/{}'.format(algorithm_id))
        return self.__map_resource('Algorithm', response)

    def run_algorithm(self, algorithm_id, data, evaluate=True, encrypt=False):
        """
        Run the given algorithm id with the passed data. The user have the ability to toggle encryption and evaluation.

        :param algorithm_id: String identifier of the algorithm
        :param data: JSON format of the data given with the correct keys as specified in the algorithm's template.
        :param evaluate: Boolean to whether evaluate the results of predictions or not.
        :param encrypt: Boolean to encrypt the data if the data is escalated to queue or not.
        :return: Prediction if results are return instantly or Task otherwise.
        """
        params = dict(evaluate=self.__process_evaluate(evaluate), encrypt=encrypt)
        files = {"features": ('features.json', json_dump(data))}
        response = self.connection.POST('/algorithms/{}/predict'.format(algorithm_id), data=params, files=files)
        resource = 'Task' if 'job_id' in response else 'Result'
        return self.__map_resource(resource, response)

    def __process_evaluate(self, evaluate):
        """
        Check the type of evaluate parameter and parse it accordingly.

        :param evaluate: evaluation of the algorithm
        :type evaluate: bool|dict|string
        :return: bool|string
        """
        if isinstance(evaluate, dict):
            return json_dump(evaluate)
        return evaluate

    def get_task_results(self, task_id):
        """
        Check COMPREDICT'S AI Core for the results of the computation.

        :param task_id: String identifier of the job.
        :return: The new results of the Task
        """
        response = self.connection.GET('/algorithms/tasks/{}'.format(task_id))
        return self.__map_resource('Task', response)

    def get_template(self, algorithm_id, file_type='input'):
        """
        Return the template that explains the data to be sent for the algorithms. Bear in mind, to close the file once
        done to delete it.

        :param algorithm_id: String identifier of the Algorithm.
        :param file_type: (default `input`) to retrieve the type of the document. Can be either `input` or `output`
        :return: NamedTemporaryFile of the results.
        """
        response = self.connection.GET('/algorithms/{}/template?type={}'.format(algorithm_id, file_type))
        return response

    def get_graph(self, algorithm_id, file_type):
        """
        Return the graph that explains the input data to be sent for the algorithms.

        :param algorithm_id: String identifier of the Algorithm.
        :param file_type: (default `input`) to retrieve the type of the document. Can be either `input` or `output`
        :return: NamedTemporaryFile of the results.
        """
        response = self.connection.GET('/algorithms/{}/graph?type={}'.format(algorithm_id, file_type))
        return response

    def RSA_decrypt(self, encrypted_msg, chunk_size=256):
        """
        Decrypt the encrypted message by the provided RSA private key.

        :param encrypted_msg: Base 64 encode of The encrypted message.
        :type encrypted_msg: binary
        :param chunk_size: It is determined by the private key length used in bytes.
        :type chunk_size: int
        :return: The decrypted message
        :rtype: string
        """
        if self.rsa_key is None:
            raise Exception("Path to private key should be provided to decrypt the response.")

        encrypted_msg = base64.b64decode(encrypted_msg)

        offset = 0
        decrypted = ""

        while offset < len(encrypted_msg):
            chunk = encrypted_msg[offset:offset + chunk_size]

            decrypted += self.rsa_key.decrypt(chunk).decode()

            offset += chunk_size

        return decrypted

    def get_base_url(self,):
        return api.BASE_URL

    def __set_base_url(self,base_url=None):
        if base_url != None:
            api.BASE_URL = base_url