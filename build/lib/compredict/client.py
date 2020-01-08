from .singleton import Singleton
from .connection import Connection
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from compredict.resources import resources
from json import dumps as json_dump
import base64

from .exceptions import ClientError


@Singleton
class api:

    def __init__(self, token=None, callback_url=None, ppk=None, passphrase=None, url=None):
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

        self.callback_url = callback_url
        url = api.BASE_URL.format(api.API_VERSION) if url is None else url
        self.connection = Connection(url, token=token)
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

    def run_algorithm(self, algorithm_id, data, evaluate=True, encrypt=False, callback_url=None, callback_param=None):
        """
        Run the given algorithm id with the passed data. The user have the ability to toggle encryption and evaluation.

        :param algorithm_id: String identifier of the algorithm
        :param data: JSON format of the data given with the correct keys as specified in the algorithm's template.
        :param evaluate: Boolean to whether evaluate the results of predictions or not.
        :param encrypt: Boolean to encrypt the data if the data is escalated to queue or not.
        :param callback_url: The callback url that will override the callback url in the class.
        :param callback_param: The callback additional parameter to be sent back when requesting the results.
        :return: Prediction if results are return instantly or Task otherwise.
        """
        if encrypt is True and self.rsa_key is None:
            raise ClientError("Please supply private key to encrypt the data")

        callback_url = callback_url if callback_url is not None else self.callback_url
        params = dict(evaluate=self.__process_evaluate(evaluate), encrypt=encrypt,
                      callback_url=callback_url, callback_param=json_dump(callback_param))
        data = json_dump(data)
        if encrypt:
            data = self.RSA_encrypt(data)
        files = {"features": ('features.json', data)}
        response = self.connection.POST('/algorithms/{}/predict2'.format(algorithm_id), data=params, files=files)
        resource = 'Task' if response is not False and 'job_id' in response else 'Result'
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

    def RSA_encrypt(self, msg, chunk_size=214):
        """
        Encrypt the message by the provided RSA public key.

        :param msg: message that to be encrypted
        :type msg: string
        :param chunk_size: the chunk size used for PKCS1_OAEP decryption, it is determined by \
        the private key length used in bytes - 42 bytes.
        :type chunk_size: int
        :return: Base 64 encryption of the encrypted message
        :rtype: binray
        """
        if self.rsa_key is None:
            raise Exception("Path to private key should be provided to decrypt the response.")

        encrypted = b''
        offset = 0
        end_loop = False

        while not end_loop:
            chunk = msg[offset:offset + chunk_size]

            if len(chunk) % chunk_size != 0:
                chunk += " " * (chunk_size - len(chunk))
                end_loop = True

            encrypted += self.rsa_key.encrypt(chunk.encode())
            offset += chunk_size

        return base64.b64encode(encrypted)

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
