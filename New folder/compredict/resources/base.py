from compredict import client


class BaseResource(object):

    def __init__(self, **kwargs):
        """
        The base resource of all resources in the SDK. The class will:

        - Copy the dictionary result to self.
        - Get the client instance for API calling.

        :param kwargs: The results of the request.
        """
        self.__dict__.update(kwargs)
        self.client = client.api.get_instance()

    def __getattr__(self, item):
        return self.item if item in self.__dict__ else None
