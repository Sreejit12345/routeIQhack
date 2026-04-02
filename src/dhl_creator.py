from api_factory import APICreator
from dhl_api import DHLAPI

class DHLAPICreator(APICreator):
    def factory_method(self):
        return DHLAPI()