from api_factory import APICreator
from fedex_api import FEDEXAPI

class FEDEXAPICreator(APICreator):
    def factory_method(self):
        return FEDEXAPI()