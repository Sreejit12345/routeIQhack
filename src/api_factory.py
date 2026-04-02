from abc import ABC, abstractmethod
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


class APIFactory(ABC):

    @abstractmethod
    def fetch_secret(self,vault_url, secret_name):
        pass
    
    @abstractmethod
    def generate_bearer_token(self):
        pass

    @abstractmethod
    def call_api(self, df,payload=None):
        pass

    @abstractmethod
    def curate_api_response(self,df=None):
        pass


class APICreator(ABC):
    @abstractmethod
    def factory_method(self):
        pass

