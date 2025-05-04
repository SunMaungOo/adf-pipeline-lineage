from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import PipelineResource
from typing import Optional,List

class DataFactoryClient:
    def __init__(self,
                 subscription_id:str,\
                 resource_group_name:str,\
                 data_factory_name:str):
        
        self.resource_group_name = resource_group_name

        self.data_factory_name = data_factory_name

        self.client = DataFactoryManagementClient(
            credential=DefaultAzureCredential(),
            subscription_id=subscription_id
        )

    def get_all_pipelines(self)->Optional[List[PipelineResource]]:
        try:
            return self.client.pipelines.list_by_factory(
                resource_group_name=self.resource_group_name,\
                factory_name=self.data_factory_name
            )
        except Exception:
            return None
    

    