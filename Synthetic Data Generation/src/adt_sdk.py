from azure.digitaltwins.core import DigitalTwinsClient
from azure.identity import AzureCliCredential

import logging
import sys

class ADTInstance(object):
    """ ADTInstance Class built using docs in https://pypi.org/project/azure-digitaltwins-core/
        which compiles a list of available methods for easy usage
    """

    def __init__(self, url, logging_enable=False):
        # Create logger
        if logging_enable:
            self.logger = logging.getLogger('azure')
            self.logger.setLevel(logging.DEBUG)
            handler = logging.StreamHandler(stream=sys.stdout)
            self.logger.addHandler(handler)
        credential = AzureCliCredential()
        self.service_client = DigitalTwinsClient(url, credential, logging_enable=logging_enable)


    #########
    # Create, list, decommission, and delete models

    def create_models(self, dtdl_models_list):
        models = self.service_client.create_models(dtdl_models_list)
        print('### Created Models:')
        print(models)

    def list_models(self):
        listed_models = self.service_client.list_models() #returns azure.core.paging.ItemPaged instance
        for i, model in enumerate(listed_models):
            print("\n model ",i)
            print(model)

    def get_model(self, model_id):
        get_model = self.service_client.get_model(model_id)
        print(f'### Get Model with id:{model_id}')
        print(get_model)
        return(get_model)

    def decommission_model(self, model_id):
        self.service_client.decommission_model(model_id)

    def delete_model(self, model_id):
        self.service_client.delete_model(model_id)

        #########
    # Create,get, query and delete digital twins

    def upsert_digital_twin(self,digital_twin_id, twin_json):
        created_twin = self.service_client.upsert_digital_twin(digital_twin_id, twin_json)
        print('### Created Digital Twin:')
        print(created_twin)
        return(created_twin)

    def get_digital_twin(self, digital_twin_id):
        get_twin = self.service_client.get_digital_twin(digital_twin_id)
        print('### Get Digital Twin:')
        print(get_twin)
        return(get_twin)

    def query_twins(self, query_expression):
        query_result = self.service_client.query_twins(query_expression)
        print('### DigitalTwins:')
        for twin in query_result:
            print(twin)
        return(query_result)

    def delete_digital_twin(self, digital_twin_id):
        self.service_client.delete_digital_twin(digital_twin_id)
        print(f'### Twin {digital_twin_id} is deleted')

    #########
    # Get and update digital twin
    def get_component(self, digital_twin_id, component_name):
        get_component = self.service_client.get_component(digital_twin_id, component_name)
        print('### Get Component:')
        print(get_component)

    def update_component(self, digital_twin_id, component_name, patch):
        self.service_client.update_component(digital_twin_id, component_name, patch)
    
    def update_digital_twin(self, digital_twin_id, patch):
        self.service_client.update_digital_twin(digital_twin_id, patch)

    #########
    # Create and list digital twin relationships
    def upsert_relationship(self, relationship):
        self.service_client.upsert_relationship(relationship["$sourceId"], relationship["$relationshipId"], relationship)
        print(f'### Created relationship with id{relationship["$relationshipId"]}')

    def list_relationships(self, digital_twin_id):
        relationships = self.service_client.list_relationships(digital_twin_id)
        for relationship in relationships:
            print(relationship)

    def list_incoming_relationships(self, digital_twin_id):
        incoming_relationships = self.service_client.list_incoming_relationships(digital_twin_id)
        for incoming_relationship in incoming_relationships:
            print(incoming_relationship)


    #########
    # Create, list, and delete event routes of digital twins
    #TODO

