import os
from importlib import reload

import osw.model.entity as model
from osw.core import OSW
from osw.wtsite import WtSite

from battmo_prefect_flow import run_performance_spec, PerformanceSpecRequest, PerformanceSpecResponse

from prefect import flow, task
from prefect.blocks.system import Secret
import random

from pydantic import BaseModel
from typing import List, Optional, Any
import uuid

from zenodo_client import Creator, Metadata, ensure_zenodo

import json
import os
import logging
import requests
import sys
# caution: path[0] is reserved for script path (or '' in REPL)
sys.path.insert(1, '/root/flows/big-map-archive-api-client')
from create_and_share_records import prepare_output_file, upload_record, publish_record, save_to_file


#os.environ["PATH"] = "/home/jovyan/.local/bin" #PATH=$PATH:~/.local/bin/
os.environ["PATH"] += os.pathsep + "/home/jovyan/.local/bin" # ensure datamodel-codegen is found
#os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"


class ConnectionSettings(model.OswBaseModel):
    osw_user_name: Optional[str] = "OnterfaceBot"
    osw_domain: Optional[str] = "onterface.open-semantic-lab.org"

@task
def connect(settings: ConnectionSettings):
    global wtsite
    # fetch secret from calculated name
    password = Secret.load(settings.osw_user_name.lower() + '-' + settings.osw_domain.replace('.','-'))
    wtsite = WtSite.from_domain(settings.osw_domain, None, {"username": settings.osw_user_name, "password": password.get()})
    global osw
    osw = OSW(site=wtsite)

@task
def fetch_schema():
    # osw.fetch_schema() #this will load the current entity schema from the OSW instance.
    # You may have to re-run the script to get the updated schema extension.
    # Requires 'pip install datamodel-code-generator'
    list_of_categories = [
        #"Category:OSWb79812225c7849b78e98f2b3b10498b3", # WorkflowRun
        #"Category:OSW553f78cc66194ae1873241207b906c4b", # BattMoModel
    ]
    for i, cat in enumerate(list_of_categories):
        mode = "append"
        if i == 0:
            mode = "replace"
        osw.fetch_schema(OSW.FetchSchemaParam(schema_title=cat, mode=mode))
    #reload(model)

@task
def query_pending_requests():
    print("Query")
    models = wtsite.semantic_search("[[Category:OSW553f78cc66194ae1873241207b906c4b]][[HasWorkflowRuns.HasStatus::Item:OSWaa8d29404288446a9f3ec7afa4e2a512]]")
    return models

class Result(model.OswBaseModel):
    battmo_result: PerformanceSpecResponse
    battmo_model: Any #model.BattmoModel

@task
def store_and_document_result(result: Result):
    print(result)
    title = "Item:" + osw.get_osw_id(result.battmo_model.uuid)
    model_entity = osw.load_entity(title).cast(model.BattmoModel)
    model_entity.performance = { "uuid": uuid.uuid4(), "energyDensity": result.battmo_result.result.energyDensity}
    #if (not model_entity.statements): model_entity.statements = []
    #model_entity.statements.append(model.Statement(
    #  quantity = "Property:HasEnergyDensity",
    #  numerical_value = str(result.battmo_result.result.energyDensity),
    #  unit = "Property:HasEnergyDensity#OSW5b8270a6329d4831b414a5ef5d4ca946",
    #  unit_symbol = "J/m³",
    #  value = str(result.battmo_result.result.energyDensity) + " J/m³"
    #));
    for run in model_entity.workflow_runs:
        if (run.uuid == result.battmo_result.uuid):
            run.status = "Item:OSWf474ec34b7df451ea8356134241aef8a"
            print(run.uuid)
            break
    osw.store_entity(model_entity)
    print("FINISHED")

    
class SimulationRequest(model.OswBaseModel):
    model_titles: List[str]
    osw_instance: Optional[str] = "onterface.open-semantic-lab.org"
    
@flow(validate_parameters=True) # validation will fail due to model.entity class
def schedule_simulation_requests(request: SimulationRequest):
    connect(ConnectionSettings(domain=request.osw_instance))
    fetch_schema()
    if not request.model_titles:
        request.model_titles = query_pending_requests()
    m = None
    uuid = None
    for title in request.model_titles:
        model_entity = osw.load_entity(title)
        #model_entity.uuid
        model_entity = model_entity.cast(model.BattmoModel)
        for run in model_entity.workflow_runs:
            if (run.status == "Item:OSWaa8d29404288446a9f3ec7afa4e2a512" # ToDo
                and run.tool
                and "Item:OSWe7c08b2300f04d0bbb0a55bca8838437" in run.tool # BattmoSimulation
               ):
                m = model_entity
                uuid = run.uuid
                print(run.uuid)
                break
    if (m):
        print(m.geometry)
        result = run_performance_spec(PerformanceSpecRequest(
            geometry = m.geometry,
            uuid = uuid
        ))
        store_and_document_result(Result(
            battmo_result = result,
            battmo_model = m
        ))
        #store_and_document_results.submit(wait_for=flowA)
        
@task()
def create_bigmaparchive_record(model_entity: model.BattmoModel):
    #url = "https://archive.big-map.eu/"
    url = "https://big-map-archive-demo.materialscloud.org/"

    # Navigate to 'Applications' > 'Personal access tokens' to create a token if necessary
    token = Secret.load("big-map-archive-demo-api-key").get();

    records_path = 'records'
    links_filename = 'records_links.json'
    
    file_name = os.path.join(records_path, '0', str(model_entity.uuid) + '.json')
    with open(file_name, "w", encoding="utf-8") as f:
        f.write(model_entity.json(exclude_none=True))
    
    # Define the metadata that will be used on initial upload
    label = "BattMo Model" 
    if (model_entity.label and model_entity.label[0] and model_entity.label[0].text):
        label = model_entity.label[0].text
    description = "Demo data" 
    if (model_entity.description and model_entity.description[0] and model_entity.description[0].text): 
        description = model_entity.description[0].text
    
    data = {
      "access": {
        "record": "public",
        "files": "public"
      },
      "files": {
        "enabled": True
      },
      "metadata": {
        "creators": [
          {
            "person_or_org": {
              "family_name": " Bot",
              "given_name": "Onterface",
              "type": "personal"
            }
          }
        ],
        "publication_date": "2022-11-28",
        "resource_type": { "id": "dataset" },
        "title": label,
        "version": "v1"
      }
    }
    with open(os.path.join(records_path, '0', 'record_metadata.json'), "w", encoding="utf-8") as f:
        f.write(json.dumps(data))

    # Specify records that you want to upload:
    # ('<record metadata json>.json', ['<datafile1>', '<datafile2>'])
    records = [
        ('record_metadata.json', [str(model_entity.uuid) + '.json'])
    ]

    publish = True

    logger = logging.getLogger()
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    try:
        prepare_output_file(records_path, links_filename)

        record_index = 0
        for record in records:
            logger.info('----------Start uploading record ' + str(record_index) + '----------')
            record_links = upload_record(url, records_path, record, record_index, token)

            if publish:
                # Publish the draft record
                publish_record(record_links, token)

            # Save the record's links to a file
            save_to_file(records_path, links_filename, record_links)

            record_index += 1
        logger.info('Uploading was successful. See the file records_links.json for links to the created records.')
    except Exception as e:
        logger.error('Error occurred: ' + str(e))
    
    print(record_links)
    if not model_entity.repository_records: model_entity.repository_records = []
    model_entity.repository_records.append(model.Repository(
        repository_name="BIG-MAP Archive",
        record_pid=record_links['record_html'].split('/')[-1],
        record_link=record_links['record_html']
    ))
    return model_entity
        
@task()
def create_zenodo_record(model_entity: model.BattmoModel):
    os.environ["ZENODO_SANDBOX_API_TOKEN"] = Secret.load("zenodo-sandbox-api-token").get();
    
    # Define the metadata that will be used on initial upload
    label = "BattMo Model" 
    if (model_entity.label and model_entity.label[0] and model_entity.label[0].text):
        label = model_entity.label[0].text
    description = "Demo data" 
    if (model_entity.description and model_entity.description[0] and model_entity.description[0].text): 
        description = model_entity.description[0].text
    
    file_name = str(model_entity.uuid) + '.json'
    with open(file_name, "w", encoding="utf-8") as f:
        f.write(model_entity.json(exclude_none=True))
        
    data = Metadata(
        title=label,
        upload_type='dataset',
        description=description,
        creators=[
            Creator(
                name='Onterface Bot',
                affiliation='Onterface',
            ),
        ],
    )
    res = ensure_zenodo(
        key=str(model_entity.uuid),  # this is a unique key you pick that will be used to store
                      # the numeric deposition ID on your local system's cache
        data=data,
        paths=[
            file_name,
        ],
        sandbox=True,  # remove this when you're ready to upload to real Zenodo
    )
    from pprint import pprint
    res = res.json()
    pprint(res)
    model_entity.doi = res['doi']
    return model_entity
        
class PublishRequest(model.OswBaseModel):
    model_titles: List[str]
    osw_instance: Optional[str] = "onterface.open-semantic-lab.org",
    repository: Optional[str] = "sandbox.zenodo.org"
    
@flow() # validation will fail due to model.entity class
def publish_results(request: PublishRequest):
    
    connect(ConnectionSettings(domain=request.osw_instance))
    fetch_schema()
    for title in request.model_titles:
        model_entity = osw.load_entity(title)
        #model_entity.uuid
        model_entity = model_entity.cast(model.BattmoModel)
       
    if request.repository == "sandbox.zenodo.org": model_entity = create_zenodo_record(model_entity)

    osw.store_entity(model_entity)
    
class BigMapPublishRequest(model.OswBaseModel):
    model_titles: List[str]
    osw_instance: Optional[str] = "onterface.open-semantic-lab.org",
    repository: Optional[str] = "big-map-archive-demo.materialscloud.org"
    
@flow()
def publish_results_bigmap(request: BigMapPublishRequest):
    
    connect(ConnectionSettings(domain=request.osw_instance))
    fetch_schema()
    for title in request.model_titles:
        model_entity = osw.load_entity(title)
        #model_entity.uuid
        model_entity = model_entity.cast(model.BattmoModel)
       
    if request.repository == "big-map-archive-demo.materialscloud.org": model_entity = create_bigmaparchive_record(model_entity)

    osw.store_entity(model_entity)
    
if __name__ == "__main__":
    #schedule_pending_requests(SimulationRequest(
    #    model_titles = ["Item:OSWfaa2ed87af914357ad6ff5110ef1632f"]
    #))
    #schedule_optimization_requests(OptimizationRequest(
    #    model_titles = ["Item:OSW57814a41a59c4411a6bd5f4f13009972"]
    #))
    publish_results(OptimizationRequest(
        model_titles = ["Item:OSWa600b56fa5c1453aa62e0b867ee9d42f"]
    ))
    #publish_results_bigmap(BigMapPublishRequest(
    #    model_titles=["Item:OSW58b04b26f6484a8cbddd5c6b70cad5bc"]
    #))