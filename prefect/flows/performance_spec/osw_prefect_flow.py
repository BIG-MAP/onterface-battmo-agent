import os
from importlib import reload

import osw.model.entity as model
from osw.core import OSW
from osw.wtsite import WtSite

from battmo_prefect_flow import run_performance_spec, PerformanceSpecRequest, PerformanceSpecResponse
from atinary_prefect_flow import run_geometry_optimization, ExecutedExperiment, BattmoOptimizationRequest, BattmoOptimizationResult
from prefect import flow, task
from prefect.blocks.system import Secret

from pydantic import BaseModel
from typing import List, Optional
import uuid

from zenodo_client import Creator, Metadata, ensure_zenodo

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
    battmo_model: model.BattmoModel
    
class OptimizationResult(model.OswBaseModel):
    atinary_result: BattmoOptimizationResult
    battmo_model: model.BattmoModel

@task
def store_and_document_result(result: Result):
    #print(result.battmo_result)
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
    
@task
def store_and_document_optimization_result(result: OptimizationResult):
    #print(result.battmo_result)
    title = "Item:" + osw.get_osw_id(result.battmo_model.uuid)
    model_entity = osw.load_entity(title).cast(model.BattmoModel)
    model_entity.geometry = result.atinary_result.best_run.geometry
    model_entity.performance = { "uuid": uuid.uuid4(), "energyDensity": result.atinary_result.best_run.spec_response.result.energyDensity}
    #if (not model_entity.statements): model_entity.statements = []
    #model_entity.statements.append(model.Statement(
    #  quantity = "Property:EnergyDensity",
    #  numerical_value = str(result.battmo_result.result.energyDensity),
    #  unit = "Property:EnergyDensity#OSW5b8270a6329d4831b414a5ef5d4ca946",
    #  unit_symbol = "J/m³",
    #  value = str(result.battmo_result.result.energyDensity) + " J/m³"
    #));
    for run in model_entity.workflow_runs:
        if (run.uuid == result.atinary_result.best_run.spec_response.uuid):
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
        
class OptimizationRequest(model.OswBaseModel):
    model_titles: List[str]
    osw_instance: Optional[str] = "onterface.open-semantic-lab.org"
    
@flow(validate_parameters=True) # validation will fail due to model.entity class
def schedule_optimization_requests(request: OptimizationRequest):
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
                and "Item:OSWb80747f1ccf340d790955572d27f678c" in run.tool # BattmoAtinaryOptimization
               ):
                m = model_entity
                uuid = run.uuid
                print(run.uuid)
                break
    if (m):
        print(m.geometry)
        
        result = run_geometry_optimization(request=BattmoOptimizationRequest(
            #geometry = m.geometry,
            uuid = uuid,
            #budget = 10
        ))
        store_and_document_optimization_result(OptimizationResult(
            atinary_result = result,
            battmo_model = m
        ))
        #store_and_document_results.submit(wait_for=flowA)
        
class PublishRequest(model.OswBaseModel):
    model_titles: List[str]
    osw_instance: Optional[str] = "onterface.open-semantic-lab.org"
    
@flow() # validation will fail due to model.entity class
def publish_results(request: PublishRequest):
    os.environ["ZENODO_SANDBOX_API_TOKEN"] = Secret.load("zenodo-sandbox-api-token").get();
    connect(ConnectionSettings(domain=request.osw_instance))
    fetch_schema()
    for title in request.model_titles:
        model_entity = osw.load_entity(title)
        #model_entity.uuid
        model_entity = model_entity.cast(model.BattmoModel)
        
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
    osw.store_entity(model_entity)
    
if __name__ == "__main__":
    #schedule_pending_requests(SimulationRequest(
    #    model_titles = ["Item:OSWfaa2ed87af914357ad6ff5110ef1632f"]
    #))
    #schedule_optimization_requests(OptimizationRequest(
    #    model_titles = ["Item:OSWfaa2ed87af914357ad6ff5110ef1632f"]
    #))
    publish_results(OptimizationRequest(
        model_titles = ["Item:OSW4539df4e82524c53b4a86ea66a702364"]
    ))