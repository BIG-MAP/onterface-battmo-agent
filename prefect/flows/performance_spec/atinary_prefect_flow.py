# export USER_API_KEY=<api key from sdlabs platform>
# pip install sdlabs_sdks/
# pip install "sdlabs_wrapper@git+https://github.com/Atinary-technologies/sdlabs_wrapper@v0.5.0"
# build: //home/jovyan/.local/bin/prefect deployment build -n performance_opt -p default-agent-pool -sb local-file-system/local -q test atinary_prefect_flow.py:run_geometry_optimization
# deploy: /home/jovyan/.local/bin/prefect deployment apply run_geometry_optimization-deployment.yaml
# run: /home/jovyan/.local/bin/prefect agent start -p 'default-agent-pool'

from prefect import flow, task
from pydantic import BaseModel, Field
from uuid import UUID, uuid4
#from loguru import logger
from typing import Union, Optional,List
import sdlabs_wrapper.models as sdlabs_models
from sdlabs_wrapper.wrapper import initialize_optimization
import copy
import battmo_prefect_flow as battmo
from prefect.blocks.system import Secret
import os
import fnmatch
import json


class ExecutedExperiment(BaseModel):
    geometry: Optional[battmo.Geometry1D] = battmo.Geometry1D()
    spec_response: Optional[battmo.PerformanceSpecResponse] = None
    batch: Optional[int] = None
    iteration: Optional[int] = None

class BattmoOptimizationRequest(BaseModel):
    uuid: UUID = Field(default_factory=uuid4, title="UUID")
    budget: int = Field(10, ge=1, le=20)
    batch_size: int = Field(1, ge=1, le=8)
    optimizer: str = Field("gpyopt", regex="^(randomsearch|grid|dragonfly|gpyopt|sobol|latinhypercube|hyperopt)$")
    random_seed: int = Field(10,ge=1,le=1e6)
    
class BattmoOptimizationResult(BaseModel):
    experiments: List[ExecutedExperiment]
    best_run: ExecutedExperiment
@task(log_prints=True)
def start_optimization(request:BattmoOptimizationRequest):
    
    secret_block = Secret.load("atinary-api-key")

    # Access the stored secret
    api_key = secret_block.get()

    optimization_config = {
        "optimization_name": "BattmoSimulationPrefectFlow",
        "description": "Optimize cell layer thicknesses",
        "sdlabs_group_id": "onterface",
        "sdlabs_account_type": "academic",
        "parameters": [
            {
                "low_value": 30e-6,
                "high_value": 150e-6,
                "name": "negative_electrode_thickness"
            },
            {
                "low_value": 30e-6,
                "high_value": 150e-6,
                "name": "positive_electrode_thickness"
            },
            {
                "low_value": 8e-6,
                "high_value": 15e-6,
                "name": "separator_thickness"
            }
        ],
        "objectives": [
            {
                "name": "energy_density",
                "goal": "max"
            }
        ],
        "inherit_data": False,
        "always_restart": True,
        "batch_size": request.batch_size,
        "algorithm": request.optimizer,
        "budget": request.budget,
        "random_seed": request.random_seed,
    }
    print(f"Optimization config {optimization_config}")
    opt_wrapper = initialize_optimization(spec_file_content=optimization_config,api_key=api_key)
    return opt_wrapper

@flow(log_prints=True)
def run_geometry_optimization(request: BattmoOptimizationRequest=BattmoOptimizationRequest()):
    """Create a fixed optimization config that will optimize for the thickness parameters
    At every iteration call SDLabs to get a new suggestion and use this suggestion in the performance spec calculation"""
    
    opt_wrapper = start_optimization(request)
    print("Initialized optimization")
    
    experiments: List[ExecutedExperiment] = []
    best_experiment:ExecutedExperiment = None
    for iteration in range(opt_wrapper.config.budget):
        print("Getting new suggestions...")
        suggestions = opt_wrapper.get_new_suggestions(max_retries=6, sleep_time_s=30)
        print(f"Suggestions found: {suggestions}")
        for batch,suggestion in enumerate(suggestions):
            # Create performance spec request
            print("Creating geometry")
            geometry = battmo.Geometry1D(
                NegativeElectrode=battmo.NegativeElectrodeClass(
                    ActiveMaterial=battmo.ActiveMaterialClass(thickness=suggestion.param_values["negative_electrode_thickness"])),
                PositiveElectrode=battmo.PositiveElectrodeClass(
                    ActiveMaterial=battmo.ActiveMaterialClass(thickness=suggestion.param_values["positive_electrode_thickness"])),
                Electrolyte=battmo.ElectrolyteClass(
                    Separator=battmo.SeparatorClass(thickness=suggestion.param_values["separator_thickness"]))
            )
            request = battmo.PerformanceSpecRequest(geometry=geometry)
            print(f"Sending request {request}")
            # call performance spec flow
            response:battmo.PerformanceSpecResponse = battmo.run_performance_spec(request)
            print(f"Obtained response {response}")
            suggestion.measurements = {
            # get energy density from PerformanceSpecResponse
            "energy_density": float(response.result.energyDensity),
            }
            print(suggestion)
            experiment = ExecutedExperiment(geometry=geometry,spec_response=response)
            if not best_experiment or experiment.spec_response.result.energyDensity > best_experiment.spec_response.result.energyDensity:
                best_experiment = experiment
                print(f"New best experiment {best_experiment}")
            experiments.append(experiment)
        if suggestions:
            print("Sending response back to sdlabs")
            opt_wrapper.send_measurements(suggestions)
    optimization_result = BattmoOptimizationResult(experiments=experiments,best_run=best_experiment)
    return optimization_result
