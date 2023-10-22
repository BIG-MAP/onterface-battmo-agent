# build: /home/jovyan/.local/bin/prefect deployment build -n performance_spec -p default-agent-pool -sb local-file-system/local -q test BattMo-PrefectFlow.py:run_performance_spec
# deploy: /home/jovyan/.local/bin/prefect deployment apply run_performance_spec-deployment.yaml 
# run: /home/jovyan/.local/bin/prefect agent start -p 'default-agent-pool'

from prefect import flow, task
from pydantic import BaseModel, Field
from uuid import UUID, uuid4
#from loguru import logger
from typing import Union, Optional


import copy

from oct2py import octave
import os
import fnmatch
import json



class TestRequest(BaseModel):
    text: str

class ActiveMaterialClass(BaseModel):
    thickness: float
    
class SeparatorClass(BaseModel):
    thickness: float
    
class NegativeElectrodeClass(BaseModel):
    ActiveMaterial: ActiveMaterialClass
    
class PositiveElectrodeClass(BaseModel):
    ActiveMaterial: ActiveMaterialClass
    
class ElectrolyteClass(BaseModel):
    Separator: SeparatorClass

class Geometry1D(BaseModel):
    format: Optional[str] = "1D"
    faceArea: Optional[float] = 1e-4
    NegativeElectrode: Optional[NegativeElectrodeClass] = NegativeElectrodeClass(ActiveMaterial = ActiveMaterialClass(thickness = 64e-6))
    PositiveElectrode: Optional[PositiveElectrodeClass] = PositiveElectrodeClass(ActiveMaterial = ActiveMaterialClass(thickness = 57e-6))
    Electrolyte: Optional[ElectrolyteClass] = ElectrolyteClass(Separator = SeparatorClass(thickness = 15e-6))
    
class PerformanceSpec(BaseModel):
    E: Optional[float]
    energyDensity: Optional[float]
    energy: Optional[float]
    
class PerformanceSpecRequest(BaseModel):
    geometry: Optional[Geometry1D] = Geometry1D()
    uuid: UUID = Field(default_factory=uuid4, title="UUID")
    
class PerformanceSpecResponse(BaseModel):
    status: Optional[str] = "ok"
    uuid: UUID
    result: PerformanceSpec

@flow
def run_performance_spec(request: PerformanceSpecRequest):
    octave.run('/root/flows/BattMo/startupBattMo.m')
    
    print(request.geometry.json())
    
    battmo_input = f'/root/flows/BattMo/Examples/{str(request.uuid)}.json'
    battmo_output = f'/root/flows/BattMo/Examples/{str(request.uuid)}.json' 
    
    with open(battmo_input, "w") as f:
        f.write(request.geometry.json())
        
    # run the simulation
    battmo_input = f'Examples/{str(request.uuid)}.json'
    #E, energyDensity, energy = octave.runJsonFunction({'ParameterData/BatteryCellParameters/LithiumIonBatteryCell/lithium_ion_battery_nmc_graphite.json', battmo_input}, battmo_output, nout=3)
    E, energyDensity, energy = octave.feval('runJsonFunction', {'ParameterData/BatteryCellParameters/LithiumIonBatteryCell/lithium_ion_battery_nmc_graphite.json', battmo_input}, battmo_output, True, nout=3)
    response = PerformanceSpecResponse(
        uuid=request.uuid,
        result=PerformanceSpec(E = E[:,0][-1], energyDensity = energyDensity[:,0][-1], energy = energy[:,0][-1])
    )
    return response

if __name__ == "__main__":
     run_performance_spec(request=PerformanceSpecRequest())