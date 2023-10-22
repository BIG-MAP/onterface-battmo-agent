from atinary_prefect_flow import run_geometry_optimization, ExecutedExperiment, BattmoOptimizationRequest, BattmoOptimizationResult

class OptimizationRequest(model.OswBaseModel):
    model_titles: List[str]
    osw_instance: Optional[str] = "onterface.open-semantic-lab.org"
    
class OptimizationResult(model.OswBaseModel):
    atinary_result: BattmoOptimizationResult
    battmo_model: Any #model.BattmoModel

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
            #budget = 10,
            random_seed = random.randint(1,1e6)
        ))
        store_and_document_optimization_result(OptimizationResult(
            atinary_result = result,
            battmo_model = m
        ))
        #store_and_document_results.submit(wait_for=flowA)

@task
def store_and_document_optimization_result(result: OptimizationResult):
    file_name = 'optimization.json'
    with open(file_name, "w", encoding="utf-8") as f:
        f.write(result.json(exclude_none=True))
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