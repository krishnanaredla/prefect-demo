from diabetic.tasks import *
from prefect import Parameter,Flow

with Flow(name="diabetic") as diabetic_flow:
    observations_path = Parameter("observations", default="./data/observations.csv")
    patients_path = Parameter("patients", default="./data/patients.csv")
    conditions_path = Parameter("conditions", default="./data/conditions.csv")
    data = load(observations_path, patients_path, conditions_path)
    param_grid = Parameter("grid",default={
    })
    processed = transform(data)
    output = Parameter("output")
    model(processed,output,param_grid)
    #deploy(md) 
