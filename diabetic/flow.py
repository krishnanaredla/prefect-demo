from diabetic.tasks import *
from prefect import Parameter

with Flow(name="diabetic") as flow:
    observations_path = Parameter("observations", default="./data/observations.csv")
    patients_path = Parameter("patients", default="./data/patients.csv")
    conditions_path = Parameter("conditions", default="./data/conditions.csv")
    data = load(observations_path, patients_path, conditions_path)
    processed = transform(data)
