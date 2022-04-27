from diabetic.flow import *

params = {
    "conditions": "./data/conditions.csv",
    "patients": "./data/patients.csv",
    "observations": "./data/observations.csv",
}

state = flow.run(**params)

if state.is_successful():
    print("Completed Successfully")
    task_name = "transform"
    task_ref = flow.get_tasks(name=task_name)[0]
    print(state.result[task_ref].result)
