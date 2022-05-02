from diabetic.flow import *
import streamlit as st
from streamlit import cli as stcli
import os
import streamlit.bootstrap
from streamlit import config as _config
from dask.distributed import Client
from prefect.engine.executors import DaskExecutor

dirname = os.path.dirname(__file__)
filename = os.path.join(dirname, 'diabetic/streamlit.py')

params = {
    "conditions": "C:\work\prefect\diabetic\data\conditions.csv",
    "patients": "C:\work\prefect\diabetic\data\patients.csv",
    "observations": "C:\work\prefect\diabetic\data\observations.csv",
    "grid": {
        "n_estimators": [200, 500],
        "max_features": ["auto", "sqrt", "log2"],
        "max_depth": [4, 5, 6, 7, 8],
        "criterion": ["gini", "entropy"],
    },
    "output" : "C:\work\prefect\diabetic\data\output\model.pkl"
}

from prefect.tasks.prefect.flow_run import (
        StartFlowRun
    )

#diabetic_flow.register(project_name="diabetic")
#diabetic_flow.run_agent()
#state = diabetic_flow.run(**params)
from prefect.client import Client
import time
client = Client()

flow_id = StartFlowRun(project_name="diabetic",
            flow_name=diabetic_flow.name, parameters=params
        ).run()

def getStatus(flow_id:str):
    finished = False
    while not finished:
        state = client.get_flow_run_info(flow_id).state
        print(state.message.strip('.'))
        time.sleep(10)
        finished = state.is_finished()
    return state


state = getStatus(flow_id)

if state.is_successful():
    print("Completed Successfully")
    #task_name = "trasnform"
    #task_ref = diabetic_flow.get_tasks(name=task_name)[0]
    #model = state.result[task_ref].result
    #_config.set_option("server.headless", True)
    #args = [params.get('output')]
    #streamlit.bootstrap.run(filename,'',args,flag_options={})
