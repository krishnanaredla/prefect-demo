from prefect.tasks.prefect.flow_run import (
        StartFlowRun
    )
from prefect.client import Client
import time
from typing import Dict 
import logging
import sys
from logging import Logger
from logging.handlers import TimedRotatingFileHandler

client = Client()


class PipelineLogger(Logger):
    def __init__(
        self,
        log_file=None,
        log_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        *args,
        **kwargs
    ):
        self.formatter = logging.Formatter(log_format)
        self.log_file = log_file

        Logger.__init__(self, *args, **kwargs)

        self.addHandler(self.get_console_handler())
        if log_file:
            self.addHandler(self.get_file_handler())
        self.propagate = False

    def get_console_handler(self):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(self.formatter)
        return console_handler

    def get_file_handler(self):
        file_handler = TimedRotatingFileHandler(self.log_file, when="midnight")
        file_handler.setFormatter(self.formatter)
        return file_handler

def getStatus(flow_id:str):
    finished = False
    while not finished:
        state = client.get_flow_run_info(flow_id).state
        logger.info(state.message.strip('.'))
        time.sleep(2)
        finished = state.is_finished()
    return state

def main(project_name:str,flow_name:str,params:Dict)->None:
    flow_id = StartFlowRun(project_name=project_name,
            flow_name=flow_name, parameters=params
        ).run()
    state = getStatus(flow_id)
    if state.is_successful():
        logger.info("Completed Successfully")
    elif state.is_failed():
        logger.info("Flow was unsuccessfully")
    else:
        pass

params = {
    "conditions": "C:\work\prefect\diabetic\data\conditions.csv",
    "patients": "C:\work\prefect\diabetic\data\patients.csv",
    "observations": "C:\work\prefect\diabetic\data\observations.csv",
    "output" : "C:\work\prefect\diabetic\data\output\model.pkl",
    "testData" : [125,80,65.9,85.5,31.4,25]
}

if __name__ == "__main__":
    logger = PipelineLogger(name="Pipeline")
    main(
        project_name="sample",
        flow_name="diabetic",
        params=params
    )







