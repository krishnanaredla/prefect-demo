import traceback
import prefect
from typing import cast, Tuple
import pandas as pd
from functools import reduce
import numpy as np


def exception_to_string(excp):
    stack = traceback.extract_stack()[:-3] + traceback.extract_tb(excp.__traceback__)
    pretty = traceback.format_list(stack)
    return "".join(pretty) + "\n  {} {}".format(excp.__class__, excp)


def handleException(task, old_state, new_state):
    logger = prefect.context.get("logger")
    if new_state.is_failed():
        if isinstance(new_state.result, Exception):
            value = "```{}```".format(repr(new_state.result))
        else:
            value = cast(str, new_state.message)
        msg = (
            f"The task `{prefect.context.task_name}` failed "
            f"in a flow run {prefect.context.flow_run_id} "
            f"with an exception {value} \n"
            f"and a full exception traceback: {exception_to_string(new_state.result)}"
        )
        logger.info(msg)
        logger.info("Model failed for sample data")
    return new_state


def transformData(dataframes: Tuple[pd.DataFrame, ...]) -> pd.DataFrame:
    ob, pt, con = dataframes
    systolic_observations_df = ob[ob["CODE"] == "785-6"][
        ["PATIENT", "DATE", "VALUE"]
    ].rename(
        columns={
            "VALUE": "systolic",
            "PATIENT": "patientid",
            "DATE": "dateofobservation",
        }
    )
    diastolic_observations_df = ob[ob["CODE"] == "786-4"][
        ["PATIENT", "DATE", "VALUE"]
    ].rename(
        columns={
            "VALUE": "diastolic",
            "PATIENT": "patientid",
            "DATE": "dateofobservation",
        }
    )
    hdl_observations_df = ob[ob["CODE"] == "787-2"][
        ["PATIENT", "DATE", "VALUE"]
    ].rename(
        columns={"VALUE": "hdl", "PATIENT": "patientid", "DATE": "dateofobservation"}
    )
    ldl_observations_df = ob[ob["CODE"] == "789-8"][
        ["PATIENT", "DATE", "VALUE"]
    ].rename(
        columns={"VALUE": "ldl", "PATIENT": "patientid", "DATE": "dateofobservation"}
    )
    bmi_observations_df = ob[ob["CODE"] == "777-3"][
        ["PATIENT", "DATE", "VALUE"]
    ].rename(
        columns={"VALUE": "bmi", "PATIENT": "patientid", "DATE": "dateofobservation"}
    )
    data_list = [
        systolic_observations_df,
        diastolic_observations_df,
        hdl_observations_df,
        ldl_observations_df,
        bmi_observations_df,
    ]
    merged_observations_df = reduce(
        lambda left, right: pd.merge(
            left,
            right,
            on=["patientid", "dateofobservation"],
        ),
        data_list,
    )
    patients_df = pt[["Id", "BIRTHDATE"]].rename(
        columns={"Id": "patientid", "BIRTHDATE": "dateofbirth"}
    )
    patients_merged_df = pd.merge(merged_observations_df, patients_df, on=["patientid"])
    patients_merged_df["age"] = (
        pd.to_datetime(patients_merged_df["dateofobservation"]).dt.tz_localize(None)
        - pd.to_datetime(patients_merged_df["dateofbirth"])
    ).dt.days / 365.25
    merged_observations_with_age_df = patients_merged_df.drop(["dateofbirth"], axis=1)
    diabetics_df = con[con["DESCRIPTION"] == "Diabetes"].rename(
        columns={"PATIENT": "patientid", "START": "start"}
    )
    data = pd.merge(
        merged_observations_with_age_df, diabetics_df, on="patientid", how="left"
    )
    data["diabetic"] = np.where(data["start"].notnull(), 1, 0)
    data = data[(data["diabetic"] == 0) | (data["dateofobservation"] >= data["start"])]
    data["RN"] = (
        data.sort_values(["dateofobservation"], ascending=[True])
        .groupby(["patientid"])
        .cumcount()
        + 1
    )
    data = data[data["RN"] == 1].drop("RN", axis=1)
    return data
