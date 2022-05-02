from prefect import task
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import  GridSearchCV
from typing import List, Tuple,Dict
from functools import reduce
import pickle

@task
def load(obPath: str, ptPath: str, cnPath: str) -> Tuple[pd.DataFrame, ...]:
    """
    Loads the required csv files
    """
    observations = pd.read_csv(obPath)
    patients = pd.read_csv(ptPath)
    conditions = pd.read_csv(cnPath)
    return (observations, patients, conditions)


@task
def transform(dataframes: Tuple[pd.DataFrame, ...]) -> pd.DataFrame:
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

@task
def model(data:pd.DataFrame,output:str,param_grid:Dict)->None:
    df = data[["systolic", "diastolic", "hdl", "ldl", "bmi", "age", "diabetic"]]
    df.sample(frac=1)
    X = df.drop("diabetic", axis=1)
    y = df["diabetic"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    rfc = RandomForestClassifier(random_state=42)
    gs_clf = GridSearchCV(rfc, param_grid=param_grid, cv=5, n_jobs=3, verbose=True)
    gs_clf.fit(X_train, y_train)
    pickle.dump(gs_clf, open(output, "wb"))
    return None


    
#@task
#def deploy(model):
#    if st._is_running_with_streamlit:
#        print("Running from python")
#        streamlitApp(model)
#    else:
#        sys.argv = ["streamlit", "run", sys.argv[0]]
#        sys.exit(stcli.main())