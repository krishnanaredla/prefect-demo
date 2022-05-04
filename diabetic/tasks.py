from prefect import task
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from typing import List, Tuple, Dict
import pickle
import prefect
from diabetic.utilities import handleException, transformData


@task
def load(obPath: str, ptPath: str, cnPath: str) -> Tuple[pd.DataFrame, ...]:
    """
    Loads the required csv files

    Args:
        observations_path (str) : Observation csv file location
        patients_path     (str) : Patients csv file location
        conditions_path   (str) : Conditions csv file location

    Returns:
        Tuple: returns tuple of loaded pandas dataframes
    """
    logger = prefect.context.get("logger")
    logger.info("Loading the input files")
    observations = pd.read_csv(obPath)
    patients = pd.read_csv(ptPath)
    conditions = pd.read_csv(cnPath)
    return (observations, patients, conditions)


@task
def transform(dataframes: Tuple[pd.DataFrame, ...]) -> pd.DataFrame:
    """
    Transforms the data into required training data

    Args:
        dataframes (Tuple[pd.DataFrame, ...]) : Tuple of input dataframes

    Returns:
        pd.DataFrame : returns training data
    """
    return transformData(dataframes)


def splitData(data: pd.DataFrame):
    """
    Split data into training and test data

    Args
    ----------
    data (pd.DataFrame) : input data

    """
    df = data[["systolic", "diastolic", "hdl", "ldl", "bmi", "age", "diabetic"]]
    df.sample(frac=1)
    X = df.drop("diabetic", axis=1)
    y = df["diabetic"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    return (X_train, X_test, y_train, y_test)


@task
def RunLogisticRegression(data: pd.DataFrame) -> Dict:
    """
    Run Logistic Regression
    """
    X_train, X_test, y_train, y_test = splitData(data)
    log_reg = LogisticRegression(random_state=0)
    log_reg.fit(X_train, y_train)
    score = log_reg.score(X_test, y_test)
    logger = prefect.context.get("logger")
    logger.info({"model": "LogisticRegression", "score": score})
    return {"model": "LogisticRegression", "score": score, "trained": log_reg}


@task
def RunKNeighborsClassifier(data: pd.DataFrame) -> Dict:
    """
    Run KNeighborsClassifier
    """
    X_train, X_test, y_train, y_test = splitData(data)
    knn = KNeighborsClassifier()
    knn.fit(X_train, y_train)
    score = knn.score(X_test, y_test)
    logger = prefect.context.get("logger")
    logger.info({"model": "KNeighborsClassifier", "score": score})
    return {"model": "KNeighborsClassifier", "score": score, "trained": knn}


@task
def RunRandomForestClassifier(data: pd.DataFrame) -> Dict:
    """
    RUn RandomForestClassifier
    """
    clf = RandomForestClassifier()
    X_train, X_test, y_train, y_test = splitData(data)
    clf.fit(X_train, y_train)
    score = clf.score(X_test, y_test)
    logger = prefect.context.get("logger")
    logger.info({"model": "RandomForestClassifier", "score": score})
    return {"model": "RandomForestClassifier", "score": score, "trained": clf}


@task
def RunSVC(data: pd.DataFrame) -> Dict:
    """
    Run SVC
    """
    X_train, X_test, y_train, y_test = splitData(data)
    svm = SVC(probability=True)
    svm.fit(X_train, y_train)
    score = svm.score(X_test, y_test)
    logger = prefect.context.get("logger")
    logger.info({"model": "SVC", "score": score})
    return {"model": "SVC", "score": score, "trained": svm}


@task
def identifyandSaveModel(data: List[Dict], output: str) -> None:
    """
    Identify the model with highest score

    Args:
        data (List[Dict]) :List of scores of all run models
        output (str) :location to store the pickle file
    """
    logger = prefect.context.get("logger")
    modelFilter = lambda data: max(data, key=lambda x: x["score"])
    model = modelFilter(data)
    logger.info("Model with highest score {}".format(model))
    pickle.dump(model["trained"], open(output, "wb"))


@task(state_handlers=[handleException])
def testModel(data: List[float], output):
    """
    test the model

    Args:
    data (List[float]) : sample test data
    output (str) : location of stored pickle file

    Raises:
        AssertionError: if test failed on sample data
    """
    logger = prefect.context.get("logger")
    df = pd.DataFrame([pd.Series(data)])
    model = pickle.load(open(output, "rb"))
    prediction = float(model.predict_proba(df)[0][1])
    logger.info("Prediction : {}".format(prediction))
    assert prediction > 0.5
    logger.info("Model passed for sample data")
