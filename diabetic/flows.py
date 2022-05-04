from diabetic.tasks import *
from prefect import Parameter, Flow
from prefect.storage import Local
from prefect.utilities.logging import get_logger
import prefect


def getFlow():
    """
    Diabetic flow

    Flow is as follows :

        -> Extras the data the from locally stored CSV files

        -> Transforms the data

        -> Run the data against different models

            -> LogisticRegression

            -> KNeighborsClassifier

            -> RandomForestClassifier

            -> SVC

        -> Identify the model with highest score

        -> Test model with sample data

    Args:
        observations_path (str) : Observation csv file location
        patients_path     (str) : Patients csv file location
        conditions_path   (str) : Conditions csv file location
        output            (str) : location to store the trained model
        testData          (list): sample data to test model

    Raises:
        AssertionError : In testModel task if test failed on sample data
    """
    with Flow(name="diabetic", storage=Local()) as diabetic_flow:
        observations_path = Parameter("observations", default="./data/observations.csv")
        patients_path = Parameter("patients", default="./data/patients.csv")
        conditions_path = Parameter("conditions", default="./data/conditions.csv")
        output = Parameter("output")
        testData = Parameter("testData")
        data = load(observations_path, patients_path, conditions_path)
        processed = transform(data)
        lr = RunLogisticRegression(processed)
        knn = RunKNeighborsClassifier(processed)
        rf = RunRandomForestClassifier(processed)
        sv = RunSVC(processed)
        model = identifyandSaveModel(
            [lr, knn, rf, sv], output, upstream_tasks=[lr, knn, rf, sv]
        )
        testModel(testData, output, upstream_tasks=[model])
    return diabetic_flow
