from mini_orchestrator import dag, task
from mini_orchestrator.runner import run_dag


def extract():
    print("Extracting...")


def transform():
    print("Transforming...")
    raise ValueError("Transformation failed")


def load():
    print("Loading...")


@dag(name="test_dag", schedule="0 * * * *")
def test_pipeline():

    extract_task = task(
        "extract",
        run=extract,
    )

    transform_task = task(
        "transform",
        run=transform,
        depends_on=[extract_task],
    )

    task(
        "load",
        run=load,
        depends_on=[transform_task],
    )


pipeline = test_pipeline

run_dag(pipeline)