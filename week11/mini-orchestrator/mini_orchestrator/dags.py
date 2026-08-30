from mini_orchestrator.dag import dag, task
from mini_orchestrator.crypto_source import (
    extract_data,
    transform_data,
    load_data
)


raw_data = []
transformed_data = []


def extract():
    global raw_data

    print("Extracting crypto data")

    raw_data = extract_data(10)

    print(f"Extracted {len(raw_data)} rows")


def transform():
    global transformed_data

    print("Transforming crypto data")

    transformed_data = transform_data(raw_data, None)

    print(f"Transformed {len(transformed_data)} rows")


def load():
    print("Loading crypto data")

    load_data(transformed_data)


def quality_check():
    print("Running data quality checks")

    if len(transformed_data) == 0:
        raise Exception("Quality check failed: no transformed data")

    print("Quality check passed")


@dag(name="crypto_etl", schedule="0 * * * *")
def crypto_etl():
    extract_task = task("extract", extract)

    transform_task = task(
        "transform",
        transform,
        depends_on=[extract_task]
    )

    load_task = task(
        "load",
        load,
        depends_on=[transform_task]
    )

    task(
        "quality_check",
        quality_check,
        depends_on=[load_task]
    )

