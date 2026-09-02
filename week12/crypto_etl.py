from mini_orchestrator import dag, task


def extract_crypto():
    print("Extracting crypto data...")


def transform_crypto():
    print("Transforming crypto data...")


def load_to_warehouse():
    print("Loading crypto data...")


@dag(name="crypto_etl", schedule="0 * * * *")
def crypto_etl():

    extract = task(
        "extract",
        run=extract_crypto,
    )

    transform = task(
        "transform",
        run=transform_crypto,
        depends_on=[extract],
    )

    task(
        "load",
        run=load_to_warehouse,
        depends_on=[transform],
    )