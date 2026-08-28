from mini_orchestrator import dag, task


def extract_crypto():
    print("      extracting...")


def transform_crypto():
    print("      transforming...")


def load_to_warehouse():
    print("      loading...")


def run_dq_checks():
    print("      running checks...")


@dag(name="example_success", schedule="0 * * * *")
def example_success():
    extract = task("extract", run=extract_crypto)
    transform = task("transform", run=transform_crypto, depends_on=[extract])
    load = task("load", run=load_to_warehouse, depends_on=[transform])
    quality_check = task("quality_check", run=run_dq_checks, depends_on=[load])
