from mini_orchestrator import dag, task

def extract_crypto():
    print("      extracting...")

def transform_that_fails():
    raise ValueError("bad API response")

def load_to_warehouse():
    print("      loading...")

@dag(name="broken_pipeline", schedule=None)
def broken_pipeline():
    extract = task("extract", run=extract_crypto)
    transform = task("transform", run=transform_that_fails, depends_on=[extract])
    load = task("load", run=load_to_warehouse, depends_on=[transform])
