from mini_orchestrator import dag, task

def extract_crypto():
    print("      extracting crypto prices...")

def transform_crypto():
    print("      transforming...")

def load_to_warehouse():
    print("      loading to warehouse...")

def run_dq_checks():
    print("      running data quality checks...")

@dag(name="crypto_etl", schedule="0 * * * *")
def crypto_etl():
    extract = task("extract", run=extract_crypto)
    transform = task("transform", run=transform_crypto, depends_on=[extract])
    load = task("load", run=load_to_warehouse, depends_on=[transform])
    quality_check = task("quality_check", run=run_dq_checks, depends_on=[load])
