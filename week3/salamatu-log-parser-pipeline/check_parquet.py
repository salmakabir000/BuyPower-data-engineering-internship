import pandas as pd

df = pd.read_parquet(
    "output/clean.parquet"
)

print(df.head())

print("\n")

print(df.dtypes)
