import pandas as pd   
import pyarrow as pa  
import pyarrow.parquet as pq


df = pd.read_parquet("yellow_tripdata_2024-01.parquet")

print(df.head())
print(df.shape)

df.to_csv(
    "yellow_tripdata_2024-01.csv",
    index=False
)

#ls -lh yellow_tripdata_2024-01.*  on commandline to see size difference 