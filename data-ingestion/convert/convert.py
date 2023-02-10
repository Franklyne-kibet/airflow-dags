import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq

df = pv.read_csv("green_tripdata_2019-07.csv.gz", convert_options=pv.ConvertOptions(column_types = {'ehail_fee': 'float64'}))

pr = pq.write_table(df, 'green_tripdata_2019-07.parquet')
