import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq

# df = pv.read_csv("green_tripdata_2019-07.csv.gz", convert_options=pv.ConvertOptions(column_types = {'ehail_fee': 'float64'}))

# pr = pq.write_table(df, 'green_tripdata_2019-07.parquet')
src_file = "fhv_tripdata_2019-01.csv.gz"

table = pv.read_csv(src_file, convert_options=pv.ConvertOptions(column_types = {'DOLocationID': 'float64', 'PUlocationID': 'float64' }))
pq.write_table(table, src_file.replace('.csv.gz', '.parquet'))
