import pandas as pd
import time

CSV_PATH = 'US_Accidents_March23.csv'
PARQUET_PATH = 'US_Accidents_March23.parquet'

print("Starting CSV read...")
start_time = time.time()
# Read the 3GB CSV
df = pd.read_csv(CSV_PATH, low_memory=False)
print(f"CSV Read Time: {time.time() - start_time:.2f} seconds")

# Convert and save as Parquet (highly compressed, column-oriented)
start_time = time.time()
df.to_parquet(PARQUET_PATH, engine='pyarrow', compression='snappy')
print(f"Parquet Write Time: {time.time() - start_time:.2f} seconds")
print(f"Parquet file saved to {PARQUET_PATH}")