import time
import pandas as pd

inputpath = '../datas/2022_place_deephaven.parquet'
outputpath = 'exp_pandas.txt'

file = open(outputpath, 'w') 

# 1) Reading exec time
start_time = time.time()
df = pd.read_parquet(inputpath)
line = f"1. Pandas - Reading exec time: {time.time() - start_time}"
print(line); file.write(line)

# 2) Mean exec time
start_time = time.time()
_ = df['x1'].mean()
lien = f"2. Pandas - Mean exec time: {time.time() - start_time}"
print(line); file.write(line) 

# 3) Std exec time
start_time = time.time()
_ = df['x1'].std()
line = f"3. Pandas - Std exec time: {time.time() - start_time}"
print(line); file.write(line) 

# 4) Unique exec time
start_time = time.time()
_ = df['user_id'].unique()
line = f"4. Pandas - Unique exec time: {time.time() - start_time}"
print(line); file.write(line) 

# 5) Cumsum exec time
start_time = time.time()
_ = df['y1'].cumsum()
line = f"5. Pandas - Cumsum exec time: {time.time() - start_time}"
print(line); file.write(line) 

# 6) Groupby Aggregation exec time
start_time = time.time()
_ = df.groupby('user_id')['x1'].mean()
line = f"6. Pandas - Groupby Aggregation exec time: {time.time() - start_time}"
print(line); file.write(line) 