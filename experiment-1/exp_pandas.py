import time
import pandas as pd

if __name__ == '__main__':  
    inputpath = '../datas/2022_place_deephaven_sample.parquet'
    outputpath = 'exp_pandas.txt'
    file = open(outputpath, 'w') 

    # 1) Reading exec time
    start_time = time.time()
    df = pd.read_parquet(inputpath)
    line = f"1. Pandas - Reading exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 2) Mean exec time
    start_time = time.time()
    _ = df['salary'].mean()
    line = f"2. Pandas - Mean exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n") 

    # 3) Std exec time
    start_time = time.time()
    _ = df['salary'].std()
    line = f"3. Pandas - Std exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n") 

    # 4) Unique exec time
    start_time = time.time()
    _ = df['id'].unique()
    line = f"4. Pandas - Unique exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n") 

    # 5) Cumsum exec time
    start_time = time.time()
    _ = df['salary'].cumsum()
    line = f"5. Pandas - Cumsum exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n") 

    # 6) Groupby Aggregation exec time
    start_time = time.time()
    _ = df.groupby('id')['salary'].mean()
    line = f"6. Pandas - Groupby Aggregation exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n") 