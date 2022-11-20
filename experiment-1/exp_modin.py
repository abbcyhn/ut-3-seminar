import time
import ray
import modin.pandas as pd


if __name__ == '__main__':  
    ray.init()

    inputpath = '../datas/2022_place_deephaven_sample.parquet'
    outputpath = 'exp_modin.txt'
    file = open(outputpath, 'w') 

    # 1) Reading exec time
    start_time = time.time()
    df = pd.read_parquet(inputpath)
    line = f"1. Modin - Reading exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 2) Mean exec time
    start_time = time.time()
    _ = df['x1'].mean()
    line = f"2. Modin - Mean exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 3) Std exec time
    start_time = time.time()
    _ = df['x1'].std()
    line = f"3. Modin - Std exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 4) Unique exec time
    start_time = time.time()
    _ = df['user_id'].unique()
    line = f"4. Modin - Unique exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 5) Cumsum exec time
    start_time = time.time()
    _ = df['y1'].cumsum()
    line = f"5. Modin - Cumsum exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 6) Groupby Aggregation exec time
    start_time = time.time()
    _ = df.groupby('user_id')['x1'].mean()
    line = f"6. Modin - Groupby Aggregation exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")
