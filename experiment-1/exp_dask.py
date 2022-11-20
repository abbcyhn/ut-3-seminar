import time
import dask
import dask.dataframe as dd

if __name__ == '__main__':  
    inputpath = '../datas/2022_place_deephaven_sample.parquet'
    outputpath = 'exp_dask.txt'
    file = open(outputpath, 'w') 

    # 1) Reading exec time
    start_time = time.time()
    df = dd.read_parquet(inputpath)
    line = f"1. Dask - Reading exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 2) Mean exec time
    start_time = time.time()
    _ = df['x1'].mean().compute(scheduler ="processes")
    line = f"2. Dask - Mean exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 3) Std exec time
    start_time = time.time()
    _ = df['x1'].std().compute(scheduler ="processes")
    line = f"3. Dask - Std exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 4) Unique exec time
    start_time = time.time()
    _ = df['user_id'].unique().compute(scheduler ="processes")
    line = f"4. Dask - Unique exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 5) Cumsum exec time
    start_time = time.time()
    _ = df['y1'].cumsum().compute(scheduler ="processes")
    line = f"5. Dask - Cumsum exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 6) Groupby Aggregation exec time
    start_time = time.time()
    _ = df.groupby('user_id')['x1'].mean().compute(scheduler ="processes")
    line = f"6. Dask - Groupby Aggregation exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")