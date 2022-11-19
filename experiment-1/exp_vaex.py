import time
import vaex

if __name__ == '__main__':  
    inputpath = '../datas/2022_place_deephaven.parquet'
    outputpath = 'exp_vaex.txt'
    file = open(outputpath, 'w') 

    # 1) Reading exec time
    start_time = time.time()
    df = vaex.open(inputpath)
    line = f"1. Vaex - Reading exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 2) Mean exec time
    start_time = time.time()
    _ = df['x1'].mean()
    line = f"2. Vaex - Mean exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 3) Std exec time
    start_time = time.time()
    _ = df['x1'].std()
    line = f"3. Vaex - Std exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 4) Unique exec time
    start_time = time.time()
    _ = df['user_id'].unique()
    line = f"4. Vaex - Unique exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 5) Cumsum exec time
    start_time = time.time()
    _ = df['y1'].cumsum()
    line = f"5. Vaex - Cumsum exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")

    # 6) Groupby Aggregation exec time
    start_time = time.time()
    _ = df.groupby('user_id')['x1'].mean()
    line = f"6. Vaex - Groupby Aggregation exec time: {time.time() - start_time}"
    print(line); file.write(f"{line}\n")