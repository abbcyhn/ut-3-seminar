"""
    DESCRIPTION:
        MERGE JSON FILE WITH ITSELF
    
    INPUT:
        1 - JSON FILE PATH
        2 - HOW MANY TIMES TO MERGE
        3 - OUTPUT PATH

    EXAMPLE COMMAND:
        python merge.py ../datas/b.json 2 ../datas/c.json
"""

import sys
import time

def main():
    inputpath = sys.argv[1]
    times = sys.argv[2]
    outputpath = sys.argv[3]
    for _ in range(int(times)):
        # with open(inputpath, 'r') as f:
        #     for line in f:
        #         with open(outputpath, 'a') as f:
        #             f.write(line)
        start = time.monotonic()
        file = open(inputpath, 'r') 
        text = file.read() 
        file.close()
        with open(outputpath, 'a') as f:
            f.write(text)
        end = time.monotonic()
        print(f"Exection Time {_} = {end-start}")
    print("Completed!")

if __name__ == '__main__':
    main()
