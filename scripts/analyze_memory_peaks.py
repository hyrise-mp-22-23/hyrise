import json
import matplotlib.pyplot as plt

GB = 1000 * 1000 * 1000
#load json
with open('./scripts/benchmark_memory_peak.json') as f_hundred:
    data_hundred = json.load(f_hundred)
with open('./scripts/benchmark_memory_peak_1_10_50.json') as f_rest:
    data_rest = json.load(f_rest)

#merge json
data = {**data_hundred, **data_rest}

print(f"<TPC-H Scale Factor> : <Benchmark Size> GB")
#convert values to int
for key,value in data.items():
    #convert value list to int
    data[key]['memory_sizes'] = [int(i) for i in data[key]['memory_sizes']]
    data[key]['memory_peak'] = max(data[key]['memory_sizes'])

    print(f"{key} : {data[key]['memory_peak'] / GB}GB")

# plot data values
    plt.plot(data[key]['memory_sizes'])
    plt.title(f'{key}GB')
    plt.show()