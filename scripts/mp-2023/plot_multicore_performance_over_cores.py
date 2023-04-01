#!/usr/bin/env python3

"""
This script plots the performance of different hyrise multithreaded runs (as deduced from benchmark result file names)
over the number of cores available.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import json

#configure directory with benchmark results
benchmark_results_dir = "./scripts/benchmark_hyrise_master_variable_core_23_01_04"

# set plot styles
plt.style.use("ggplot")
sns.set(font_scale=1.5)

#initalize pandas data frame with columns
data_df = pd.DataFrame(columns=["scale_factor", "warmup", "latency"])
#iterate over files in mmap_multi_core_benchmark_results which end with .json
for filename in os.listdir(benchmark_results_dir):
    if filename.endswith(".json"):
        with open(os.path.join(benchmark_results_dir, filename)) as f:
            #load json
            data = json.load(f)

            type = filename.split("_")[1] + "_" + filename.split("_")[2]
            if filename.split("_")[-2].startswith('warmup') or filename.split("_")[-1].startswith('warmup'):
                type += "_warmup"
            else:
                type += "_no_warmup"
            num_cores = filename.split("_")[3]
            #calculate latency
            latency_all = 0
            for benchmark in data['benchmarks']:
                query_latency = 0
                for run in benchmark['successful_runs']:
                    query_latency += run['duration']
                query_latency /= len(benchmark['successful_runs'])
                latency_all += query_latency
            #convert latency_all from nanoseconds to milliseconds
            latency_all /= 1000000
            #append data to data frame
            data_df = data_df.append({"num_cores": int(num_cores), "type": type, "latency": latency_all}, ignore_index=True)

benchmark_results = sns.lineplot(data=data_df, x="num_cores", y="latency", hue="type", marker='o', linestyle='--')

benchmark_results.set(
    xlabel="Available #Cores", ylabel="Latency in ms/iter (Sum over all Queries)", title=f"Comparison of MMAP-based Hyrise vs. Hyrise Master in \nSum of Average Latency over All Queries Depending on Available Number of Cores."
)

benchmark_results.set(xticks=data_df.num_cores.values)
# for item, color in zip(data_df.groupby('type'), sns.color_palette()):
#     #item[1] is a grouped data frame
#     if (item[1][['type']].values[0] == 'mmap_hyrise_warmup'):
#         for x,y in item[1][['scale_factor','latency']].values:
#             benchmark_results.text(x,y,f'{x:.2f}',color='black',horizontalalignment='center',verticalalignment='bottom')

plt.legend(title="Warmup")
plt.show()

