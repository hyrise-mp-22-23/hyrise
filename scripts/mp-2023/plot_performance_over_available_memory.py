#!/usr/bin/env python3

"""
This script plots the performance of different hyrise implementations (as deduced from benchmark result file names)
over available memory.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import json
import re

#configure directory with benchmark results

benchmark_results_dir = ""
pgfault_stats = ""
limit_sizes = [6, 7, 8, 10, 12, 16, 18, 20]

# set plot styles
plt.style.use("ggplot")
sns.set(font_scale=1.5)

def parse_occurred_pagefaults(memory_stat_string):
    regex = re.compile('pgfault ([0-9]*)')
    return int(regex.findall(memory_stat_string)[0])

#load pgfault stats
with open(pgfault_stats) as f:
    pgfault_data = json.load(f)

#initalize pandas data frame with columns
data_df = pd.DataFrame(columns=["memory", "type", "latency", "pgfaults"])
for filename in os.listdir(benchmark_results_dir):
    if filename.endswith(".json"):
        with open(os.path.join(benchmark_results_dir, filename)) as f:
            data = json.load(f)

            latency_all = 0
            for benchmark in data['benchmarks']:
                query_latency = 0
                for run in benchmark['successful_runs']:
                    query_latency += run['duration']
                query_latency /= len(benchmark['successful_runs'])
                latency_all += query_latency
            #convert latency_all from nanoseconds to milliseconds
            latency_all /= 1000000

            if 'master' in filename:
                type = 'master'
                for limit_size in limit_sizes:
                    data_df = data_df.append({"memory": int(limit_size), "type": type, "latency": latency_all}, ignore_index=True)
            else:
                type = 'mmap-based'
                memory = float(filename.split("_")[5][:-7])
                #assuming only int memory limit sizes
                memory_key = str(int(memory))
                execution_pgfaults = 0
                if memory_key in pgfault_data:
                    pgfaults_stats = pgfault_data[memory_key]
                    before_memory_stats = pgfaults_stats['before']['memory_stat']
                    after_memory_stats = pgfaults_stats['after']['memory_stat']
                    execution_pgfaults = parse_occurred_pagefaults(after_memory_stats) - parse_occurred_pagefaults(before_memory_stats)
                #append data to data frame
                data_df = data_df.append({"memory": memory, "type": type, "latency": int(latency_all), "pgfaults": execution_pgfaults}, ignore_index=True)


benchmark_results = sns.lineplot(data=data_df, x="memory", y="latency", hue="type", marker='o', linestyle='--')
benchmark_results_second = sns.lineplot(data=data_df, x="memory", y="pgfaults", color='red', marker='o', linestyle='dotted', ax=benchmark_results.twinx())

benchmark_results.set(
    xlabel="Available RAM in GB", ylabel="Latency in ms/iter (Sum over all Queries)", title=f"MMAP-based Single-Threaded Hyrise\nSum of Average Latency over all Queries Depending on Memory Limitation."
)

benchmark_results_second.set(ylabel="#Pagefaults during Execution")

#benchmark_results.set(xticks=data_df.memory.values)
# for item, color in zip(data_df.groupby('type'), sns.color_palette()):
#     #item[1] is a grouped data frame
#     if (item[1][['type']].values[0] == 'mmap_hyrise_warmup'):
#         for x,y in item[1][['scale_factor','latency']].values:
#             benchmark_results.text(x,y,f'{x:.2f}',color='black',horizontalalignment='center',verticalalignment='bottom')

plt.legend(title="Type")
plt.show()

