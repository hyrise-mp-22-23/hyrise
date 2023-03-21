#!/usr/bin/env python3
import pandas as pd
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import os
import json

# set plot styles
plt.style.use("ggplot")
sns.set(font_scale=1.5)

limit_sizes = [6, 7, 8, 10, 12, 16, 18, 20]

#initalize pandas data frame with columns
data_df = pd.DataFrame(columns=["memory", "type", "latency"])
for filename in os.listdir("./scripts/benchmark_mmap_based_single_threaded"):
    if filename.endswith(".json"):
        with open(f"./scripts/benchmark_mmap_based_single_threaded/{filename}") as f:
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
                memory = int(filename.split("_")[5][:-2])
                #calculate latency

                #append data to data frame
                data_df = data_df.append({"memory": int(memory), "type": type, "latency": int(latency_all)}, ignore_index=True)

print(data_df)
benchmark_results = sns.lineplot(data=data_df, x="memory", y="latency", hue="type", marker='o', linestyle='--')

benchmark_results.set(
    xlabel="Available RAM in GB", ylabel="Latency in ms/iter (Sum over all Queries)", title=f"MMAP-based Single-Threaded Hyrise\nSum of Average Latency over all Queries Depending on Memory Limitation."
)

#benchmark_results.set(xticks=data_df.memory.values)
# for item, color in zip(data_df.groupby('type'), sns.color_palette()):
#     #item[1] is a grouped data frame
#     if (item[1][['type']].values[0] == 'mmap_hyrise_warmup'):
#         for x,y in item[1][['scale_factor','latency']].values:
#             benchmark_results.text(x,y,f'{x:.2f}',color='black',horizontalalignment='center',verticalalignment='bottom')

plt.legend(title="Type")
plt.show()

