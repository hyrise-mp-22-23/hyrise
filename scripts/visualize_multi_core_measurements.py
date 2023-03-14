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

#initalize pandas data frame with columns
data_df = pd.DataFrame(columns=["scale_factor", "warmup", "latency"])
#iterate over files in mmap_multi_core_benchmark_results which end with .json
for filename in os.listdir("./scripts/mmap_multi_core_results_corrected"):
    if filename.endswith(".json"):
        with open(f"./scripts/mmap_multi_core_results_corrected/{filename}") as f:
            #load json
            data = json.load(f)

            type = filename.split("_")[1] + "_" + filename.split("_")[2]
            if filename.split("_")[-2].startswith('warmup') or filename.split("_")[-1].startswith('warmup'):
                type += "_warmup"
            else:
                type += "_no_warmup"
            num_cores = filename.split("_")[3]
            if num_cores == "1" or num_cores == "64":
                continue
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
    xlabel="#Cores", ylabel="Latency in ms/iter (Sum over all queries)", title=f"Comparison of MMAP-based Hyrise vs. Hyrise Master in \nsum of average latency over all queries depending on number of cores."
)

benchmark_results.set(xticks=data_df.num_cores.values)
# for item, color in zip(data_df.groupby('type'), sns.color_palette()):
#     #item[1] is a grouped data frame
#     if (item[1][['type']].values[0] == 'mmap_hyrise_warmup'):
#         for x,y in item[1][['scale_factor','latency']].values:
#             benchmark_results.text(x,y,f'{x:.2f}',color='black',horizontalalignment='center',verticalalignment='bottom')

plt.legend(title="Warmup")
plt.show()

