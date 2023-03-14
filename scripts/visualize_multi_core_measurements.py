#!/usr/bin/env python3
import pandas as pd
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import os
import json

# set plot styles
plt.style.use("ggplot")

#initalize pandas data frame with columns
data_df = pd.DataFrame(columns=["scale_factor", "warmup", "latency"])
#iterate over files in mmap_multi_core_benchmark_results which end with .json
for filename in os.listdir("./scripts/mmap_multi_core_results_corrected"):
    if filename.endswith(".json"):
        with open(f"./scripts/mmap_multi_core_results_corrected/{filename}") as f:
            #load json
            data = json.load(f)
            type = filename.split("_")[1] + "_" + filename.split("_")[2]
            print(filename.split("_"))
            print([filename.split("_")[-2], filename.split("_")[-1]])
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
            data_df = data_df.append({"scale_factor": int(num_cores), "type": type, "latency": latency_all}, ignore_index=True)

benchmark_results = sns.lineplot(data=data_df, x="scale_factor", y="latency", hue="type", marker='o')

benchmark_results.set(
    xlabel="scale_factor", ylabel="Latency Sum in ms/iter", title=f"Latency sum for MMAP-Hyrise depending on scale_factor and warmup"
)

plt.legend(title="Warmup")
plt.show()

