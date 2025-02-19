#!/usr/bin/env python3

"""
This script analyzes and plots the memory peak results create by
'measure_memory_peak_during_benchmark_execution.py`.
"""

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
import json
import pandas as pd
import numpy as np

# set plot styles
plt.style.use("ggplot")
sns.set(font_scale=1.5)

GB = 1000 * 1000 * 1000
# load json
with open("") as f_hundred:
    data_hundred = json.load(f_hundred)
with open("") as f_rest:
    data_rest = json.load(f_rest)

# merge json
data = {**data_hundred, **data_rest}

print(f"<TPC-H Scale Factor> : <Benchmark Size> GB")
# convert values to int
for key, value in data.items():
    # convert value list to int
    data[key]["memory_sizes"] = [int(i) for i in data[key]["memory_sizes"]]
    data[key]["memory_peak"] = max(data[key]["memory_sizes"])

    print(f"{key} : {data[key]['memory_peak'] / GB}GB")

    # plot data values
    plt.plot([value / GB for value in data[key]["memory_sizes"]])
    plt.title(f"Memory Footprint over Time for TPC-H Benchmark SF{key} on Hyrise Master")
    plt.xlabel("Time matched to queries being executed for 80 seconds each")
    # add ticks to xscale
    plt.xticks(np.arange(0, 1760, step=80), [f"Q{i}" for i in range(1, 23)], rotation=90)
    plt.ylabel("Memory in GB")
    plt.show()

# plot memory peak against scale factor
# create pandas data frame
integer_keys = sorted([int(key) for key in data.keys()])
memory_peaks_percents = [((data[str(key)]["memory_peak"] / GB) / key) * 100 for key in integer_keys]
df = pd.DataFrame(columns=["scale_factor", "memory_peak"])
# add list as column to data frame
df.scale_factor = [str(key) for key in integer_keys]
df.memory_peak = memory_peaks_percents

memory_peaks = sns.barplot(data=df, x="scale_factor", y="memory_peak")

memory_peaks.set(
    xlabel="Scale Factor",
    ylabel="Percent of Benchmark Data Size",
    title=f"Memory Peaks for Query Execution of TPC-H Benchmark\non Hyrise Master depending on Scale Factor.",
)

memory_peaks.yaxis.set_major_formatter(mtick.PercentFormatter())

plt.show()
