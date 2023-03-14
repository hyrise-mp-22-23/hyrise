import json
import matplotlib.pyplot as plt

# read in the JSON data
path = '../cmake-build-release/merge_10.json'
with open(path, 'r') as f:
    data = json.load(f)

labels = ['Hyrise Master', 'Hyrise MMAP']
times = []
master_mean = data["results"][0]["mean"]
scale_factor = data["results"][0]["command"].split("-s ")[1].strip()

for result in data["results"]:
    centered_data = [time / master_mean for time in result["times"]]
    times.append(centered_data)

# create a boxplot using matplotlib
fig, ax = plt.subplots()
ax.boxplot(times, labels=labels)

ax.set_title(f'Comparison of Hyrise Master and MMAP sf={scale_factor}')
ax.set_xlabel('System')
ax.set_ylabel('Time (s)')

plt.show()