import os
import subprocess
import time
import signal

GB = 1024 * 1024 * 1024
memory_limit = 1 * GB

# print execution directory
print("Current working directory: " + os.getcwd())

benchmark_command = [
    './cmake-build-debug/hyriseBenchmarkTPCH', #args.executable
    '--scheduler',
    '--clients=48',
    '-m',
    'Shuffled',
    '-s',
    '10',
    '-t',
    '1200',
    '-w',
    '20',
    '-o',
    'benchmark_mmap_based_page_cache_1gb.json'
    ]

print("Executing command: " + subprocess.list2cmdline(benchmark_command) + "\n")

sp = subprocess.Popen(benchmark_command)

print("Waiting 5 minutes to let setup finish...")
time.sleep(5 * 60)

print(f"Creating cgroup for memory limit and setting its memory.high property to {memory_limit}.")
os.system("sudo cgcreate -g memory:memory-limit")
os.system("sudo cgset -r memory.high=" + str(memory_limit) + " memory-limit")

print("Print result of memory limit setting on memory-limit group.")
os.system("sudo cgget -r memory.high memory-limit")
os.system("sudo cgget -r memory.max memory-limit")

print("Moving benchmark process into memory-limited cgroup.")
os.system("sudo cgclassify -g memory:memory-limit " + str(sp.pid))

print("Letting benchmark run for 3 minutes to allow reduction of memory footprint.")
time.sleep(3 * 60)

print("Setting memory.max hard limit on memory-limit group.")
os.system("sudo cgset -r memory.max=" + str(memory_limit) + " memory-limit")

sp.wait()