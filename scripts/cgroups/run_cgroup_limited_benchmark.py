import os
import subprocess
import time
import signal

GiB = 1024 * 1024 * 1024
memory_limit = 1 * GiB
unlimited = 200 * GiB

# print execution directory
print("Current working directory: " + os.getcwd())

benchmark_command = [
    './cmake-build-release/hyriseBenchmarkTPCH', #args.executable
    #'--scheduler',
    #'--clients=48',
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
os.system("sudo cgset -r memory.max=" + str(unlimited) + " memory-limit")

print("Print result of memory limit setting on memory-limit group.")
os.system("sudo cgget -r memory.high memory-limit")
os.system("sudo cgget -r memory.max memory-limit")

print("Moving benchmark process into memory-limited cgroup.")
os.system("sudo cgclassify -g memory:memory-limit " + str(sp.pid))

print("Letting benchmark run for 3 minutes to allow reduction of memory footprint.")
for i in range(0, 6):
    print("********* Memory Metadata of benchmark process: *******************:")
    os.system("sudo cgget -r memory.current memory-limit")
    os.system("sudo cgget -r memory.pressure memory-limit")
    os.system("sudo cgget -r memory.stat.anon memory-limit")
    os.system("sudo cgget -r memory.stat.file memory-limit")
    os.system("sudo cgget -r memory.stat.shmem memory-limit")
    os.system("sudo cgget -r memory.stat.file_mapped memory-limit")
    os.system("sudo cgget -r memory.stat.file_dirty memory-limit")
    os.system("sudo cgget -r memory.stat.file_writeback memory-limit")
    os.system("sudo cgget -r memory.stat.slab_unreclaimable memory-limit")
    os.system("sudo cgget -r memory.stat.slab_reclaimable memory-limit")
    os.system("sudo cgget -r memory.stat.pgfault memory-limit")
    os.system("sudo cgget -r memory.stat.pgmajfault memory-limit")
    os.system("sudo cgget -r memory.stat.pgsteal memory-limit")
    os.system("sudo cgget -r memory.stat.pglazyfree memory-limit")

    time.sleep(30)

print("Setting memory.max hard limit on memory-limit group.")
os.system("sudo cgset -r memory.max=" + str(memory_limit) + " memory-limit")

sp.wait()