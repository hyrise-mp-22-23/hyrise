import os
import subprocess
import time
import signal

GiB = 1024 * 1024 * 1024
# memory_limit = 1 * GiB
unlimited = 200 * GiB

memory_limits = [0.5, 1.5, 2, 3, 4, 5, 6, 7, 8, 9, 10]
memory_limits = [i * GiB for i in memory_limits]

# print execution directory
print("Current working directory: " + os.getcwd())

for memory_limit in memory_limits:

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
        'benchmark_mmap_based_page_cache_' + str(memory_limit) + 'gb.json'
        ]

    os.system("sudo rm *.bin")
    print(f"Creating cgroup for memory limit and setting its memory.high property to {unlimited}.")
    os.system("sudo cgcreate -g memory:memory-limit")
    os.system("sudo cgset -r memory.high=" + str(unlimited) + " memory-limit")
    os.system("sudo cgset -r memory.max=" + str(unlimited) + " memory-limit")

    print("Print result of memory limit setting on memory-limit group.")
    os.system("sudo cgget -r memory.high memory-limit")
    os.system("sudo cgget -r memory.max memory-limit")


    print("Executing command: " + subprocess.list2cmdline(benchmark_command) + "\n")

    sp = subprocess.Popen(benchmark_command)

    print("Moving benchmark process into memory-limited cgroup.")
    os.system("sudo cgclassify -g memory:memory-limit " + str(sp.pid))

    print("Waiting 3.5 minutes to let setup finish...")
    time.sleep(3.5 * 60)

    print("Setting memory.high soft limit on memory-limit group.")
    os.system("sudo cgset -r memory.high=" + str(memory_limit) + " memory-limit")
    os.system("sudo cgget -r memory.high memory-limit")

    print("Letting benchmark run for 3 minutes to allow reduction of memory footprint.")
    for i in range(0, 6):
        print("********* Memory Metadata of benchmark process: *******************:")
        os.system("sudo cgget -r memory.current memory-limit")
        os.system("sudo cgget -r memory.pressure memory-limit")
        os.system("sudo cgget -r memory.stat memory-limit")

        time.sleep(30)

    print("Setting memory.max hard limit on memory-limit group.")
    os.system("sudo cgset -r memory.max=" + str(memory_limit) + " memory-limit")

    sp.wait()