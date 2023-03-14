import os
import subprocess
import time

GB = 1000 * 1000 * 1000
unlimited = 200 * GB

memory_limits = [20, 16, 14, 12, 10, 8, 7, 6]

for memory_limit in memory_limits:

    benchmark_command = [
        'numactl',
        '-m',
        '0',
        '-N',
        '0',
        './cmake-build-release/hyriseBenchmarkTPCH',
        '-m',
        'Shuffled',
        '--scheduler',
        '-s',
        '10',
        '-t',
        '600',
        '-w',
        '5',
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
    os.system("sudo cgset -r memory.high=" + str(memory_limit * GB) + " memory-limit")
    os.system("sudo cgget -r memory.high memory-limit")

    print("Letting benchmark run for 3 minutes to allow reduction of memory footprint.")
    time.sleep(3 * 60)

    sp.wait()