import os
import subprocess
import time

GB = 1000 * 1000 * 1000
unlimited = 200 * GB

memory_limits = [20, 19, 18, 17, 16, 15, 14, 13.5, 13, 12.5, 12.25, 12, 11.75, 11.5, 11, 10, 9, 8, 7, 6.5, 6, 5.5, 5, 4.5, 4]

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

    timeout_s = 60 * 45  #max 45 minutes for TPC-H 10

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

    try:
        sp.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        print(f"Benchmark {benchmark_command} timed out after {timeout_s} seconds. Killing it.")
        sp.kill()
