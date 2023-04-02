#!/usr/bin/env python3

"""
This script contains the setup for limited memory benchmarks for TPCH-100.
This script benchmarks a hyrise implementation for the TPCH benchmark with setting varying memory limits for the execution
of the benchmark (not the setup!). The memory limits are set via cgroups and the process is immediately moved into the
cgroup after creation of the process. This is important as cgroups only measure memory that was allocated by a process
while being in the cgroup.
"""

import os
import subprocess
import time

GB = 1000 * 1000 * 1000
unlimited = 500 * GB

timeout_s = 60 * 45  # max 45 minutes for TPC-H 100

memory_limits = [300, 250, 100]

for memory_limit in memory_limits:

    benchmark_command = [
        "numactl",
        "-m",
        "0",
        "-N",
        "0",
        "./cmake-build-release/hyriseBenchmarkTPCH",
        "-m",
        "Shuffled",
        "-s",
        "100",
        "-t",
        "1200",
        "-w",
        "20",
        "-o",
        "benchmark_mmap_based_100_gb_page_cache_" + str(memory_limit) + "gb.json",
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
    for i in range(0, 3):
        print("********* Memory Metadata of benchmark process: *******************:")
        os.system("sudo cgget -r memory.current memory-limit")
        os.system("sudo cgget -r memory.pressure memory-limit")
        os.system("sudo cgget -r memory.stat memory-limit")

        time.sleep(30)

    try:
        sp.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        print(f"Benchmark {benchmark_command} timed out after {timeout_s} seconds. Killing it.")
        sp.kill()
