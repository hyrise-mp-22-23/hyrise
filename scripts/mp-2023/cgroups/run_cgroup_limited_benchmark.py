#!/usr/bin/env python3

"""
This script benchmarks a hyrise implementation for the TPCH benchmark with setting varying memory limits for the execution
of the benchmark (not the setup!). The memory limits are set via cgroups and the process is immediately moved into the
cgroup after creation of the process. This is important as cgroups only measure memory that was allocated by a process
while being in the cgroup.
"""

import os
import subprocess
import time

GB = 1000 * 1000 * 1000
unlimited = 200 * GB

timeout_s = 60 * 45  #max 45 minutes for TPC-H 10

memory_limits = [12]
pagefault_stats = {}

def get_memory_stats(cgroup_name, print_info=""):
    print(print_info)
    memory_stat = os.system(f"sudo cgget -r memory.stat {cgroup_name}")
    memory_events = os.system(f"sudo cgget -r memory.events {cgroup_name}")
    memory_pressure = os.system(f"sudo cgget -r memory.pressure {cgroup_name}")
    print(memory_stat, memory_events, memory_pressure)


for memory_limit in memory_limits:

    benchmark_command = [
        'numactl',
        '-m',
        '0',
        '-N',
        '0',
        './cmake-build-release/hyriseBenchmarkTPCH',
        # '-m',
        # 'Shuffled',
        '-s',
        '10',
        '-t',
        '300',
        '-w',
        '20',
        '-o',
        'benchmark_mmap_based_page_cache_' + str(memory_limit) + 'gb.json'
        ]

    os.system("sudo rm *.bin")
    print(f"Creating cgroup for memory limit and setting its memory.high property to {unlimited}.")
    os.system("sudo cgcreate -g memory:memory-limit")

    get_memory_stats("memory-limit", "Print memory stats of cgroup after creation.")

    os.system("sudo cgset -r memory.high=" + str(unlimited) + " memory-limit")
    os.system("sudo cgset -r memory.max=" + str(unlimited) + " memory-limit")

    print("Print result of memory limit setting on memory-limit group.")
    os.system("sudo cgget -r memory.high memory-limit")
    os.system("sudo cgget -r memory.max memory-limit")

    print("Executing command: " + subprocess.list2cmdline(benchmark_command) + "\n")

    sp = subprocess.Popen(benchmark_command)

    print("Moving benchmark process into memory-limited cgroup.")
    os.system("sudo cgclassify -g memory:memory-limit " + str(sp.pid))

    get_memory_stats("memory-limit", "Print memory stats of cgroup after moving benchmarking process into it.")

    print("Waiting 3.5 minutes to let setup finish...")
    time.sleep(3.5 * 60)

    get_memory_stats("memory-limit", "Print memory stats of cgroup after 3.5 minutes of setup.")

    print("Setting memory.high soft limit on memory-limit group.")
    os.system("sudo cgset -r memory.high=" + str(memory_limit * GB) + " memory-limit")
    os.system("sudo cgget -r memory.high memory-limit")

    print("Letting benchmark run for 3 minutes to allow reduction of memory footprint.")
    time.sleep(3 * 60)

    get_memory_stats("memory-limit", "Print memory stats of cgroup after waiting during warmup.")

    try:
        sp.wait(timeout=timeout_s)
        get_memory_stats("memory-limit", "Print memory stats after benchmark finished.")
    except subprocess.TimeoutExpired:
            print(f"Benchmark {benchmark_command} timed out after {timeout_s} seconds. Killing it.")
            sp.kill()
