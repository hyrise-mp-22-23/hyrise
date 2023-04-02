#!/usr/bin/env python3

"""
This script benchmarks a hyrise implementation for the TPCH benchmark with setting varying memory limits for the execution
of the benchmark (not the setup!). The memory limits are set via cgroups and the process is immediately moved into the
cgroup after creation of the process. This is important as cgroups only measure memory that was allocated by a process
while being in the cgroup.
"""

import os
import json
import subprocess
import time
from collections import defaultdict

GB = 1000 * 1000 * 1000
unlimited = 200 * GB

timeout_s = 60 * 45  #max 45 minutes for TPC-H 10

memory_limits = [20, 16, 12, 11, 10, 9, 8, 7, 6]
warmup_time = 20
pagefault_stats = defaultdict(dict)

def get_memory_stats(cgroup_name, print_info=""):
    print(print_info)
    memory_stat = str(subprocess.check_output(['sudo', 'cgget', '-r', 'memory.stat', cgroup_name]))
    memory_events = str(subprocess.check_output(['sudo', 'cgget', '-r', 'memory.events', cgroup_name]))
    memory_pressure = str(subprocess.check_output(['sudo', 'cgget', '-r', 'memory.pressure', cgroup_name]))

    return {'memory_stat': memory_stat, 'memory_events': memory_events, 'memory_pressure': memory_pressure}

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
        f'{warmup_time}',
        '-o',
        'benchmark_mmap_based_page_cache_' + str(memory_limit) + 'gb.json'
        ]

    os.system("sudo rm *.bin")

    #create unique memory limit cgroup for each benchmark for easier measurements
    timestamp = time.time()
    cgroup_name = f"memory-limit-{timestamp}"
    print(f"Creating cgroup for memory limit and setting its memory.high property to {unlimited}.")
    os.system(f"sudo cgcreate -g memory:{cgroup_name}")

    os.system(f"sudo cgset -r memory.high={str(unlimited)} {cgroup_name}")
    os.system(f"sudo cgset -r memory.max={str(unlimited)} {cgroup_name}")

    print("Print result of memory limit setting on memory-limited group.")
    os.system(f"sudo cgget -r memory.high {cgroup_name}")
    os.system(f"sudo cgget -r memory.max {cgroup_name}")

    print("Executing command: " + subprocess.list2cmdline(benchmark_command) + "\n")

    p = subprocess.Popen(benchmark_command, stdout=subprocess.PIPE)

    setup_running = True
    while setup_running:
        for line in iter(p.stdout.readline, b''):
            print(line)
            if b'Starting Benchmark' in line:
                print("Moving benchmark process into memory-limited cgroup.")
                os.system(f"sudo cgclassify -g memory:{cgroup_name} {str(p.pid)}")
                print("Setting memory.high soft limit on memory-limit group.")
                os.system(f"sudo cgset -r memory.high={str(memory_limit * GB)} {cgroup_name}")
                os.system(f"sudo cgget -r memory.high {cgroup_name}")
            if b'Warming up for TPC-H 22' in line:
                setup_running = False
                #we still need to wait for warmup for TPC-H 22 to finish
                time.sleep(warmup_time)
                break

    print("Setup finished")
    pagefault_stats[memory_limit]['before'] = get_memory_stats(cgroup_name, "Get memory stats of cgroup after finished setup.")

    try:
        p.wait(timeout=timeout_s)
        pagefault_stats[memory_limit]['after'] = get_memory_stats(cgroup_name, "Get memory stats after benchmark finished.")
    except subprocess.TimeoutExpired:
            print(f"Benchmark {benchmark_command} timed out after {timeout_s} seconds. Killing it.")
            p.kill()

    #write pagefault_stats to file
    with open(f'pagefault_stats_{memory_limit}_gb.json', 'w') as f:
        f.write(json.dumps(pagefault_stats))
