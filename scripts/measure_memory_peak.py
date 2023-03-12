import os
import subprocess
import time
import json

scale_factors = [0.1, 1, 10, 50, 100]
benchmark_memory_sizes = {}
for scale_factor in scale_factors:
    memory_sizes = []
    benchmark_command = [
        'numactl',
        '-m',
        '0',
        '-N',
        '0',
        './cmake-build-release/hyriseBenchmarkTPCH',
        '-s',
        f'{scale_factor}',
        '-w',
        '20',
    ]

    print(f"Creating cgroup for memory peak measurement.")
    os.system("sudo cgcreate -g memory:memory-measure")

    print("Executing command: " + subprocess.list2cmdline(benchmark_command) + "\n")
    p = subprocess.Popen(benchmark_command, stdout=subprocess.PIPE)

    print("Moving benchmark process into memory-measure cgroup.")
    os.system("sudo cgclassify -g memory:memory-measure " + str(p.pid))

    setup_running = True
    while setup_running:
        for line in iter(p.stdout.readline, b''):
            print(line)
            if b'Starting Benchmark' in line:
                setup_running = False
                break
    print("Setup finished")

    print("Start measuring memory usage")
    while p.poll() is None:
        result = subprocess.check_output(['sudo', 'cgget', '-nv', '-r', 'memory.current', 'memory-measure'])
        current_memory_size = result.decode('utf-8').strip()
        memory_sizes.append(int(current_memory_size))
        time.sleep(1)

    print("Benchmark finished")

    benchmark_memory_sizes[scale_factor] = {'memory_peak': max(memory_sizes), 'memory_sizes': memory_sizes}

    with open('benchmark_memory_peak.json', 'w') as f:
        json.dump(benchmark_memory_sizes, f)