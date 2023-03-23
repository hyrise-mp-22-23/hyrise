import subprocess
import itertools
import math
import os

#num physical cores, num logical cores
num_cores = [24, 48]
warmup = [True, False]
num_clients = [1, 2, 4, 8, 16, 24, 32, 48]
for num_core, num_client, do_warmup in itertools.product(num_cores, num_clients, warmup):
    os.system("sudo rm -rf *.bin")
    print(f'Num Core: {num_core}, NumClients: {num_client}, Warmup: {do_warmup}')
    benchmark_command = [
        'numactl',
        '-m',
        '0',
        '-N',
        '0',
        './cmake-build-release/hyriseBenchmarkTPCH',
        '-s',
        '10',
        '--scheduler',
        f'--clients={num_client}',
        f'--cores={num_core}',
        '-m',
        'Shuffled',
        '-t',
        '1200',
        '-o',
        f'benchmark_mmap_hyrise_{num_core}_cores_{num_client}_clients_{"with_warmup" if do_warmup else ""}.json'
    ]
    if (do_warmup):
        benchmark_command.append('-w')
        benchmark_command.append('20')

    print("Executing command: " + subprocess.list2cmdline(benchmark_command) + "\n")
    p = subprocess.Popen(benchmark_command, stdout=subprocess.PIPE)
    p.wait()