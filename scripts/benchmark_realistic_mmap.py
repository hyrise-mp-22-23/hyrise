import subprocess
import itertools
import math
import os

num_cores = [2, 4, 8, 16, 24, 32, 48]
for num_core, do_warmup in itertools.product(num_cores, [True]):
    os.system("sudo rm -rf *.bin")
    print(num_core, do_warmup)
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
        f'--clients={math.ceil(num_core/2)}',
        f'--cores={num_core}',
        '-m',
        'Shuffled',
        '-t',
        '600',
        '-o',
        f'benchmark_mmap_hyrise_{num_core}_cores_{"with_warmup" if do_warmup else ""}.json'
    ]
    if (do_warmup):
        benchmark_command.append('-w')
        benchmark_command.append('5')

    print("Executing command: " + subprocess.list2cmdline(benchmark_command) + "\n")
    p = subprocess.Popen(benchmark_command, stdout=subprocess.PIPE)
    p.wait()