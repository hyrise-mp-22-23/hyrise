import subprocess
import itertools
import math

num_cores = [1, 2, 4, 6, 8, 12, 16, 24, 32, 48, 64]
for num_core, do_warmup in itertools.product(num_cores, [True]):
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
        f'--clients={num_core}',
        f'--cores={math.ceil(num_core/2)}',
        '-m',
        'Shuffled',
        '-t',
        '300',
        '-o',
        f'benchmark_hyrise_master_{num_core}_cores_{"with_warmup" if do_warmup else ""}.json'
    ]
    if (do_warmup):
        benchmark_command.append('-w')
        benchmark_command.append('20')

    print("Executing command: " + subprocess.list2cmdline(benchmark_command) + "\n")
    p = subprocess.Popen(benchmark_command, stdout=subprocess.PIPE)
    p.wait()