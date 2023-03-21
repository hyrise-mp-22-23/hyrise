import subprocess
import os
import signal
import time

bc_realistic_scenario_martin_hyrise_master = ['sudo', 'python3', 'scripts/benchmark_hyrise_variable_multicore.py']
bc_realistic_scenario_martin_hyrise_master_cwd = '/mnt/md0/Theresa.Hradilak/hyrise_master/hyrise'

bc_realistic_scenario_marcel_hyrise_master = ['sudo', 'python3', 'scripts/benchmark_hyrise_fixed_multicore.py']
bc_realistic_scenario_marcel_hyrise_master_cwd = '/mnt/md0/Theresa.Hradilak/hyrise_master/hyrise'

bc_realistic_scenario_martin_hyrise_mmap = ['sudo', 'python3', 'scripts/benchmark_realistic_mmap_variable_multicore.py']
bc_realistic_scenario_martin_hyrise_mmap_cwd = '/mnt/md0/Theresa.Hradilak/hyrise'

bc_realistic_scenario_marcel_hyrise_mmap = ['sudo', 'python3', 'scripts/benchmark_realistic_mmap_fixed_multicore.py']
bc_realistic_scenario_marcel_hyrise_mmap_cwd = '/mnt/md0/Theresa.Hradilak/hyrise'

bc_hyrise_master_tpch_10 = ['numactl',
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
                            'benchmark_hyrise_master_sf_10_unlimitedgb.json'
                            ]
bc_hyrise_master_tpch_10_cwd = '/mnt/md0/Theresa.Hradilak/hyrise_master/hyrise'

bc_hyrise_mmap_limited_memory_sf_10 = ['sudo', 'python3', 'scripts/cgroups/run_cgroup_limited_benchmark.py']
bc_hyrise_mmap_limited_memory_sf_10_cwd = '/mnt/md0/Theresa.Hradilak/hyrise'

bc_hyrise_master_tpch_100 = ['numactl',
                             '-m',
                             '0',
                             '-N',
                             '0',
                             './cmake-build-release/hyriseBenchmarkTPCH',
                             '-m',
                             'Shuffled',
                             '-s',
                             '100',
                             '-t',
                             '1200',
                             '-w',
                             '20',
                             '-o',
                             'benchmark_hyrise_master_sf_100_unlimitedgb.json'
                             ]
bc_hyrise_master_tpch_100_cwd = '/mnt/md0/Theresa.Hradilak/hyrise_master/hyrise'

bc_hyrise_mmap_limited_memory_sf_100 = ['sudo', 'python3', 'scripts/cgroups/run_cgroup_limited_benchmark_100.py']
bc_hyrise_mmap_limited_memory_sf_100_cwd = '/mnt/md0/Theresa.Hradilak/hyrise'

benchmarks = [
    (bc_realistic_scenario_martin_hyrise_master, bc_realistic_scenario_martin_hyrise_master_cwd),
    (bc_realistic_scenario_marcel_hyrise_master, bc_realistic_scenario_marcel_hyrise_master_cwd),
    (bc_realistic_scenario_martin_hyrise_mmap, bc_realistic_scenario_martin_hyrise_mmap_cwd),
    (bc_realistic_scenario_marcel_hyrise_mmap, bc_realistic_scenario_marcel_hyrise_mmap_cwd),
    (bc_hyrise_master_tpch_10, bc_hyrise_master_tpch_10_cwd),
    (bc_hyrise_mmap_limited_memory_sf_10, bc_hyrise_mmap_limited_memory_sf_10_cwd),
    (bc_hyrise_master_tpch_100, bc_hyrise_master_tpch_100_cwd),
    (bc_hyrise_mmap_limited_memory_sf_100, bc_hyrise_mmap_limited_memory_sf_100_cwd)
]
#failsafe, scripts should handle own timeouts if in doubt
timeout_seconds = 20  # max 36 hours per benchmark script allowed

for benchmark in benchmarks:
    benchmark_command = benchmark[0]
    benchmark_cwd = benchmark[1]

    try:
        p = subprocess.Popen(benchmark_command, cwd=benchmark_cwd, start_new_session=True)
        p.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        print(f'Timeout for {benchmark_command} ({timeout_seconds}s) expired')
        print('Terminating the whole process group...')
        os.killpg(os.getpgid(p.pid), signal.SIGTERM)
        time.sleep(10)
