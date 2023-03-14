import subprocess

bc_realistic_scenario_hyrise_master = ['sudo', 'python3', 'scripts/benchmark_hyrise_multicore.py']
bc_realistic_scenario_hyrise_master_cwd = '/mnt/md0/Theresa.Hradilak/hyrise_master/hyrise'

bc_realistic_scenario_hyrise_mmap = ['sudo', 'python3', 'scripts/benchmark_realistic_mmap.py']
bc_realistic_scenario_hyrise_mmap_cwd = '/mnt/md0/Theresa.Hradilak/hyrise'

bc_hyrise_master_tpch_10 = ['numactl',
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
                            'benchmark_hyrise_master_sf_10_unlimitedgb.json'
                            ]
bc_hyrise_master_tpch_10_cwd = '/mnt/md0/Theresa.Hradilak/hyrise_master/hyrise'

bc_hyrise_mmap_limited_memory_sf_10 = ['sudo', 'python3', 'scripts/cgroup/run_cgroup_limited_benchmark.py']
bc_hyrise_mmap_limited_memory_sf_10_cwd = '/mnt/md0/Theresa.Hradilak/hyrise'

bc_hyrise_master_tpch_100 = ['numactl',
                            '-m',
                            '0',
                            '-N',
                            '0',
                            './cmake-build-release/hyriseBenchmarkTPCH',
                            '-m',
                            'Shuffled',
                            '--scheduler',
                            '-s',
                            '100',
                            '-t',
                            '600',
                            '-w',
                            '5',
                            '-o',
                            'benchmark_hyrise_master_sf_100_unlimitedgb.json'
                            ]
bc_hyrise_master_tpch_100_cwd = '/mnt/md0/Theresa.Hradilak/hyrise_master/hyrise'

bc_hyrise_mmap_limited_memory_sf_100 = ['sudo', 'python3', 'scripts/cgroup/run_cgroup_limited_benchmark_100.py']
bc_hyrise_mmap_limited_memory_sf_100_cwd = '/mnt/md0/Theresa.Hradilak/hyrise'


benchmarks =[
    (bc_realistic_scenario_hyrise_master, bc_realistic_scenario_hyrise_master_cwd),
    (bc_realistic_scenario_hyrise_mmap, bc_realistic_scenario_hyrise_mmap_cwd),
    (bc_hyrise_master_tpch_10, bc_hyrise_master_tpch_10_cwd),
    (bc_hyrise_mmap_limited_memory_sf_10, bc_hyrise_mmap_limited_memory_sf_10_cwd),
    (bc_hyrise_master_tpch_100, bc_hyrise_master_tpch_100_cwd),
    (bc_hyrise_mmap_limited_memory_sf_100, bc_hyrise_mmap_limited_memory_sf_100_cwd)
]

for benchmark in benchmarks:
    benchmark_command = benchmark[0]
    benchmark_cwd = benchmark[1]
    sp = subprocess.run(benchmark_command, cwd=benchmark_cwd, timeout=60)
    sp.wait()


# - Realistic Scenario new benchmark
# - hyrise_master
# - hyrise_mmap
# - Hyrise Master Compare -> with equal as Hyrise LGTM -> 10
# - Hyrise LGTM
# - SF 10
# - Hyrise Master Compare -> with equal as Hyrise LGTM -> 100
# - Hyrise LGTM
# - SF 100
