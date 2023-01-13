import os
import time
import subprocess

thread_range = [1, 2, 4, 8, 16, 32, 48, 64]

run_types = ['read', 'randread']

filesizes = ['1000M']

# fio --minimal hardcoded positions
fio_total_io_pos = 5
fio_bandwidth = 6
fio_runtime_under_test = 8
fio_bandwidth_mean = 44

kernel_version = os.uname()[2]
columns = "name,iterations,real_time,cpu_time,time_unit,bytes_per_second,items_per_second,label,error_occurred," \
          "error_message"
f = open(f"""fio_benchmark_{kernel_version}_{time.strftime("%H%M%S")}_fio.csv""", "w+")
f.write(columns + "\n")


def run_and_write_command(run, command, fio_type_offset, fio_size, numjobs):
    os.system("sleep 2")  # Give time to finish inflight IOs
    output = subprocess.check_output(command, shell=True)
    if "write" in run:
        fio_type_offset = 41
    # fio is called with --group_reporting. This means that all
    # statistics are group for different jobs.
    split_output = output.split(b';')
    total_io = float(split_output[fio_type_offset + fio_total_io_pos].decode("utf-8"))
    bandwidth = float(split_output[fio_type_offset + fio_bandwidth].decode("utf-8"))
    runtime = float(split_output[fio_type_offset + fio_runtime_under_test].decode("utf-8"))
    bandwidth_mean = float(split_output[fio_type_offset + fio_bandwidth_mean].decode("utf-8"))
    result = f"\"FileIOMicroBenchmarkFixture/FIO_{run}/{fio_size[:-1]}/{numjobs}/\",1,{str(runtime * 1000)},{str(runtime * 1000)},ns,{str(bandwidth * 1000)},,,,\n"""
    f.write(result)
    f.flush()


for fio_size in filesizes:
    for run in run_types:

        # multithreaded
        for numjobs in thread_range:
            command = f"""sudo fio -minimal -name=fio-bandwidth --bs=4k --size={
            fio_size} --direct=1 --rw={run} --filename=file.txt --numjobs={
            numjobs} --group_reporting --thread --refill_buffers --ioengine=sync"""

            fio_type_offset = 0
            print(command)
            run_and_write_command(run, command, fio_type_offset, fio_size, numjobs)

        # single-threaded
        numjobs = 1

        command = f"""sudo fio -minimal -name=fio-bandwidth --bs=4k --ioengine=sync --size={
        fio_size} --direct=1 --rw={
        run} --filename=file.txt --numjobs={
        numjobs} --group_reporting --refill_buffers"""

        fio_type_offset = 0
        print(command)
        run_and_write_command(run, command, fio_type_offset, fio_size, numjobs)

f.closed
