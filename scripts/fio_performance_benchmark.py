import os
import time
import subprocess


# fio --minimal hardcoded positions
fio_total_io_pos = 5
fio_bandwidth = 6
fio_runtime_under_test = 8
fio_bandwidth_mean = 44


kernel_version = os.uname()[2]
columns = "name,iterations,real_time,cpu_time,time_unit,bytes_per_second,items_per_second,label,error_occurred,error_message"
f = open(kernel_version + time.strftime("%H%M%S") + "-fio.csv", "w+")
f.write(columns + "\n")


#single threaded
for fio_size in ('10M', '100M', '1000M'):
    for run in ('read', 'randread', 'write', 'randwrite'):
        numjobs = 1
        iodepth = 1

        command = "sudo fio -minimal  -name=fio-bandwidth --bs=128k --ioengine=libaio --iodepth=64 --size=" + str(fio_size) + " --direct=1 --rw=" + str(
            run) + " --filename=/dev/nvme0n1 --numjobs=" + str(
            numjobs) + "--iodepth=" + str(iodepth) + " --numjobs=" + str(numjobs) + " --group_reporting --thread --refill_buffers"  # + " --time_based --runtime=" + fio_runtime

        fio_type_offset = 0
        print(command)
        os.system("sleep 2")  # Give time to finish inflight IOs
        output = subprocess.check_output(command, shell=True)
        if "write" in run:
            fio_type_offset = 41

        # fio is called with --group_reporting. This means that all
        # statistics are group for different jobs.

        split_output = output.split(b';')

        total_io = float(split_output[fio_type_offset + fio_total_io_pos].decode("utf-8"));
        bandwidth = float(split_output[fio_type_offset + fio_bandwidth].decode("utf-8"));
        runtime = float(split_output[fio_type_offset + fio_runtime_under_test].decode("utf-8"));
        bandwidth_mean = float(split_output[fio_type_offset + fio_bandwidth_mean].decode("utf-8"));

        result = "\"FileIOMicroBenchmarkFixture/FIO_" + str(run) + "/" + str(fio_size) + "/" + str(
            numjobs) + "/" + "\"" + "," + str(1)

        result = result + "," + str(runtime * 1000)
        result = result + "," + str(runtime * 1000) + ",ns"
        result = result + "," + str(bandwidth * 1000)
        result = result + ",,,,"
        f.write(result + "\n")
        f.flush()

# multithreaded
for fio_size in ('10M', '100M', '1000M'):
    for run in ('read', 'randread', 'write', 'randwrite'):
        for numjobs in (1, 2, 4, 8, 16, 24, 32, 48, 64):

            iodepth = numjobs
            command = "sudo fio -minimal  -name=fio-bandwidth --bs=128k --ioengine=libaio --iodepth=64 --size=" + str(
                fio_size) + " --direct=1 --rw=" + str(
                run) + " --filename=/dev/nvme0n1 --numjobs=" + str(
                numjobs) + "--iodepth=" + str(iodepth) + " --numjobs=" + str(
                numjobs) + " --group_reporting --thread --refill_buffers"  # + " --time_based --runtime=" + fio_runtime

            fio_type_offset = 0

            os.system("sleep 2")  # Give time to finish inflight IOs
            output = subprocess.check_output(command, shell=True)
            if "write" in run:
                fio_type_offset = 41

            # fio is called with --group_reporting. This means that all
            # statistics are group for different jobs.

            # iops
            # print(output)

            split_output = output.split(b';')

            total_io = float(split_output[fio_type_offset + fio_total_io_pos].decode("utf-8"));
            bandwidth = float(split_output[fio_type_offset + fio_bandwidth].decode("utf-8"));
            runtime = float(split_output[fio_type_offset + fio_runtime_under_test].decode("utf-8"));
            bandwidth_mean = float(split_output[fio_type_offset + fio_bandwidth_mean].decode("utf-8"));

            result = "\"FileIOMicroBenchmarkFixture/FIO_" + str(run) + "/" + str(fio_size) + "/" + str(
                numjobs) + "/" + "\"" + "," + str(1)

            result = result + "," + str(runtime*1000)
            result = result + "," + str(runtime*1000) + ",ns"
            result = result + "," + str(bandwidth*1000)
            result = result + ",,,,"
            f.write(result + "\n")
            f.flush()

f.closed


