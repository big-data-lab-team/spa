#!/usr/bin/env python3
from os import path as op
from datetime import datetime
from slurmpy import Slurm
from subprocess import Popen, PIPE
from multiprocessing import cpu_count
import argparse
import time
import json
import os
import hashlib
import threading
import requests


def write_bench_start(bench):
    with open(bench, 'a+') as f:
        f.write(",".join([os.linesep + datetime.now().isoformat(),
                          str(time.time())]))


def write_bench_end(bench):
    with open(bench, 'a+') as f:
        f.write(',{}'.format(str(time.time())))


def gen_hash(template):
    return '{0}-{1}'.format(
            hashlib.sha1(template.encode("utf-8")).hexdigest(),
            hashlib.md5(os.urandom(16)).hexdigest())


def start_workers(s, num_nodes, compute_conf, template, rand_hash,
                  submit_func, jobs):

    for i in range(num_nodes):
        # Create masters/workers in the node
        if submit_func == "bash":
            thread = threading.Thread(target=s.run,
                                      kwargs=dict(command=template,
                                                  cmd_kwargs=compute_conf,
                                                  _cmd=submit_func))
            thread.daemon = True
            thread.start()

        # SLURM batch submit masters/workers
        else:
            worker_hash = "{0}-{1}".format(rand_hash, i)
            job_id = s.run(template, name_addition=worker_hash,
                           cmd_kwargs=compute_conf, _cmd=submit_func)
            jobs.append(str(job_id))

    while(num_nodes > 0
          and not op.isfile(compute_conf["mstr_log"])):
        time.sleep(5)

    master_id = ""
    with open(compute_conf["mstr_log"], 'r') as f:
        master_id = f.readline().strip(os.linesep)

    print(jobs)
    return master_id


def configure(conf, job_id, rand_hash):

    program_start = datetime.now().strftime("%Y-%m-%d")

    if "COMPUTE" not in conf:
        conf["COMPUTE"] = {}

    if "mstr_bench" not in conf["COMPUTE"]:
        conf["COMPUTE"]["mstr_bench"] = op.join(
                conf["logdir"],
                "master-{0}-benchmarks.{1}.out".format(program_start, job_id))

    if "mstr_log" not in conf["COMPUTE"]:
        conf["COMPUTE"]["mstr_log"] = op.join(
                conf["logdir"],
                "master-{0}-{1}.out".format(program_start, rand_hash))

    if ("deploy" in conf["DRIVER"] and conf["DRIVER"]["deploy"] == "cluster"
        and "drvr_log" not in conf["COMPUTE"]):
        conf["COMPUTE"]["drvr_log"] = op.join(
                conf["logdir"],
                "driver-{0}-{1}.out".format(program_start, rand_hash))

    conf["COMPUTE"]["mstr_lock"] = op.join(
            conf["logdir"],
            "master-{0}-{1}.lock".format(program_start, rand_hash))


    conf["COMPUTE"]["drvr_lock"] = op.join(
            conf["logdir"],
            "driver-{0}-{1}.lock".format(program_start, rand_hash))

    conf["COMPUTE"]["logdir"] = conf["logdir"]

    return program_start


def submit_sbatch(template, conf):

    if "benchmark" in conf:
        write_bench_start(conf["benchmark"])

    submit_func = "sbatch"
    rand_hash = "" #gen_hash(template)
    job_id = '${SLURM_JOB_ID}'
    configure(conf, job_id, rand_hash)
    s = Slurm(conf["name"], conf["SLURM_CONF_GLOBAL"])
    conf["DRIVER"]["mstr_bench"] = conf["COMPUTE"]["mstr_bench"]
    job_id = s.run(template, cmd_kwargs=conf["DRIVER"], _cmd=submit_func)

    job_id = str(job_id)
    condition = True
    time.sleep(5)

    while condition:
        p = Popen(["squeue", "-j", job_id], stdout=PIPE, stderr=PIPE)
        (out, err) = p.communicate()
        out = str(out, 'utf-8')

        print("Squeue output: ", out)

        out = out.split(os.linesep)
        out.pop(0)
        queue = [l.split(' ')[0] for l in out if l.split(' ') != '']

        condition = job_id in queue
        time.sleep(5 * 60)

    if "benchmark" in conf:
        write_bench_end(conf["benchmark"])


def submit_locally(template, conf):

    submit_func = "bash"
    rand_hash = gen_hash(template)
    job_id = ""

    configure(conf, job_id, rand_hash)
    s = Slurm(conf["name"], conf["SLURM_CONF_GLOBAL"])
    master_url = start_workers(s, conf["num_nodes"], conf["COMPUTE"],
                               template, rand_hash, submit_func)

    program = ["spark-submit", "--master", master_url]

    if "jars" in conf["DRIVER"]:
        program.extend(["--jars", conf["DRIVER"]["jars"]])

    program.append(conf["DRIVER"]["program"])

    p = Popen(program, stdout=PIPE, stderr=PIPE)
    stdin, stderr = p.communicate()
    print(stdin, stderr)


def submit_pilots(template, conf):
    
    if "benchmark" in conf:
        write_bench_start(conf["benchmark"])

    submit_func = "sbatch"
    rand_hash = gen_hash(template)
    slurm_job_id = '${SLURM_JOB_ID}'
    s = Slurm(conf["name"], conf["SLURM_CONF_GLOBAL"])
    program_start = configure(conf, slurm_job_id, rand_hash)
    jobs = []

    if conf["DRIVER"]["deploy"] == "cluster":
        program = ["\'spark-submit", "--master", "$MASTER_URI",
                   "--executor-cores=${SLURM_CPUS_PER_TASK}",
                   "--executor-memory=${SLURM_SPARK_MEM}M",
                   "--deploy-mode cluster"]

        if "jars" in conf["DRIVER"]:
            program.extend(["--jars", conf["DRIVER"]["jars"]])

        program.extend([conf["DRIVER"]["program"], "\'"])
        conf["COMPUTE"]["driver_prog"] = " ".join(program)

    master_url = start_workers(s, conf["num_nodes"], conf["COMPUTE"],
                               template, rand_hash, "sbatch", jobs)

    # PySpark is only possible in client mode, therefore deploying driver
    # in a slurm interactive node as a temporary solution
    if conf["DRIVER"]["deploy"] == "client":
        program = None
        driver_out = op.join(conf["logdir"],
                             "driver-{0}-{1}.out".format(program_start, rand_hash))
        fw = open(driver_out, "wb")
        fr = open(driver_out, "r")
        p = Popen(conf["DRIVER"]["slurm_alloc"], stdin=PIPE, stdout=fw,
                  stderr=fw, bufsize=1)
        for module in conf["DRIVER"]["modules"]:
            p.stdin.write("module load {}\n".format(module).encode('utf-8'))

        p.stdin.write("echo start $(date +%s.%N)\n".encode('utf-8'))

        program = ["spark-submit", "--master", master_url,
                   "--executor-cores=${SLURM_CPUS_PER_TASK}",
                   "--executor-memory=${SLURM_MEM_PER_NODE}M"]
        
        if "jars" in conf["DRIVER"]:
            program.extend(["--jars", conf["DRIVER"]["jars"]])

        program.extend([conf["DRIVER"]["program"], "\n"])

        program = " ".join(program)

        p.stdin.write(program.encode('utf-8'))

        out = fr.read()

        p.stdin.write("echo end $(date +%s.%N)\n".encode('utf-8'))
        p.stdin.write("echo 'SUCCEEDED' >> {}".format(
                     conf["COMPUTE"]["mstr_log"]).encode('utf-8'))
        fw.close()
        fr.close()

    else:
        driver_rest = ""
        driver_api = None
        while not op.isfile(conf["COMPUTE"]["drvr_log"]):
            print("driver log not created")
            time.sleep(5)
        with open(conf["COMPUTE"]["drvr_log"], 'r') as f:
            driver_rest = f.readline().strip(os.linesep)

        time.sleep(5)
        try:
            r = requests.get(driver_rest)
            driver_api = r.json()
            print(driver_api)
        except Exception as e:
            print(str(e))

        while (driver_api is not None and driver_api["driverState"] == "SUBMITTED"):
            time.sleep(5)

            try:
                r = requests.get(driver_rest)
                driver_api = r.json()
                print(driver_api)
            except Exception as e:
                print(str(e))
                break

        while (driver_api is not None and driver_api["driverState"] == "RUNNING"):
            p = Popen(["squeue", "-j", ",".join(jobs)], stdout=PIPE, stderr=PIPE)
            (out, err) = p.communicate()
            out = str(out, 'utf-8')
            err = str(err, 'utf-8')
            print("stdout: ", out)
            print("stderr: ", err)

            if 'send/recv' in out or 'send/recv' in err:
                continue
        
            running_jobs = out.split(os.linesep)
            running_jobs = [l.split(' ')[0] for l in running_jobs if l.split(' ')[0] != '']

            print('Running jobs:', running_jobs)
            jobs = list(set(jobs).intersection(running_jobs))

            nodes_to_start = conf["num_nodes"] - len(jobs)

            if nodes_to_start > 0:
                master_url = start_workers(s, nodes_to_start, conf["COMPUTE"],
                                           template, rand_hash, "sbatch", jobs)

            time.sleep(5 * 60)

            try:
                r = requests.get(driver_rest)
                driver_api = r.json()
                print(driver_api)

            except Exception as e:
                print(str(e))
                break

        if len(jobs) > 0:
            args = ["scancel"]
            args.extend(jobs)
            print('Cancelling jobs:', args)
            p = Popen(args, stdout=PIPE, stderr=PIPE)
            (out, err) = p.communicate()
            print(str(out, 'utf-8'), str(err, 'utf-8'))

    if "benchmark" in conf:
        write_bench_end(conf["benchmark"])


def main():

    parser = argparse.ArgumentParser(
            description='Pilot-Agent scheduling for SLURM')

    parser.add_argument('template', type=str,
                        help="SLURM batch script template")
    parser.add_argument('params', type=argparse.FileType('r'),
                        help="SLURM batch script params (JSON)")
    parser.add_argument('-B', '--batch_submit', action='store_true',
                        help="Batch submit request for resources (no pilots)")
    parser.add_argument('-D', '--no_submit', action='store_true',
                        help="Create but do not submit sbatch scripts")
    args = parser.parse_args()

    conf = None
    with args.params as f:
        conf = json.load(f)

    if args.no_submit:
        submit_locally(args.template, conf)
    elif args.batch_submit:
        submit_sbatch(args.template, conf)
    else:
        submit_pilots(args.template, conf)


if __name__ == '__main__':
    main()
