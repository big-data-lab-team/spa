#!/usr/bin/env python3
import os
from os import path as op
import sys
import hashlib

def gen_hash(template):
    return '{0}-{1}'.format(
            hashlib.sha1(template.encode("utf-8")).hexdigest(),
            hashlib.md5(os.urandom(16)).hexdigest())

import logging
rand_hash = gen_hash(sys.argv[1])
hist_fn = op.abspath('pilot_{0}.log'.format(rand_hash))

logging.basicConfig(filename=hist_fn, level=logging.DEBUG,
                    filemode='w+',
                    format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')

from datetime import datetime
from slurmpy import Slurm
from subprocess import Popen, PIPE
from multiprocessing import cpu_count
import argparse
import time
import json
import threading
import requests
from copy import deepcopy
import glob


def write_bench_start(bench):
    with open(bench, 'a+') as f:
        f.write(",".join([os.linesep + datetime.now().isoformat(),
                          str(time.time())]))


def write_bench_end(bench):
    with open(bench, 'a+') as f:
        f.write(',{}'.format(str(time.time())))


def write_bench_result(bench, result):
    with open(bench, 'a+') as f:
        f.write(',{}'.format(result))

def job_status(filename):
    with open(filename, 'r') as df:
        errors = ["{0} {1}".format(i,l) for i,l in enumerate(df) if 'ERROR' in l]

    if len(errors) > 0:
        logging.error('Program errored at lines:\n %s', '\n'.join(errors))
        result = 'ERRORED'
    else:
        result = 'FINISHED'
    return result


def start_workers(s, num_nodes, compute_conf, template, rand_hash,
                  submit_func, jobs):

    logging.info('Launching pilots')
    for i in range(num_nodes):
        # Create masters/workers in the node
        if submit_func == "bash":
            logging.info('Launching pilot thread')
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
            logging.info('Launched SLURM pilot %s', job_id)
            jobs.append(str(job_id))

    while(num_nodes > 0
          and not op.isfile(compute_conf["mstr_log"])):
        logging.warning('Master not initialized. Sleeping for 5 seconds')
        time.sleep(5)

    master_id = ""
    with open(compute_conf["mstr_log"], 'r') as f:
        master_id = f.readline().strip(os.linesep)
        logging.info('Master created with id %s', master_id)

    #print(jobs)
    return master_id


def configure(conf, job_id, rand_hash):

    logging.debug('Configuring SLURM/Pilot template')
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

    logging.debug('Done configuration')
    return program_start


def submit_sbatch(template, conf):
    logging.warning(hist_fn)

    logging.info('Starting batch submission')

    if "benchmark" in conf:
        write_bench_start(conf["benchmark"])

    submit_func = "sbatch"
    rand_hash = "" #gen_hash(template)
    job_id = '${SLURM_JOB_ID}'
    program_start = configure(conf, job_id, rand_hash)
    s = Slurm(conf["name"], conf["SLURM_CONF_GLOBAL"])
    conf["DRIVER"]["mstr_bench"] = conf["COMPUTE"]["mstr_bench"]
    logging.info('Command to be executed: %s', conf["DRIVER"]["program"])
    job_id = s.run(template, cmd_kwargs=conf["DRIVER"], _cmd=submit_func)

    job_id = str(job_id)

    logging.info('Batch job ID: %s', job_id)
    condition = True
    time.sleep(5)

    while condition:
        p = Popen(["squeue", "-j", job_id], stdout=PIPE, stderr=PIPE)
        (out, err) = p.communicate()
        out = str(out, 'utf-8')

        logging.debug("Squeue output: %s", out)

        out = out.split(os.linesep)
        out.pop(0)
        queue = [l.strip().split(' ')[0] for l in out if l.strip().split(' ') != '']

        condition = job_id in queue
        if condition:
            logging.info('Job still running, sleeping for 5 mins')
            time.sleep(5 * 60)

    logging.info('Batch Job terminated')
    result = 'UNKNOWN'
    logfile = [op.join(d,f) for d,s,lf in os.walk(op.abspath('logs')) for f in lf if '{}.err'.format(job_id) in f]

    if len(logfile) > 0:
        logging.info('Driver logfile: %s', logfile[0])
        result = job_status(logfile[0])
    else:
        logging.warning('No logfile generated.')

    if "benchmark" in conf:
        write_bench_end(conf["benchmark"])
        write_bench_result(conf["benchmark"], result)


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

    submit_func = "sbatch"
    slurm_job_id = '${SLURM_JOB_ID}'
    s = Slurm(conf["name"], conf["SLURM_CONF_GLOBAL"])
    program_start = configure(conf, slurm_job_id, rand_hash)
    
    logging.warning(hist_fn)

    logging.info('Starting pilot submission')
    if "benchmark" in conf:
        write_bench_start(conf["benchmark"])

    jobs = []
    result = "UNKNOWN"

    if conf["DRIVER"]["deploy"] == "cluster":
        program = ["\'spark-submit", "--master", "$MASTER_URI",
                   "--executor-cores=${SLURM_CPUS_PER_TASK}",
                   "--executor-memory=${SLURM_SPARK_MEM}M",
                   "--deploy-mode cluster"]

        if "jars" in conf["DRIVER"]:
            program.extend(["--jars", conf["DRIVER"]["jars"]])

        program.extend([conf["DRIVER"]["program"], "\'"])
        conf["COMPUTE"]["driver_prog"] = " ".join(program)

    logging.info('Program to launch: %s', program)
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
        driver_id = None
        spark_log = op.abspath('sworker_logs')
        logging.info('Spark worker log directory: %s', spark_log)

        while not op.isfile(conf["COMPUTE"]["drvr_log"]):
            logging.warning("Driver log %s not created. Sleeping for 5 seconds",
                         conf["COMPUTE"]["drvr_log"])
            time.sleep(5)

        while "http" not in driver_rest:
            with open(conf["COMPUTE"]["drvr_log"], 'r') as f:
                driver_rest = f.readline().strip(os.linesep)
                logging.info('Driver URL at: %s', driver_rest)
            time.sleep(5)

        try:
            r = requests.get(driver_rest)
            driver_api = r.json()
            logging.debug("Driver status: %s", driver_api)
            driver_id = driver_api["submissionId"]
        except Exception as e:
            logging.error(str(e))

        while (driver_api is not None and "driverState" in driver_api
               and driver_api["driverState"] == "SUBMITTED"):
            logging.warning('Driver in SUBMITTED state. Sleeping for 5 seconds')
            time.sleep(5)

            try:
                r = requests.get(driver_rest)
                driver_api = r.json()
                logging.debug(driver_api)
                driver_id = driver_api["submissionId"]
            except Exception as e:
                logging.error(str(e))
                break

        while (driver_api is not None and "driverState" in driver_api
               and driver_api["driverState"] == "RUNNING"):
            logging.info('Driver currently in RUNNING state')
            p = Popen(["squeue", "-j", ",".join(jobs)], stdout=PIPE, stderr=PIPE)
            (out, err) = p.communicate()
            out = str(out, 'utf-8')
            err = str(err, 'utf-8')
            logging.debug("stdout: %s", out)
            logging.debug("stderr: %s", err)

            if 'send/recv' in out or 'send/recv' in err:
                continue
        
            running_jobs = out.split(os.linesep)[1:]
            running_jobs = [l.strip().split(' ')[0] for l in running_jobs if l.strip().split(' ')[0] != '']

            logging.info('Currently running jobs: %s', " ,".join(running_jobs))
            jobs = list(set(jobs).intersection(running_jobs))

            nodes_to_start = conf["num_nodes"] - len(jobs)
            old_jobs = deepcopy(jobs)

            if nodes_to_start != conf["num_nodes"] and nodes_to_start > 0:
                logging.info('Starting %d new pilots', nodes_to_start)
                master_url = start_workers(s, nodes_to_start, conf["COMPUTE"],
                                           template, rand_hash, "sbatch", jobs)

            time.sleep(5 * 60)

            try:
                r = requests.get(driver_rest)
                driver_api = r.json()

                result = driver_api["driverState"]
                #driver_id = driver_api["submissionId"]
                #print(driver_api)

            except Exception as e:
                logging.error(str(e))
                break

        logging.info('Program execution completed')
        if len(jobs) > 0:
            args = ["scancel"]
            args.extend(jobs)
            logging.info('Cancelling jobs: %s', ', '.join(jobs))
            p = Popen(args, stdout=PIPE, stderr=PIPE)
            (out, err) = p.communicate()
            logging.debug('stdout: %s', str(out, 'utf-8'))
            logging.debug('stderr: %s', str(err, 'utf-8'))

    if "benchmark" in conf:
        if result != 'FINISHED' and driver_id is not None:
            logging.debug('Driver ID: %s', driver_id)
            driver_log = [op.join(d, f[0]) for d,s,f in os.walk(spark_log) if driver_id in d]
            driver_err_log = [i for i in driver_log if 'stderr' in i]

            logging.debug('Driver logfile: %s', driver_err_log)
            
            if len(driver_err_log) > 0:

                result = job_status(driver_err_log[0])

        write_bench_end(conf["benchmark"])
        write_bench_result(conf["benchmark"], result)

    logging.shutdown()

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
