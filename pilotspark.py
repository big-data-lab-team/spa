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


#conf["num_nodes"]
#conf["COMPUTE"]
def start_workers(s, num_nodes, compute_conf, template, rand_hash, submit_func):

    for i in range():
        # Create masters/workers in the node
        if args.no_submit:
            thread = threading.Thread(target=s.run,
                                      kwargs=dict(command=template,
                                                  cmd_kwargs=compute_conf,
                                                  _cmd=submit_func))
            thread.daemon = True
            thread.start()

        # SLURM batch submit masters/workers
        else:
            s.run(template, name_addition=rand_hash,
                  cmd_kwargs=compute_conf, _cmd=submit_func)


def submit_pilots(template, conf):

    submit_func = "bash" if args.no_submit else "sbatch"
    job_id = rand_hash if args.no_submit else '${SLURM_JOB_ID}'

    s = Slurm(conf["name"], conf["SLURM_CONF_GLOBAL"])

    program_start = datetime.now().strftime("%Y-%m-%d")

    rand_hash = '{0}-{1}'.format(
            hashlib.sha1(args.template.encode("utf-8")).hexdigest(),
            hashlib.md5(os.urandom(16)).hexdigest())

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

    conf["COMPUTE"]["mstr_lock"] = op.join(
            conf["logdir"],
            "master-{0}-{1}.lock".format(program_start, rand_hash))

    conf["COMPUTE"]["logdir"] = conf["logdir"]

    return master_url = start_workers(s, conf["num_nodes"], conf["COMPUTE"],
                                      template, rand_hash, "sbatch")

    while(conf["num_nodes"] > 0
          and not op.isfile(conf["COMPUTE"]["mstr_log"])):
        time.sleep(5)

    master_url = ""

    with open(conf["COMPUTE"]["mstr_log"], 'r') as f:
        master_url = f.readline().strip('\n')

    program = None
    driver_out = op.join(conf["logdir"],
                         "driver-{0}-{1}.out".format(program_start, rand_hash))

    # PySpark is only possible in client mode, therefore deploying driver
    # in a slurm interactive node as a temporary solution
    if not args.no_submit:
        fw = open(driver_out, "wb")
        fr = open(driver_out, "r")
        p = Popen(conf["DRIVER"]["slurm_alloc"], stdin=PIPE, stdout=fw,
                  stderr=fw, bufsize=1)
        for module in conf["DRIVER"]["modules"]:
            p.stdin.write("module load {}\n".format(module).encode('utf-8'))

        p.stdin.write("echo start $(date +%s.%N)\n".encode('utf-8'))
        program = ("spark-submit --master {0} "
                   "--executor-cores=${{SLURM_CPUS_PER_TASK}} "
                   "--executor-memory=${{SLURM_MEM_PER_NODE}}M  "
                   "--driver-memory=${{SLURM_MEM_PER_NODE}}M {1}\n") \
            .format(master_url, conf["DRIVER"]["program"])
        p.stdin.write(program.encode('utf-8'))

        out = fr.read()

        p.stdin.write("echo end $(date +%s.%N)\n".encode('utf-8'))
        p.stdin.write("echo 'SUCCEEDED' >> {}".format(
                     conf["COMPUTE"]["mstr_log"]).encode('utf-8'))
        fw.close()
        fr.close()

    # If only one node is required, run Spark in local mode
    elif conf["num_nodes"] == 1:
        program = ("spark-submit --master local[*] {}\n") \
                .format(conf["DRIVER"]["program"])
        p = Popen(program.split(), stdout=PIPE, stderr=PIPE)
        stdin, stderr = p.communicate()
        print(stdin, stderr)

    # Not submitting to Slurm, therefore execute driver in node
    else:
        program = ("spark-submit --master {0} {1}\n") \
                .format(master_url, conf["DRIVER"]["program"])
        p = Popen(program.split(), stdout=PIPE, stderr=PIPE)
        stdin, stderr = p.communicate()
        print(stdin, stderr)



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
        submit_locally(template, conf)
    elif args.batch_submit:
        submit_sbatch(template, conf)
    else:
        submit_pilots(template, conf)


if __name__ == '__main__':
    main()
