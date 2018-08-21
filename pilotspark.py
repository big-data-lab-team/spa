from os import path as op
import argparse, time, json, subprocess, os
from datetime import datetime
from slurmpy import Slurm

def main():

    parser = argparse.ArgumentParser(description='Pilot-Agent scheduling for SLURM')
    parser.add_argument('template', type=str, help="SLURM batch script template")
    parser.add_argument('params', type=argparse.FileType('r'), help="SLURM batch script params (JSON)")
    parser.add_argument('-y', '--yarn', action='store_true', help="Yarn scheduler will be used")
    parser.add_argument('-D', '--no_submit', action='store_true', help="Create but do not submit sbatch scripts" )
    args = parser.parse_args()

    conf = None
    with args.params as f:
        conf = json.load(f)

    if args.yarn:
        open(op.join(os.environ['HADOOP_HOME'], 'etc/hadoop/slaves'), 'w').close()

    submit_func = "bash" if args.no_submit else "sbatch"

    s = Slurm("pilotspark", conf["SLURM_CONF_GLOBAL"])

    program_start = datetime.now().strftime("%Y%m%d-%H%M%S%f")

    if not "mstr_bench" in conf["COMPUTE"]:
        conf["COMPUTE"]["mstr_bench"] = op.join(conf["logdir"], "master-{}-benchmarks.$SLURM_JOB_ID.out".format(program_start))

    if not "mstr_log" in conf["COMPUTE"]:
        conf["COMPUTE"]["mstr_log"] = op.join(conf["logdir"], "master-{}.out".format(program_start))

    conf["COMPUTE"]["mstr_lock"] = op.join(conf["logdir"], "master-{}.lock".format(program_start))
    
    conf["COMPUTE"]["logdir"] = conf["logdir"]   

    for i in range(conf["num_nodes"]):
            
        # SLURM batch submit workers
        s.run(args.template, cmd_kwargs=conf["COMPUTE"], _cmd=submit_func)
        
        

if __name__ == '__main__':
    main()
