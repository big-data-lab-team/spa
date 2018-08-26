from os import path as op
import argparse, time, json, os
import hashlib
from datetime import datetime
from slurmpy import Slurm
from subprocess import Popen, PIPE

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

    if args.yarn and 'COMPUTE' in os.environ:
        open(op.join(os.environ['HADOOP_HOME'], 'etc/hadoop/slaves'), 'w').close()
    elif args.yarn:
        open(op.join(conf["COMPUTE"]["HADOOP_HOME"], 'etc/hadoop/slaves'), 'w').close()

    submit_func = "bash" if args.no_submit else "sbatch"

    s = Slurm(conf["name"], conf["SLURM_CONF_GLOBAL"])

    program_start = datetime.now().strftime("%Y-%m-%d")

    rand_hash = '{0}-{1}'.format(hashlib.sha1(args.template.encode("utf-8")).hexdigest(), hashlib.md5(os.urandom(16)).hexdigest())
    job_id = rand_hash if args.no_submit else '${SLURM_JOB_ID}' 

    if not "COMPUTE" in conf:
        conf["COMPUTE"] = {}

    if not "mstr_bench" in conf["COMPUTE"]:
        conf["COMPUTE"]["mstr_bench"] = op.join(conf["logdir"], "master-{0}-benchmarks.{1}.out".format(program_start, job_id))

    if not "mstr_log" in conf["COMPUTE"]:
        conf["COMPUTE"]["mstr_log"] = op.join(conf["logdir"], "master-{0}-{1}.out".format(program_start, rand_hash))

    conf["COMPUTE"]["mstr_lock"] = op.join(conf["logdir"], "master-{0}-{1}.lock".format(program_start, rand_hash))
    
    conf["COMPUTE"]["logdir"] = conf["logdir"]   

    for i in range(conf["num_nodes"]):
            
        # SLURM batch submit workers
        s.run(args.template, name_addition=rand_hash, cmd_kwargs=conf["COMPUTE"], _cmd=submit_func)
        
    while not op.isfile(conf["COMPUTE"]["mstr_log"]):
        time.sleep(5)

    master_url = ""

    with open(conf["COMPUTE"]["mstr_log"], 'r') as f:
        master_url = f.readline().strip('\n')

    driver_out = op.join(conf["logdir"], "driver-{0}-{1}.out".format(program_start, rand_hash))

    fw = open(driver_out, "wb")
    fr = open(driver_out, "r")
    p = Popen(conf["DRIVER"]["slurm_alloc"], stdin = PIPE, stdout = fw, stderr = fw, bufsize = 1)
    for module in conf["DRIVER"]["modules"]:
        p.stdin.write("module load {}\n".format(module).encode('utf-8'))
    
    p.stdin.write("echo start $(date +%s.%N)\n".encode('utf-8'))
    program = ("spark-submit --master {0} --executor-cores=${{SLURM_CPUS_PER_TASK}} "
               "--executor-memory=${{SLURM_MEM_PER_NODE}}M {1}\n").format(master_url, conf["DRIVER"]["program"])

    p.stdin.write(program.encode('utf-8'))
    
    out = fr.read()

    p.stdin.write("echo end $(date +%s.%N)\n".encode('utf-8'))
    p.stdin.write("echo 'SUCCEEDED' >> {}".format(conf["COMPUTE"]["mstr_log"]).encode('utf-8'))
    fw.close()
    fr.close()



if __name__ == '__main__':
    main()
