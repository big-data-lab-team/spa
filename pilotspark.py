import argparse, time, json, subprocess
from datetime import datetime

def main():

    parser = argparse.ArgumentParser(description='Pilot-Agent scheduling for SLURM')
    parser.add_argument('conf', type=argparse.FileType('r'), help="SLURM batch script (JSON)")
    args = parser.parse_args()

    conf = None
    with args.conf as f:
        conf = json.load(f)

    program_start = datetime.now().strftime("%Y%m%d-%H%M%S%f")
    master_fn = "master-{}.sh".format(program_start)
    master_log = "{0}/master-{1}.txt".format(conf["logdir"], program_start)
    
    with open(master_fn, "w") as master:
        master.write("#!/bin/bash\n")
        
        for param in conf["master"]["sbatch"]:
            master.write("#SBATCH {0}={1}\n".format(param["id"], param["value"])) 

        master.write("\n\n")
    
        for path in conf["path"]:
            master.write("export {0}={1}\n".format(path["id"], path["value"])) 

        # start master
        master.write("export MASTER_URI=$(grep -Po 'spark://.*' $($SPARK_HOME/sbin/start-master.sh | grep -Po '/.*out')) \n")
        master.write("echo $MASTER_URI > {}\n".format(master_log))

        # run master
        master.write("srun -n 1 -N 1 $SPARK_HOME/bin/spark-submit --master $MASTER_URI {}\n".format(conf["master"]["program"]))
        
        # stop program
        master.write("$SPARK_HOME/sbin/stop-all.sh")        
   
    # SLURM batch submit master
    process = subprocess.Popen(['sbatch', master_fn], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.communicate()
         
    for i in range(conf["workers"]["amount"]):
        worker_fn = "worker-{0}-{1}.sh".format(program_start, i)

        walltime = None
        time = None
        with open(worker_fn, "w") as worker:
            worker.write("#!/bin/bash\n")

            for param in conf["workers"]["sbatch"]:
                worker.write("#SBATCH {0}={1}\n".format(param["id"], param["value"]))
                
                if param["id"] == "--time":
                    walltime = param["value"]

            worker.write("\n\n")

            for path in conf["path"]:
                worker.write("export {0}={1}\n".format(path["id"], path["value"]))

            # wait until master ip logfile has been created
            worker.write("while [ ! -f {} ]; do sleep 10; done \n".format(master_log))
            
            # get master ip
            worker.write("export MASTER_URI=$(cat {})\n".format(master_log))

            # start worker
            worker.write("$SPARK_HOME/sbin/start-slave.sh $MASTER_URI\n")

            if walltime != None:
                time = sum([a*b for a,b in zip([3600,60,1], [int(i) for i in walltime.split(":")])])

            worker.write("while true; do sleep 5; done\n")
            
            
        # SLURM batch submit workers
        process = subprocess.Popen(['sbatch', worker_fn], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.communicate()
   
        
        
        

if __name__ == '__main__':
    main()
