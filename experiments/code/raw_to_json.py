#!/usr/bin/env python3

import argparse
from os import listdir, path as op, linesep
from time import strptime
import json
import glob


out16p_fn_template = 'pilots16_{}_dedicated.json'
out8p_fn_template = 'pilots8_{}_dedicated.json'
outb_fn_template = 'batch_{}_dedicated.json'

## to help parse through other logfiles 
total_order = []

def convert_strtime(strtime):
    return strptime(strtime, '%Y-%m-%dT%X.%f')

def dump_to_file(fn, data):

    with open(fn, 'w+') as f:
        json.dump(data, f, indent=3)

def get_jobs(fn, sj_benchmark_dir, s_logs, exec_mode):

    job_ids = {}
    count = 0
    log_files = glob.glob(op.join(fn, '*{}*.out'.format(exec_mode)))

    for f in sorted(log_files, key=lambda x: op.basename(x).split('-')[0]):
        print(f)
        with open(f, 'r') as logfile:
            sjids = []
            sjelems = []
            log_status = False
            worker_logdir = None
            driver_id = None
            driver_path = None

            for line in logfile:
                if ('batch' not in exec_mode and 'Launched SLURM pilot' in line
                     or 'batch' in exec_mode and 'Batch job ID:' in line):
                    sjids.append(int(line.split(' ')[-1]))
                elif 'Spark worker log directory' in line:
                    worker_logdir = line.split(' ')[-1].strip(linesep)
                elif "'submissionId': " in line:
                    driver_id = line.split("'submissionId': ")[1].split(',')[0].strip("'")

            if driver_id is not None:
                driver_path = op.join(worker_logdir, driver_id)

            for sj in sjids:
                sj_elem = {}
                sj_elem['id'] = sj
                sj_elem['start_time'] = None
                sj_elem['end_time'] = None
                nodes, success = get_jobid_success(sj, s_logs)
                sj_elem['nodes'] = nodes

                if success or driver_id is None:
                    sj_elem['succeeded'] = success
                else:
                    success, workers = get_success(driver_path, list(nodes.keys())[0] if len(nodes) > 0 else None)
                    sj_elem['succeeded'] = success
                    sj_elem['nodes'] = workers

                sjelems.append(sj_elem)

                bench_dir = glob.glob(op.join(sj_benchmark_dir, '*benchmarks.{}.out'.format(sj)))

                if len(bench_dir) > 0:
                    with open(bench_dir[0], 'r') as f:
                        for bench in f:
                            if 'start' in bench:
                                sj_elem['start_time'] = float(bench.split(' ')[-1])
                            elif 'end' in bench:
                                sj_elem['end_time'] = float(bench.split(' ')[-1])

            job_ids[count] = sjelems
            count += 1


    return job_ids


def get_success(fp, node):
    success = False
    workers = set()
    if op.isdir(fp) and node is not None:
        with open(op.join(fp, 'stderr'), 'r') as f:
            for line in f:
                if 'Executor added' in line and node in line:
                    proc = line.split(' ')[-4].strip('(').strip(')').split(':')[1]
                    workers.add(proc)
                elif 'Finished task' in line and '125/125' in line:
                    success = True
    return success, { node: list(workers) }


def order_pilots(directory, sjids, exec_mode="batch"):
    
    if exec_mode == "batch":
        out_fn = outb_fn_template
    elif exec_mode == "8p":
        out_fn = out8p_fn_template
    else:
        out_fn = out16p_fn_template

    total_order = []
    for fn in listdir(directory):
        dedicated = None
        pilots = None
        if (exec_mode == 'batch' and 'batch' not in fn
            or exec_mode == '8p' and '8' not in fn
            or exec_mode == '16p' and '16' not in fn):
            continue
        elif (exec_mode != 'batch' and 'batch' in fn
              or exec_mode != '8p' and '8' in fn
              or exec_mode != '16p' and '16' in fn):
            print(fn)
            continue

        split_fn = fn.split('_')

        dedicated = int(split_fn[1].split('d')[0])
        if exec_mode != 'batch':
            pilots = int(split_fn[0].split('n')[0])
        else:
            if dedicated == 1:
                dedicated = 'single'
            elif dedicated == 2:
                dedicated = 'double'
            elif dedicated == 3:
                dedicated = 'triple'
            else:
                dedicated = 'quadruple'
        
        abs_fn = op.abspath(op.join(directory, fn))

        with open(abs_fn, 'r') as f:
            for line in f:
                if line == '' or line == linesep:
                    continue
                exec_instance = {}
                components = line.split(',')

                if exec_mode != 'batch':
                    exec_instance['name'] = '{0}n{1}d'.format(pilots, dedicated)
                else:
                    exec_instance['name'] = 'batch_{}'.format(dedicated)
                exec_instance['timestamp'] = components[0]
                exec_instance['start_time'] = float(components[1])

                if len(components) > 2:
                    exec_instance['end_time'] = float(components[2])
                    exec_instance['success'] = None
                else:
                    exec_instance['end_time'] = None
                    exec_instance['success'] = False
                
                print(exec_instance['name'])
                total_order.append(exec_instance)


    total_order.sort(key=lambda x: convert_strtime(x['timestamp']))

    print(len(total_order))
    for idx, elem in enumerate(total_order):
        
        node_workers = {}
        
        for sj in sjids[idx]:
            for k,v in sj['nodes'].items():
                if k not in node_workers:
                    node_workers[k] = v

        elem['worker_count'] = sum([len(el[1]) for el in node_workers.items()])
        elem['sid'] = sjids[idx]
        

        elem['success'] = True in [sj['succeeded'] for sj in sjids[idx]]


    dump_to_file(out_fn.format('total'), total_order)
    

def get_jobid_success(job_id, master_logs):
   
    logfile = glob.glob(op.join(op.abspath(master_logs), '*.{}.out'.format(job_id)))
    batch = False
    executors = {}

    if len(logfile) > 0:
        if 'batch' in logfile[0]:
            logfile = glob.glob(op.join(op.abspath(master_logs), '*.{}.err'.format(job_id)))
            batch = True

        with open(logfile[0], 'r') as f:
            for line in f:
                if not batch and 'NODE: ' in line:
                    executors[line.split(' ')[-1].strip('\n')] = []
                elif '"finishedexecutors"' in line:
                    return executors, False
                elif batch and 'Executor added' in line:
                    host_port = line.split(' ')[-4].strip('(').strip(')')
                    node, proc = host_port.split(':')
                    
                    if node in executors:
                        executors[node].add(proc)
                    else:
                        executors[node] = set([proc])

                elif batch and 'Finished task' in line and '125/125' in line:
                    for k in executors.keys():
                        executors[k] = list(executors[k])

                    return executors, True
    for k in executors.keys():
        executors[k] = list(executors[k])
    return executors, False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dir', type=str, help='The directory containing all makespan files')
    parser.add_argument('log', type=str, help='The logfile detailing slurm job ids and if they succeeded')
    parser.add_argument('pilot', type=str, help='Pilot benchmark dir')
    parser.add_argument('splog', type=str, help='Spark log folder')
    args = parser.parse_args()

    execution_modes = ["batch", "8p", "16p"]

    for exec_mode in execution_modes:
        sjids = get_jobs(args.log, args.pilot, args.splog, exec_mode)
        order_pilots(args.dir, sjids, exec_mode)

if __name__=='__main__':
    main()
