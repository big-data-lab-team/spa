#!/usr/bin/env python3

import argparse
from os import listdir, path as op, linesep
from time import strptime
import json
import glob


outp_fn_template = 'pilots_{}_dedicated.json'
outb_fn_template = 'batch_{}_dedicated.json'

## to help parse through other logfiles 
total_order = []

def convert_strtime(strtime):
    return strptime(strtime, '%Y-%m-%dT%X.%f')

def dump_to_file(fn, data):

    with open(fn, 'w+') as f:
        json.dump(data, f, indent=3)

def get_jobs(fn, sj_benchmark_dir, s_logs):

    job_ids = {}
    count = 1
    with open(fn, 'r') as logfile:
        sjids = []
        success = []
        sjelems = []
        for line in logfile:
            contents = line.split(' ')
            if len(contents) > 0 and "b'Submitted" in contents:
                sjids.append(int(contents[-1][:-2]))
                next_line = logfile.readline()
                while 'submitted:' in next_line:
                    contents = next_line.split(' ')
                    sjids.append(int(contents[-1][:-2]))
                    next_line = logfile.readline()

                for sj in sjids:
                    sj_elem = {}
                    sj_elem['id'] = sj
                    sj_elem['start_time'] = None
                    sj_elem['end_time'] = None
                    sj_elem['succeeded'] = get_jobid_success(sj, s_logs)
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
                sjids = []
                sjelems = []

    return job_ids


def order_pilots(directory, sjids, isbatch=False):
    
    if isbatch:
        out_fn = outb_fn_template
    else:
        out_fn = outp_fn_template

    for fn in listdir(directory):
        dedicated = None
        pilots = None
        if isbatch and 'batch' not in fn:
            continue
        elif not isbatch and 'batch' in fn:
            print(fn)
            continue

        split_fn = fn.split('_')

        dedicated = int(split_fn[1].split('d')[0])
        if not isbatch:
            pilots = int(split_fn[0].split('n')[0])
        else:
            if dedicated == 1:
                dedicated = 'single'
            elif dedicated == 2:
                dedicated = 'double'
            else:
                dedicated = 'triple'
        
        abs_fn = op.abspath(op.join(directory, fn))

        with open(abs_fn, 'r') as f:
            for line in f:
                if line == '' or line == linesep:
                    continue
                exec_instance = {}
                components = line.split(',')

                if not isbatch:
                    exec_instance['name'] = '{0}n{1}d'.format(pilots, dedicated)
                else:
                    exec_instance['name'] = 'batch_{}'.format(dedicated)
                exec_instance['timestamp'] = components[0]
                exec_instance['start_time'] = float(components[1])
                exec_instance['end_time'] = float(components[2])
                exec_instance['success'] = None
                
                total_order.append(exec_instance)


    total_order.sort(key=lambda x: convert_strtime(x['timestamp']))

    for idx, elem in enumerate(total_order):
        elem['sid'] = sjids[idx + 1]
        elem['success'] = True in [sj['succeeded']for sj in sjids[idx + 1]]


    dump_to_file(out_fn.format('total'), total_order)
    

def get_jobid_success(job_id, master_logs):
   
    logfile = glob.glob(op.join(op.abspath(master_logs), '*.{}.out'.format(job_id)))
    batch = False

    if len(logfile) > 0:
        if 'batch' in logfile[0]:
            logfile = glob.glob(op.join(op.abspath(master_logs), '*.{}.err'.format(job_id)))
            batch = True

        with open(logfile[0], 'r') as f:
            for line in f:
                if '"finishedexecutors"' in line:
                    if '"finishedexecutors" : [ ]' not in line:
                        return True
                elif batch and 'Finished task' in line and '125/125' in line:
                    return True
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dir', type=str, help='The directory containing all makespan files')
    parser.add_argument('log', type=str, help='The logfile detailing slurm job ids and if they succeeded')
    parser.add_argument('pilot', type=str, help='Pilot benchmark dir')
    parser.add_argument('splog', type=str, help='Spark log folder')
    parser.add_argument('-b', '--batch', action='store_true', help='Pilot benchmark dir')
    args = parser.parse_args()

    sjids = get_jobs(args.log, args.pilot, args.splog)
    order_pilots(args.dir, sjids, args.batch)

if __name__=='__main__':
    main()
