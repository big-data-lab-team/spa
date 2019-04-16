#!/usr/bin/env python3

from subprocess import Popen, PIPE
from os import path as op, linesep, getcwd, listdir
from random import shuffle
from tempfile import TemporaryFile, NamedTemporaryFile
import time
import shutil


iterations = 1
num_chunks = 125
project_dir = "/home/vhayots/projects/def-glatard/vhayots/spa/"
template_dir = op.join(project_dir, "example/standalone/hpc/")
cond_dir = op.join(project_dir, "experiments/code/conditions")
application = op.join(project_dir, "pilotspark.py")
hpc_batch_template = op.join(template_dir, "hpc_batch_template.sh")
hpc_pilot_template = op.join(template_dir, "hpc_pilot_template.sh")
batch_out = '/scratch/vhayots/spa/scalaout_batch'
pilot8_out = '/scratch/vhayots/spa/scalaout_8pilots'
pilot16_out = '/scratch/vhayots/spa/scalaout_16pilots'

batch_single = [application, hpc_batch_template,
                op.join(cond_dir, "hpc_scala_single.json"), "-B"]
batch_double = [application, hpc_batch_template,
                op.join(cond_dir, "hpc_scala_double.json"), "-B"]
batch_triple = [application, hpc_batch_template,
                op.join(cond_dir, "hpc_scala_triple.json"), "-B"]

experiments = [
                {
                    'cond' : '1dedicated',
                    'batch': batch_single,
                    '8pilot' :[application, hpc_pilot_template, op.join(cond_dir, "hpc_scala_8n1d_pilot.json")],
                    '16pilot': [application, hpc_pilot_template, op.join(cond_dir, "hpc_scala_16n1d_pilot.json")]

                },
                {
                    'cond' : '2dedicated',
                    'batch' : batch_double,
                    '8pilot' : [application, hpc_pilot_template, op.join(cond_dir, "hpc_scala_8n2d_pilot.json")],
                    '16pilot' : [application, hpc_pilot_template, op.join(cond_dir, "hpc_scala_16n2d_pilot.json")]
                },
                {
                    'cond' : '3dedicated',
                    'batch' : batch_triple,
                    '8pilot' : [application, hpc_pilot_template, op.join(cond_dir, "hpc_scala_8n3d_pilot.json")],
                    '16pilot' : [application, hpc_pilot_template, op.join(cond_dir, "hpc_scala_16n3d_pilot.json")]
                }
              ]

def get_results(sp, tmp_file, launch, cond, out_dir):
    try:
        sp.wait()
        tmp_file.seek(0)

        out_log = op.join(getcwd(), 'lb_{0}_{1}.out'.format(launch, 'cond'))
        print("Appending to logfile", out_log)
        with open(out_log, 'a+') as f:
            f.write(tmp_file.read())

            num_files = 0
            min_fs = 0
            if op.isdir(out_dir):
                num_files = len([name for name in listdir(out_dir) if op.isfile(op.join(out_dir, name))])
                min_fs = min([op.getsize(op.join(out_dir, name)) for name in listdir(out_dir) if op.isfile(op.join(out_dir, name))])

            status = None
            if min_fs < 652190352 or num_files < 125:
                status = "FAILED"
            else:
                status = "PASSED"
            f.write('***** {0} Experiment {1} : {2} {3}*****'.format(launch, ' '.join(exps[launch]),
                                                                                      status, num_files))
        tmp_file.close()
    except Exception as e:
        print(str(e))

    print("Completed obtaining results")

count = 0 
while count < iterations :
    shuffle(experiments)
    for exps in experiments:
        print(exps['batch'])
        print(exps['8pilot'])
        print(exps['16pilot'])
        f_batch = NamedTemporaryFile(mode='w+', prefix='batch_', suffix='_{}'.format(exps['cond']))
        f_8pilot = NamedTemporaryFile(mode='w+', prefix='pilot8_', suffix='_{}'.format(exps['cond']))
        f_16pilot = NamedTemporaryFile(mode='w+', prefix='pilot16', suffix='_{}'.format(exps['cond']))
        x = Popen(exps['batch'], bufsize=1, stdout=f_batch, stderr=f_batch)
        y = Popen(exps['8pilot'], bufsize=1, stdout=f_8pilot, stderr=f_8pilot)
        z = Popen(exps['16pilot'], bufsize=1, stdout=f_16pilot, stderr=f_16pilot)

        get_results(x, f_batch, "batch", exps['cond'], batch_out)
        get_results(y, f_8pilot, "8pilot", exps['cond'], pilot8_out)
        get_results(z, f_16pilot, "16pilot", exps['cond'], pilot16_out)

        try:
            shutil.rmtree(batch_out, ignore_errors=True)
            shutil.rmtree(pilot8_out, ignore_errors=True)
            shutil.rmtree(pilot16_out, ignore_errors=True)

        except Exception as e:
            print(str(e))

    count += 1
