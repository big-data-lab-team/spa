#!/usr/bin/env python3

from subprocess import Popen, PIPE
from os import path as op, linesep, getcwd, listdir
from random import shuffle
from tempfile import TemporaryFile
import time
import shutil


iterations = 1
num_chunks = 125
template_dir = "example/standalone/hpc/"
cond_dir = "/home/vhayots/project/vhayots/spa-temp/experiments/code/conditions"
application = "/home/vhayots/project/vhayots/spa-temp/pilotspark.py"
hpc_batch_template_multin = op.join(template_dir, "hpc_batch_template.sh")
hpc_pilot_template = op.join(template_dir, "hpc_pilot_template.sh")
batch_out = '/scratch/vhayots/scalaout2'
pilot_out = '/scratch/vhayots/scalaout'

batch_single = [application, op.join(template_dir, "hpc_batch_singlenode.sh"),
                op.join(cond_dir, "hpc_scala_single.json"), "-B"]
batch_double = [application, hpc_batch_template_multin,
                op.join(cond_dir, "hpc_scala_double.json"), "-B"]
batch_triple = [application, hpc_batch_template_multin,
                op.join(cond_dir, "hpc_scala_triple.json"), "-B"]

experiments = [
                {
                    'cond' : '1dedicated_8p',
                    'batch': batch_single,
                    'pilot' :[application, hpc_pilot_template, op.join(cond_dir, "hpc_scala_8n1d_pilot.json")]
                },
                {
                    'cond' : '1dedicated_16p',
                    'batch' : batch_single,
                    'pilot' : [application, hpc_pilot_template, op.join(cond_dir, "hpc_scala_16n1d_pilot.json")]
                },
                {
                    'cond' : '2dedicated_8p',
                    'batch' : batch_double,
                    'pilot' : [application, hpc_pilot_template, op.join(cond_dir, "hpc_scala_8n2d_pilot.json")]
                },
                {
                    'cond' : '2dedicated_16p',
                    'batch' : batch_double,
                    'pilot' : [application, hpc_pilot_template, op.join(cond_dir, "hpc_scala_16n2d_pilot.json")]
                },
                {
                    'cond' : '3dedicated_8p',
                    'batch' : batch_triple,
                    'pilot' : [application, hpc_pilot_template, op.join(cond_dir, "hpc_scala_8n3d_pilot.json")]
                },
                {
                    'cond' : '3dedicated_16p',
                    'batch' : batch_triple,
                    'pilot' : [application, hpc_pilot_template, op.join(cond_dir, "hpc_scala_16n3d_pilot.json")]
                }
              ]

def get_results(sp, tmp_file, launch, cond, out_dir):
    sp.wait()
    tmp_file.seek(0)

    with open(op.join(getcwd(), 'lb_{0}_{1}.out'.format(launch, 'cond')), 'a+') as f:
        f.write(str(tmp_file.read(), 'utf-8'))
        num_files = len([name for name in listdir(out_dir) if op.isfile(op.join(out_dir, name))])

        status = None
        if num_files != num_chunks:
            status = "FAILED"
        else:
            status = "PASSED"
    f.write('***** {0} Experiment {1} : {2} {3}*****'.format(launch, ' '.join(exps[launch]),
                                                                              status, num_files))
    tmp_file.close()


shuffle(experiments)

count = 0 
while count < iterations :
    for exps in experiments:
        print(exps['batch'])
        print(exps['pilot'])
        f_batch = TemporaryFile(prefix='batch', suffix=exps['cond'])
        f_pilot = TemporaryFile(prefix='pilot', suffix=exps['cond'])
        x = Popen(exps['batch'], stdout=f_batch, stderr=f_batch)
        y = Popen(exps['pilot'], stdout=f_pilot, stderr=f_pilot)

        try:
            get_results(x, f_batch, "batch", exps['cond'], batch_out)
            get_results(y, f_pilot, "pilot", exps['cond'], pilot_out)
            shutil.rmtree(batch_out)
            shutil.rmtree(pilot_out)

        except Exception as e:
            print(str(e))

    count += 1
