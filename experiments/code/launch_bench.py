#!/usr/bin/env python3

from subprocess import Popen, PIPE
from os import path as op, linesep, getcwd, listdir
from random import shuffle
from tempfile import TemporaryFile
import time
import shutil


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

shuffle(experiments)

count = 0 
while count < 3 :
    for exps in experiments:
        print(exps['batch'])
        print(exps['pilot'])
        f_batch = TemporaryFile(prefix='batch', suffix=exps['cond'])
        f_pilot = TemporaryFile(prefix='pilot', suffix=exps['cond'])
        x = Popen(exps['batch'], stdout=f_batch, stderr=f_batch)
        y = Popen(exps['pilot'], stdout=f_pilot, stderr=f_pilot)

        try:
            x.wait()
            f_batch.seek(0)
            with open(op.join(getcwd(), 'lb_batch_{}.out'.format(exps['cond'])), 'a+') as f:
                f.write(str(f_batch.read(), 'utf-8'))
                num_files = len([name for name in listdir(batch_out) if op.isfile(op.join(batch_out, name))])

                if num_files != num_chunks:
                    f.write('***** Batch Experiment {} : FAILED*****'.format(' '.join(exps['batch'])))
                else:
                    f.write('***** Batch Experiment {} : PASSED*****'.format(' '.join(exps['batch'])))
            f_batch.close()

            y.wait()
            f_pilot.seek(0)
            with open(op.join(getcwd(), 'lb_pilot_{}.out'.format(exps['cond'])), 'a+') as f:
                f.write(str(f_pilot.read(), 'utf-8'))
                num_files = len([name for name in listdir(pilot_out) if op.isfile(op.join(pilot_out, name))])

                if num_files != num_chunks:
                    f.write('***** Pilot Experiment {} : FAILED*****'.format(' '.join(exps['pilot'])))
                else:
                    f.write('***** Pilot Experiment {} : PASSED*****'.format(' '.join(exps['pilot'])))
            f_pilot.close()
            shutil.rmtree(batch_out)
            shutil.rmtree(pilot_out)

        except Exception as e:
            print(str(e))

    count += 1
