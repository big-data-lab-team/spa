#!/usr/bin/env python3

from subprocess import Popen, PIPE
from os import path as op, linesep, getcwd, listdir
from random import shuffle
from tempfile import TemporaryFile, NamedTemporaryFile
import time
import shutil
import sys


iterations = 5
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
batch_quad = [application, hpc_batch_template,
              op.join(cond_dir, "hpc_scala_quadruple.json"), "-B"]

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
                },
                {
                    'cond' : '4dedicated',
                    'batch' : batch_quad,
                    '8pilot' : [application, hpc_pilot_template, op.join(cond_dir, "hpc_scala_8n4d_pilot.json")],
                    '16pilot' : [application, hpc_pilot_template, op.join(cond_dir, "hpc_scala_16n4d_pilot.json")]
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
    if len(sys.argv) > 1 and sys.argv[1] == "--no_interleave":
        print("running without interleaving")
        exps = experiments[2]

        options = [('batch', batch_out), ('8pilot', pilot8_out), ('16pilot', pilot16_out)]
        shuffle(options)
        
        for opt in options:
            f = NamedTemporaryFile(mode='w+', prefix='{}_'.format(opt[0]), suffix='_{}'.format(exps['cond']))
            x = Popen(exps[opt[0]], bufsize=1, stdout=f, stderr=f)
            get_results(x, f, opt[0], exps['cond'], opt[1])

            try:
                shutil.rmtree(opt[1], ignore_errors=True)

            except Exception as e:
                print(str(e))
        
    else:
        shuffle(experiments)
        for exps in experiments:
            print(exps['batch'])
            print(exps['8pilot'])
            print(exps['16pilot'])
            f_batch = NamedTemporaryFile(mode='w+', prefix='batch_', suffix='_{}'.format(exps['cond']))
            f_8pilot = NamedTemporaryFile(mode='w+', prefix='pilot8_', suffix='_{}'.format(exps['cond']))
            f_16pilot = NamedTemporaryFile(mode='w+', prefix='pilot16', suffix='_{}'.format(exps['cond']))
            options = [('batch', f_batch, batch_out),
                       ('8pilot', f_8pilot, pilot8_out),
                       ('16pilot', f_16pilot, pilot16_out)]
            shuffle(options)
            x = Popen(exps[options[0][0]], bufsize=1, stdout=options[0][1], stderr=options[0][1])
            y = Popen(exps[options[1][0]], bufsize=1, stdout=options[1][1], stderr=options[1][1])
            z = Popen(exps[options[2][0]], bufsize=1, stdout=options[2][1], stderr=options[2][1])

            get_results(x, options[0][1], options[0][0], exps['cond'], options[0][2])
            get_results(y, options[1][1], options[1][0], exps['cond'], options[1][2])
            get_results(z, options[2][1], options[2][0], exps['cond'], options[2][2])

            try:
                shutil.rmtree(batch_out, ignore_errors=True)
                shutil.rmtree(pilot8_out, ignore_errors=True)
                shutil.rmtree(pilot16_out, ignore_errors=True)

            except Exception as e:
                print(str(e))

    count += 1
