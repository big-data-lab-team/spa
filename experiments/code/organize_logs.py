#!/usr/bin/env python3

import json
import sys
import os
import glob
from shutil import copyfile

json_f = sys.argv[1]

def format_name(name):
    if 'batch' in name:
        return 'batch'
    else:
        return name[:-2].replace('n', 'p')

def get_conf(name):
    if 'single' in name or '1d' in name:
            return '1d'
    elif 'double' in name or '2d' in name:
        return '2d'
    elif 'triple' in name or '3d' in name:
        return '3d'
    else:
        return '4d'

with open(json_f, 'r') as f:
    data = json.load(f)

    exp_count = 0
    for exp in data:
        folder = 'cleaned_logs/exp-{0}-{1}'.format(exp_count, get_conf(exp['name']))
        os.makedirs(folder, exist_ok=True)
        
        exp_folder = os.path.join(folder, exp['name'])
        os.makedirs(exp_folder, exist_ok=True)
        pilot_log = sorted(glob.glob("../data/logger-out/*{}*".format(format_name(exp['name']))))[exp_count]
        print(pilot_log)

        if 'batch' not in exp['name']:
            driver_name = None
            with open(pilot_log, 'r') as f:
                for line in f:
                    if "'submissionId': '" in line:
                        driver_name = line.split("'submissionId': '")[1].split("', ")[0]
                    elif "GET /v1/submissions/status/" in line:
                        driver_name = line.split("GET /v1/submissions/status/")[1].split(' ')[0]

            if driver_name is not None:
                print(driver_name)
                try:
                    copyfile(os.path.join('sworker_logs', driver_name, 'stderr'), os.path.join(exp_folder, driver_name))
                except Exception as e:
                    print(str(e))
            else:
                print("ERROR: Driver name not found in logs. Experiment {0} - {1}".format(exp_count, exp['name']))



        copyfile(pilot_log, os.path.join(exp_folder, os.path.basename(pilot_log)))

        for sid in exp['sid']:
            for f in glob.glob("logs/*{}*".format(sid['id'])):
                copyfile(f, os.path.join(exp_folder, f.strip('logs/')))

        exp_count += 1
                

