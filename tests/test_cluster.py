import pytest
import subprocess, os, shutil

logfile="dockerlogs"

def test_standalone():
    os.makedirs(logfile, exist_ok=True)
    proc = subprocess.Popen(['python3.6', 'pilotspark.py', 'example/standalone/cloud/cloud_pilot.sh', 'example/standalone/cloud/dockerparams.json', '-D'])
    assert not proc.wait()
    assert len(os.listdir(logfile)) == 3
    shutil.rmtree(logfile)
    #assert (str(proc.stdout.read()).split('\n')[:4]==open("tests/pageranks.txt","r").read()) 

def test_local():
    os.makedirs("dockerlogs", exist_ok=True)
    proc = subprocess.Popen(['python3.6', 'pilotspark.py', 'example/standalone/cloud/cloud_pilot.sh', 'example/standalone/cloud/dockerparams-local.json', '-D'])
    assert not proc.wait()
    assert len(os.listdir(logfile)) == 0
    shutil.rmtree(logfile)
