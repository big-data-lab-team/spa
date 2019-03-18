#!/usr/bin/env python3

import sys
import matplotlib.pyplot as plt
from json import load
from statistics import pstdev
from numpy import arange

batchfile = sys.argv[1]
pilotfile = sys.argv[2]


batch_1d_values = []
pilot8_1d_values = []
pilot16_1d_values = []
batch8_1d_values = []
batch16_1d_values = []

batch_2d_values = []
pilot8_2d_values = []
pilot16_2d_values = []
batch8_2d_values = []
batch16_2d_values = []

batch_3d_values = []
pilot8_3d_values = []
pilot16_3d_values = []
batch8_3d_values = []
batch16_3d_values = []

batch_values = {}
pilot_values = {}

all_batch = []
all_8pilots = []
all_16pilots = []
all_8batch = []
all_16batch = []

with open(batchfile, 'r') as f:
    batch_values = load(f)
with open(pilotfile, 'r') as f:
    pilot_values = load(f)

for elem in batch_values:
    #print(elem['name'], elem['success'], pilot_values[idx]['name'], pilot_values[idx]['success'])
    if 'single' in elem['name'] and elem['success']:
        batch_1d_values.append(elem['end_time'] - elem['start_time'])
    elif 'double' in elem['name'] and elem['success']:
        batch_2d_values.append(elem['end_time'] - elem['start_time'])
    elif 'triple' in elem['name'] and elem['success']:
        batch_3d_values.append(elem['end_time'] - elem['start_time'])
    
for idx, pilot in enumerate(pilot_values):
    if '8n1d' in pilot['name']:
        if pilot['success']:
            pilot8_1d_values.append(pilot['end_time'] - pilot['start_time'])
            print(pilot['name'], pilot8_1d_values[-1])
        
        if batch_values[idx]['success']:
            batch8_1d_values.append(batch_values[idx]['end_time'] - batch_values[idx]['start_time'])
    elif '16n1d' in pilot['name']:
        if pilot['success']:
            pilot16_1d_values.append(pilot['end_time'] - pilot['start_time'])
            print(pilot['name'], pilot16_1d_values[-1])

        if batch_values[idx]['success']:
            batch16_1d_values.append(batch_values[idx]['end_time'] - batch_values[idx]['start_time'])
    elif '8n2d' in pilot['name']:
        if pilot['success']:
            pilot8_2d_values.append(pilot['end_time'] - pilot['start_time'])
            print(pilot['name'], pilot8_2d_values[-1])

        if batch_values[idx]['success']:
            batch8_2d_values.append(batch_values[idx]['end_time'] - batch_values[idx]['start_time'])
    elif '16n2d' in pilot['name']:
        if pilot['success']:
            pilot16_2d_values.append(pilot['end_time'] - pilot['start_time'])
            print(pilot['name'], pilot16_2d_values[-1])

        if batch_values[idx]['success']:
            batch16_2d_values.append(batch_values[idx]['end_time'] - batch_values[idx]['start_time'])
    elif '8n3d' in pilot['name']:
        if pilot['success']:
            pilot8_3d_values.append(pilot['end_time'] - pilot['start_time'])
            print(pilot['name'], pilot8_3d_values[-1])

        if batch_values[idx]['success']:
            batch8_3d_values.append(batch_values[idx]['end_time'] - batch_values[idx]['start_time'])
    elif '16n3d' in pilot['name']:
        
        if pilot['success']:
            pilot16_3d_values.append(pilot['end_time'] - pilot['start_time'])
            print(pilot['name'], pilot16_3d_values[-1])

        if batch_values[idx]['success']:
            batch16_3d_values.append(batch_values[idx]['end_time'] - batch_values[idx]['start_time'])

def add_mean(values, l):
    if len(values) > 0:
        l.append(sum(values)/len(values))
    else:
        l.append(0)

add_mean(batch_1d_values, all_batch)
add_mean(batch_2d_values, all_batch)
add_mean(batch_3d_values, all_batch)
add_mean(pilot8_1d_values, all_8pilots)
add_mean(pilot8_2d_values, all_8pilots)
add_mean(pilot8_3d_values, all_8pilots)
add_mean(pilot16_1d_values, all_16pilots)
add_mean(pilot16_2d_values, all_16pilots)
add_mean(pilot16_3d_values, all_16pilots)
add_mean(batch8_1d_values, all_8batch)
add_mean(batch8_2d_values, all_8batch)
add_mean(batch8_3d_values, all_8batch)
add_mean(batch16_1d_values, all_16batch)
add_mean(batch16_2d_values, all_16batch)
add_mean(batch16_3d_values, all_16batch)

all_bl = [batch_1d_values, batch_2d_values, batch_3d_values]
all_8pl = [pilot8_1d_values, pilot8_2d_values, pilot8_3d_values]
all_16pl = [pilot16_1d_values, pilot16_2d_values, pilot16_3d_values]
all_8bl = [batch8_1d_values, batch8_2d_values, batch8_3d_values]
all_16bl = [batch16_1d_values, batch16_2d_values, batch16_3d_values]


all_batch_stdev = [pstdev(val) if len(val) > 1 else 0 for val in all_bl]
all_8pilots_stdev = [pstdev(val) if len(val) > 1 else 0 for val in all_8pl]
all_16pilots_stdev = [pstdev(val) if len(val) > 1 else 0 for val in all_16pl]
all_8batch_stdev = [pstdev(val) if len(val) > 1 else 0 for val in all_8bl]
all_16batch_stdev = [pstdev(val) if len(val) > 1 else 0 for val in all_16bl]

n_conf = 3
index = arange(n_conf)
bar_width = 0.2
opacity = 0.4

fig, ax = plt.subplots()

rects1 = ax.bar(index, all_batch, bar_width,
                alpha=opacity, color='b',
                yerr=all_batch_stdev, label='batch')

rects2 = ax.bar(index + bar_width, all_8pilots, bar_width,
                alpha=opacity, color='r',
                yerr=all_8pilots_stdev,
                label='8 pilots')

rects3 = ax.bar(index + 2*bar_width, all_16pilots, bar_width,
                alpha=opacity, color='g',
                yerr=all_16pilots_stdev,
                label='16 pilots')

ax.set_xlabel('Resource requirements')
ax.set_ylabel('Makespan (s)')
ax.set_xticks(index + bar_width / 2)
ax.set_xticklabels(('1 dedicated', '2 dedicated', '3 dedicated'))
ax.legend()

fig.tight_layout()
plt.savefig('figure1.pdf')


fig, ax = plt.subplots()

print(batch_2d_values)
import math
std = math.sqrt(sum([math.pow(elem-(sum(pilot8_2d_values)/len(pilot8_2d_values)), 2) for elem in pilot8_2d_values])/len(pilot8_2d_values))
print(std, pilot8_2d_values, all_8pilots_stdev)

rects1 = ax.bar(index, all_8batch, bar_width,
                alpha=opacity, color='b',
                yerr=all_8batch_stdev, label='batch')

rects3 = ax.bar(index + bar_width, all_8pilots, bar_width,
                alpha=opacity, color='r',
                yerr=all_8pilots_stdev,
                label='8 pilots')

ax.set_xlabel('Resource requirements')
ax.set_ylabel('Makespan (s)')
ax.set_xticks(index + bar_width / 2)
ax.set_xticklabels(('1 dedicated', '2 dedicated', '3 dedicated'))
ax.legend()
fig.tight_layout()
plt.savefig('figure2.pdf')

fig, ax = plt.subplots()

rects1 = ax.bar(index, all_16batch, bar_width,
                alpha=opacity, color='b',
                yerr=all_16batch_stdev, label='batch')

rects3 = ax.bar(index + bar_width, all_16pilots, bar_width,
                alpha=opacity, color='g',
                yerr=all_16pilots_stdev,
                label='16 pilots')

ax.set_xlabel('Resource requirements')
ax.set_ylabel('Makespan (s)')
ax.set_xticks(index + bar_width / 2)
ax.set_xticklabels(('1 dedicated', '2 dedicated', '3 dedicated'))
ax.legend()
fig.tight_layout()
plt.savefig('figure3.pdf')
