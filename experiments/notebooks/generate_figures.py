#!/usr/bin/env python3
import json
from statistics import mean
from matplotlib import pyplot as plt
import numpy as np
import sys


def load_json(fn):
    with open(fn, 'r') as f:
        return json.load(f)


batch = None #load_json(sys.argv[1])
pilots8 = None #load_json(sys.argv[2])
pilots16 = None #load_json(sys.argv[3])

system = None #sys.argv[4]

def makespan_dict():
    return { 'batch': [], '8pilots': [], '16pilots': [] }


def all_succeeded(i):
    return (batch[i]['success']
            and pilots8[i]['success']
            and pilots16[i]['success'])


dedicated_1 = makespan_dict()
dedicated_2 = makespan_dict()
dedicated_3 = makespan_dict()
dedicated_4 = makespan_dict()

get_makespan = lambda x: (x['end_time'] - x['start_time']
                          if x['success'] and x['end_time'] is not None
                          else 0)
is_same = lambda x, i: x in pilots8[i]['name'] and x in pilots16[i]['name']


def fill_dictionaries():
    current_dict = None
    for i in range(len(batch)):
        if 'single' in batch[i]['name']:
            assert(is_same('1d', i))
            current_dict = dedicated_1
        elif 'double' in batch[i]['name']:
            assert(is_same('2d', i))
            current_dict = dedicated_2
        elif 'triple' in batch[i]['name']:
            assert(is_same('3d', i))
            current_dict = dedicated_3
        elif 'quadruple' in batch[i]['name']:
            assert(is_same('4d', i))
            current_dict = dedicated_4

        current_dict['batch'].append(get_makespan(batch[i]))
        current_dict['8pilots'].append(get_makespan(pilots8[i]))
        current_dict['16pilots'].append(get_makespan(pilots16[i]))


def iteration_fig(data, n_dedicated, system="Beluga", save=None):
    n_groups = len(data['batch'])

    fig, ax = plt.subplots()

    index = np.arange(n_groups)
    bar_width = 0.30
    opacity = 0.4

    rects1 = ax.bar(index, data['batch'], bar_width, alpha=opacity,
                    color='r', label='batch')

    rects2 = ax.bar(index + bar_width, data['8pilots'], bar_width,
                    alpha=opacity, color='b', label='8 pilots')

    rects3 = ax.bar(index + 2*bar_width, data['16pilots'], bar_width,
                    alpha=opacity, color='green', label='16 pilots')


    ax.set_xlabel('Iteration #')
    ax.set_ylabel('Makespan (s)')
    ax.set_xticks(index + bar_width / 2)
    ax.set_xticklabels((i + 1 for i in range(n_groups)))
    ax.legend()
    
    if save is None:
        ax.set_title('{0} - {1} dedicated'.format(system, n_dedicated))
        plt.show()
    else:
        plt.savefig(save)


def get_makespan_diff(d, num_pilots):
    b = d['batch']
    p = d['{}pilots'.format(num_pilots)]
    data = [b[i] - p[i] for i in range(len(b)) if b[i] != 0 and p[i] != 0]

    return data


def makespan_box(d1, d2, d3, d4, num_pilots, system="Beluga", save=None):
    fig, ax = plt.subplots()
    data = [get_makespan_diff(d1, num_pilots),
            get_makespan_diff(d2, num_pilots),
            get_makespan_diff(d3, num_pilots),
            get_makespan_diff(d4, num_pilots)]

    pos = np.array(range(len(data))) + 1
    bp = ax.boxplot(data, sym='k+', positions=pos)

    ax.set_xlabel('# dedicated')
    ax.set_ylabel('Makespan (s)')
    plt.setp(bp['whiskers'], color='k', linestyle='-')
    plt.setp(bp['fliers'], markersize=3.0)

    if save is None:
        ax.set_title('{0} - {1} pilots'.format(system, num_pilots))
        plt.show()
    else:
        plt.savefig(save)


def queuing_time(bd, pd):
    return [bd[i]['sid'][0]['start_time'] -
            min([p['start_time'] for p in pd[i]['sid']
                 if p['start_time'] is not None])
            for i in range(len(bd))
            if bd[i]['success'] and bd[i]['end_time'] is not None
            and pd[i]['success'] and pd[i]['end_time'] is not None]


def makespan_all(bd, pd):
    return [(bd[i]['end_time'] - bd[i]['start_time']) - 
            (pd[i]['end_time'] - pd[i]['start_time'])
            for i in range(len(bd))
            if bd[i]['success'] and bd[i]['end_time'] is not None
            and pd[i]['success'] and pd[i]['end_time'] is not None]


def makespan_queue(num_pilot):
    pilot_data = None
    if num_pilot == 8:
        pilot_data = pilots8
    else:
        pilot_data = pilots16
    return (queuing_time(batch, pilot_data), makespan_all(batch, pilot_data))


def scatter_fig(x, y, num_pilots, xlabel, ylabel, system="Beluga", save=None):
    fig, ax = plt.subplots()
    ax.scatter(x, y, alpha=0.5)

    b, m = np.polynomial.polynomial.polyfit(x, y, 1)
    ax.plot(np.asarray(x), b + m*np.asarray(x), 'k-')
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)

    if save is None:
        ax.set_title('{0} - {1} pilots'.format(system, num_pilots))
        plt.show()
    else:
        plt.savefig(save)


def add_kv(d, k, v, w=True):

    if len(v) > 0 and type(v[0]) == dict:
        v = get_ld_keys(v)

    if k is not None:
        if k not in d:
            d[k] = set(v)
        else:
            d[k].update(v)


def get_ld_keys(l, w=False):
    temp = []
    if w:
        for el in l:
            temp.extend(["{0}:{1}".format(k, p) for k,v in el.items()
                         for p in v])
    else:
        for el in l:
            temp.extend(el.keys())

    return temp



def get_pilot_info(job, t, w=False):
    if job['end_time'] is None:
        return []

    makespan_dict = {}
    
    makespan_dict[job['start_time']] = set()
    makespan_dict[job['end_time']] = set()

    true_t = None

    # First passage
    for sid in job['sid']:
        if sid['end_time'] is None:
            sid['end_time'] = job['end_time']

        sid[t] = sid[t] if type(sid[t]) == list else [sid[t]]


        add_kv(makespan_dict, sid['start_time'], sid[t], w)
        add_kv(makespan_dict, sid['end_time'], sid[t], w)

    # Second passage
    for sid in job['sid']:
        for k,v in makespan_dict.items():
            if (sid['end_time'] is not None and sid['start_time'] is not None
                and k > sid['start_time'] and k < sid['end_time']):
                if t == 'nodes':
                    v.update(get_ld_keys(sid[t], w))
                else:
                    v.update(sid[t])
    
    return [(k, len(v)) for k, v in sorted(makespan_dict.items())]


pilot_avgtype = lambda x: (sum([(x[i][1]*(x[i+1][0] - x[i][0])) 
                              for i in range(1, len(x) - 1)]) 
                           / (x[-1][0] - x[0][0]))

get_pilot_avgnodes = lambda x,y: ((pilot_avgtype(get_pilot_info(x, 'nodes'))),
                                  ((y['end_time'] - y['start_time'])
                                   - (x['end_time'] - x['start_time'])),
                                   x['end_time'] - x['start_time'])


get_batch_avgnodes = lambda x:(((len(set(x['sid'][0]['nodes'].keys()))
                                 * (x['sid'][0]['end_time']
                                    - x['sid'][0]['start_time']))
                                / (x['end_time'] - x['start_time'])),
                               x['end_time'] - x['start_time'])


get_avgpilots = lambda x,y: ((pilot_avgtype(get_pilot_info(x, 'id'))),
                             ((y['end_time'] - y['start_time'])
                               - (x['end_time'] - x['start_time'])))


def get_ncpus(name):
    if 'single' in name or 'double' in name or 'triple' in name:
        return 16
    elif '8n1d' in name:
        return 2
    elif '16n1d' in name:
        return 1
    elif '8n2d' in name:
        return 4
    elif '16n2d' in name:
        return 2
    elif '8n3d' in name:
        return 6
    elif '16n3d':
        return 3

get_pilot_avgpilots = lambda x,y: ((pilot_avgtype(get_pilot_info(x, 'id'))),
                                  ((y['end_time'] - y['start_time'])
                                    - (x['end_time'] - x['start_time'])),
                                   x['end_time'] - x['start_time'])

get_pilot_avgw = lambda x,y: ((pilot_avgtype(get_pilot_info(x, 'nodes', True))),
                                  ((y['end_time'] - y['start_time'])
                                    - (x['end_time'] - x['start_time'])),
                                   x['end_time'] - x['start_time'])



get_batch_avgpilots = lambda x:(((len(set(x['sid'][0]['nodes']))
                                  * (x['sid'][0]['end_time']
                                     - x['sid'][0]['start_time']))
                                 / (x['end_time'] - x['start_time'])),
                                x['end_time'] - x['start_time'])

get_batch_avgw = lambda x:(((len(set(['{0}:{1}'.format(k, v)
                                      for k,v in x['sid'][0]['nodes'].items()]))
                                 * (x['sid'][0]['end_time']
                                    - x['sid'][0]['start_time']))
                                / (x['end_time'] - x['start_time'])),
                               x['end_time'] - x['start_time'])


def main():
    batch = load_json(sys.argv[1])
    pilots8 = load_json(sys.argv[2])
    pilots16 = load_json(sys.argv[3])

    system = sys.argv[4]


if __name__ == "__main__":
    main()
