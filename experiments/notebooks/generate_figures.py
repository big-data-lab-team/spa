#!/usr/bin/env python3
import json
from statistics import mean
from matplotlib import pyplot as plt
import matplotlib.lines as mlines
import numpy as np
import sys


def load_json(fn):
    with open(fn, 'r') as f:
        return json.load(f)


batch = None #load_json(sys.argv[1])
pilots8 = None #load_json(sys.argv[2])
pilots16 = None #load_json(sys.argv[3])

system = None #sys.argv[4]

# NOTE: 3600 is 1hour in seconds - essentially, pipeline would not have passed if longer
max_queue = 3600

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

def pilot_queueing_times(s):
    global max_queue
    return ([p['start_time'] - s['start_time']
             if p['start_time'] is not None else max_queue
             for p in s['sid']] 
            if s['success'] and s['end_time'] is not None
            else [0] )

def fill_dictionaries(dedicated_1, dedicated_2, dedicated_3,
                      dedicated_4, queue=False):
    global max_queue
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

        if queue:
            batch_pq = pilot_queueing_times(batch[i])
            pilot8_pq = pilot_queueing_times(pilots8[i])
            pilot16_pq = pilot_queueing_times(pilots16[i])
            current_dict['batch'] \
                .append(mean(batch_pq))
            current_dict['8pilots'] \
                .append(mean(pilot8_pq))
            current_dict['16pilots'] \
                .append(mean(pilot16_pq))
        else:
            current_dict['batch'].append(get_makespan(batch[i]))
            current_dict['8pilots'].append(get_makespan(pilots8[i]))
            current_dict['16pilots'].append(get_makespan(pilots16[i]))


def repetition_fig(data, n_dedicated, system="Beluga", save=None, ylim=None):
    n_groups = len(data['batch'])
    print(n_groups)

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


    ax.set_xlabel('Repetition')
    ax.set_ylabel('Makespan (s)')

    if ylim is not None:
        ax.set_ylim(0, ylim)
    elif system == "beluga":
        ax.set_ylim(0, 15000)
    else:
        ax.set_ylim(0, 15000) # 60000

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


def basic_model(xy1, xy2, xy3, xy4, num_pilots, xlabel, ylabel, system="Beluga", save=None):   
    fig, ax = plt.subplots()
    execution_mode = "Batch" if type(num_pilots) == str and "batch" in num_pilots else "Pilots" 
    ax.scatter(xy1[0], xy1[1], c="#6600cc", alpha=0.5, label="{} 1 dedicated".format(execution_mode))                                                  
    ax.scatter(xy2[0], xy2[1], c="#ff0000", alpha=0.5, label="{} 2 dedicated".format(execution_mode))
    ax.scatter(xy3[0], xy3[1], c="#ffa500", alpha=0.5, label="{} 3 dedicated".format(execution_mode))
    ax.scatter(xy4[0], xy4[1], c="#008000", alpha=0.5, label="{} 4 dedicated".format(execution_mode))
    
    if len(xy1) > 2:
        ax.scatter(xy1[2], xy1[3], c="#290066", marker="v", alpha=0.5, label="Pilots 1 dedicated")
        ax.scatter(xy2[2], xy2[3], c="#800033", marker="v", alpha=0.5, label="Pilots 2 dedicated")
        ax.scatter(xy3[2], xy3[3], c="#ff6600", marker="v", alpha=0.5, label="Pilots 3 dedicated")
        ax.scatter(xy4[2], xy4[3], c="#003300", marker="v", alpha=0.5, label="Pilots 4 dedicated")
    if len(xy1) > 4:
        ax.scatter(xy1[4], xy1[5], c="#290066", marker="+", alpha=0.5, label="Pilots 1 dedicated")
        ax.scatter(xy2[4], xy2[5], c="#800033", marker="+", alpha=0.5, label="Pilots 2 dedicated")
        ax.scatter(xy3[4], xy3[5], c="#ff6600", marker="+", alpha=0.5, label="Pilots 3 dedicated")
        ax.scatter(xy4[4], xy4[5], c="#003300", marker="+", alpha=0.5, label="Pilots 4 dedicated")
        
    lgnd = []
    if len(xy1) > 2:
        pilot_symbol = mlines.Line2D([], [], color="black", alpha=0.5, marker='v', linestyle='None',
                                     label='Pilots')
        lgnd.append(pilot_symbol)
    if len(xy1) > 4:
        pilot8_symbol = mlines.Line2D([], [], color="black", alpha=0.5, marker='v', linestyle='None',
                                     label='8 Pilots')
        pilot16_symbol = mlines.Line2D([], [], color="black", alpha=0.5, marker='+', linestyle='None',
                                     label='16 Pilots')
        lgnd = [ pilot8_symbol, pilot16_symbol ]

    batch_symbol = mlines.Line2D([], [], color="black", alpha=0.5, marker='o', linestyle='None',
                                 label=execution_mode)
    d1_symbol = mlines.Line2D([], [], color="purple", alpha=0.5, linestyle='-',
                              label='Configuration 1')
    d2_symbol = mlines.Line2D([], [], color="red", alpha=0.5, linestyle='-',
                              label='Configuration 2')
    d3_symbol = mlines.Line2D([], [], color="orange", alpha=0.5, linestyle='-',
                              label='Configuration 3')
    d4_symbol = mlines.Line2D([], [], color="green", alpha=0.5, linestyle='-',
                              label='Configuration 4')

    lgnd.insert(0, batch_symbol)
    lgnd.extend([d1_symbol, d2_symbol, d3_symbol, d4_symbol])
    ax.legend(handles=lgnd)
    
    get_c = lambda x, y, z=False: (10*(x+20)*125)/y if z else 10*(x+20)*np.ceil(125/y)
    ceil_results = False
    n_workers = np.arange(5, 70)
    
    a = 0.2
    ax.plot(n_workers, get_c(45, n_workers, ceil_results),'-', c='purple', alpha=a)
    ax.plot(n_workers, get_c(90, n_workers, ceil_results), '-', c='red', alpha=a)
    ax.plot(n_workers, get_c(120, n_workers, ceil_results), '-', c='orange', alpha=a)
    ax.plot(n_workers, get_c(180, n_workers, ceil_results), '-', c='green', alpha=a)
    
    ax.set_xlabel(xlabel)                                                        
    ax.set_ylabel(ylabel)                                                        
    ax.set_ylim(0, 14000)#(0,61000)
                                                                                 
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
        v = get_ld_keys(v, w)

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

        start = (max(sid['start_time'], sid['job_start'])
                 if 'job_start' in sid and sid['job_start'] is not None
                 else sid['start_time'])

        add_kv(makespan_dict, start, sid[t], w)
        add_kv(makespan_dict, sid['end_time'], sid[t], w)

    # Second passage
    for sid in job['sid']:
        start = (max(sid['start_time'], sid['job_start'])
                 if 'job_start' in sid and sid['job_start'] is not None
                 else sid['start_time'])
        for k,v in makespan_dict.items():
            if (sid['end_time'] is not None and start is not None
                and k >= start and k <= sid['end_time']):
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
                                     - (max(x['sid'][0]['start_time'],
                                           x['sid'][0]['job_start'])
                                        if 'job_start' in x['sid'][0]
                                        and x['sid'][0]['job_start'] is not None
                                        else x['sid'][0]['start_time'])))
                                 / (x['end_time'] - x['start_time'])),
                                x['end_time'] - x['start_time'])

get_batch_avgw = lambda x:(((len(set(['{0}:{1}'.format(k, p)
                                      for k,v in x['sid'][0]['nodes'].items()
                                      for p in v]))
                                 * (x['sid'][0]['end_time']
                                    - (max(x['sid'][0]['start_time'],
                                           x['sid'][0]['job_start'])
                                       if 'job_start' in x['sid'][0]
                                       and x['sid'][0]['job_start'] is not None
                                       else x['sid'][0]['start_time'])))
                                / (x['end_time'] - x['start_time'])),
                               x['end_time'] - x['start_time'])

m = lambda x,y : [el['end_time'] - el['start_time']
               for el in x if el['success']
               and y in el['name'] and el['end_time'] is not None]
w_batch = lambda y :[get_batch_avgw(el)[0]
                    for el in batch if el['success']
                    and y in el['name'] and el['end_time'] is not None]
w_pilot = lambda x,y: [get_pilot_avgw(el, el)[0]
                      for el in x
                      if el['success'] and el['end_time'] is not None
                      and y in el['name']]

def get_m_w(mode):
    if mode == 'batch':
        return [(w_batch(n), m(batch, n)) for n in ['single', 'double',
                                                    'triple', 'quadruple']]
    elif mode == '8p':
        return [(w_pilot(pilots8, n), m(pilots8, n)) for n in ['1d', '2d',
                                                                '3d', '4d']]
    else:
        return [(w_pilot(pilots16, n), m(pilots16, n)) for n in ['1d', '2d',
                                                                 '3d', '4d']]



def main():
    global batch, pilots8, pilots16
    batch = load_json(sys.argv[1])
    pilots8 = load_json(sys.argv[2])
    pilots16 = load_json(sys.argv[3])

    system = sys.argv[4]

    assert(len(batch)==len(pilots8)==len(pilots16))

    global dedicated_1, dedicated_2, dedicated_3, dedicated_4

    fill_dictionaries(dedicated_1, dedicated_2, dedicated_3, dedicated_4)

    pq_1 = makespan_dict()
    pq_2 = makespan_dict()
    pq_3 = makespan_dict()
    pq_4 = makespan_dict()

    fill_dictionaries(pq_1, pq_2, pq_3, pq_4, queue=True)

    repetition_fig(pq_1, 1, system=system,
                   save="figures/pq_1_{}".format(system))
    repetition_fig(pq_2, 2, system=system,
                   save="figures/pq_2_{}".format(system))
    repetition_fig(pq_3, 3, system=system,
                   save="figures/pq_3_{}".format(system))
    repetition_fig(pq_4, 4, system=system,
                   save="figures/pq_4_{}".format(system))

    repetition_fig(dedicated_1, 1, system=system,
                  save="figures/dedicated_1_{}.pdf".format(system))
    repetition_fig(dedicated_2, 2, system=system,
                  save="figures/dedicated_2_{}.pdf".format(system))
    repetition_fig(dedicated_3, 3, system=system,
                  save="figures/dedicated_3_{}.pdf".format(system))
    repetition_fig(dedicated_4, 4, system=system,
                  save="figures/dedicated_4_{}.pdf".format(system))

    batchmw_1d, batchmw_2d, batchmw_3d, batchmw_4d = tuple(get_m_w("batch"))
    pilots8mw_1d, pilots8mw_2d, pilots8mw_3d, pilots8mw_4d = tuple(get_m_w("8p"))
    pilots16mw_1d, pilots16mw_2d, pilots16mw_3d, pilots16mw_4d = tuple(get_m_w("16p"))

    print(get_batch_avgw(batch[6])[0], batchmw_4d)

    #data = get_pilot_info(pilots8[1], 'nodes', True)
    #print(pilots8[1]["name"], data)
    #print(sum([data[i][1]*(data[i + 1][0] - data[i][0]) for i in range(len(data) - 1)]) / ( data[-1][0] - data[0][0] ))

    basic_model(batchmw_1d + pilots8mw_1d,
                batchmw_2d + pilots8mw_2d,
                batchmw_3d + pilots8mw_3d,
                batchmw_4d + pilots8mw_4d, "batch - 8", "W", "M",
                system=system, save="figures/mw_8_{}.pdf".format(system))

    basic_model(batchmw_1d + pilots16mw_1d,
                batchmw_2d + pilots16mw_2d,
                batchmw_3d + pilots16mw_3d,
                batchmw_4d + pilots16mw_4d, "batch - 16", "W", "M",
                system=system, save="figures/mw_16_{}.pdf".format(system))

    basic_model(batchmw_1d + pilots8mw_1d + pilots16mw_1d,
                batchmw_2d + pilots8mw_2d + pilots16mw_2d,
                batchmw_3d + pilots8mw_3d + pilots16mw_3d,
                batchmw_4d + pilots8mw_4d + pilots16mw_4d, "batch - 8 -  16",
                "W", "M", system=system,
                save="figures/mw_{}.pdf".format(system))

if __name__ == "__main__":
    main()
