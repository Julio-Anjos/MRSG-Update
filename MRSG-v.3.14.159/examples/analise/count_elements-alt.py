#!/usr/bin/python

import sys
import matplotlib.pyplot as plt; plt.rcdefaults()
from matplotlib import rc
rc('font',**{'family':'sans-serif','sans-serif':['Helvetica']})
rc('text', usetex=True)
plt.rcParams.update({'font.size': 12})
import numpy as np
from matplotlib import pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from matplotlib.collections import PolyCollection
from matplotlib.colors import colorConverter

class Task:
    MAP = 0
    REDUCE = 1
    SHUFFLE = 2

    def __init__(self, t, start=0, end=0):
        self._start = start
        self._end = end
        self._type = t

    @property
    def start(self):
        return self._start

    @property
    def end(self):
        return self._end

    @property
    def type(self):
        return self._type

    @start.setter
    def start(self, v):
        self._start = v

    @end.setter
    def end(self, v):
        self._end = v

    @type.setter
    def type(self, v):
        self._type = v

if len(sys.argv) < 3:
    print 'Please provide input and output files'
    exit(-1)

events = []
tasks = {}
finput = open(sys.argv[1], 'r')
# First line seems to be labels
finput.readline()

for line in finput:
    fields = line.strip().split(',')
    taskid = fields[0]
    desc = fields[1]
    time = float(fields[3])
    operation = fields[4]
    if 'START' in operation:
        if '_REDUCE' in desc:
            shuffle = Task(Task.SHUFFLE, start=time)
            tasks[taskid + '_r'] = shuffle
        else:
            task = Task(Task.MAP, start=time)
            tasks[taskid] = task
    elif 'END' in operation:
        if '_REDUCE' in desc:
            shuffle = tasks[taskid + '_r']
            shuffle.end = float(fields[5])
            reduce = Task(Task.REDUCE, start=float(fields[5]), end=time)
            tasks[taskid] = reduce
            events.append(shuffle.end)
        else:
            map = tasks[taskid]
            map.end = time
    events.append(time)

finput.close()

foutput = open(sys.argv[2], 'w')
foutput.write("time,n_maps,n_reduces,n_shuffles\n")

maps = [0]
reduces = [0]
shuffles = [0]
times = [0]
last_time = 0

events.sort()

for time in events:
    map = 0
    reduce = 0
    shuffle = 0
    for task in tasks.values():
        if task.start <= time and task.end >= time:
        # if cmp(task.start, time) <= 0 and cmp(task.end, time) >= 0:
            if task.type == Task.MAP:
                map += 1
            elif task.type == Task.REDUCE:
                reduce += 1
            else:
                shuffle += 1

    foutput.write( '%.5f,%d,%d,%d\n' % (time, map, reduce, shuffle) )

    times.append(time)
    maps.append(map)
    reduces.append(reduce)
    shuffles.append(shuffle)
    last_time = time

foutput.close()

times.append(last_time + 0.01)
maps.append(0)
reduces.append(0)
shuffles.append(0)

# fig, ax = plt.subplots()
# ax.plot(np.array(times), maps, 'r-', alpha=0.5, linewidth=2)
# ax.plot(np.array(times), reduces, 'b-', alpha=0.5, linewidth=2)
# ax.plot(np.array(times), shuffles, 'g-', alpha=0.5, linewidth=2)

# ax.legend(['maps', 'reduces', 'shuffles'])

# ax.set_ylim(0,15)
# ax.set_ylabel("Number of elements")
# ax.set_xlabel("Time")

# code required to create a 3d, filled line graph
fig = plt.figure()
ax = fig.gca(projection='3d')

cc = lambda arg: colorConverter.to_rgba(arg)

verts = []
zs = [0.1, 0.2, 0.3]

verts.append(list(zip(times, maps)))
verts.append(list(zip(times, reduces)))
verts.append(list(zip(times, shuffles)))

poly = PolyCollection(verts, offsets=None, facecolors = [ cc('r'), cc('#48d1cc'), cc('#f0ffff')], closed=False)

poly.set_alpha(0.75)
ax.add_collection3d(poly, zs=zs, zdir='y')

ax.invert_zaxis()

ax.view_init(elev=15., azim=280)

ax.set_xlabel('Time')
ax.set_xlim3d(0, last_time)
ax.set_ylim3d(0.1, 0.3)
ax.set_zlabel('\# Tasks')
ax.set_zlim3d(0, 12)

ax.axes.get_yaxis().set_visible(False)
ax.axes.get_yaxis().set_ticks([])

map_proxy = plt.Rectangle((0, 0), 1, 1, fc=cc('r'))
reduce_proxy = plt.Rectangle((0, 0), 1, 1, fc=cc('#48d1cc'))
shuffle_proxy = plt.Rectangle((0, 0), 1, 1, fc=cc('#f0ffff'))
ax.legend([map_proxy, reduce_proxy ,shuffle_proxy,],['map', 'reduce','shuffle'], ncol=3, loc='upper center')

plt.show()
plt.savefig('graph.png')
