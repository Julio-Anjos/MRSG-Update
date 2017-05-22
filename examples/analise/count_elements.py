#!/usr/bin/python

import sys

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
            reduce = Task(Task.REDUCE, time)
            tasks[taskid] = reduce
            shuffle = Task(Task.SHUFFLE, time)
            tasks[taskid + '_r'] = shuffle
        else:
            task = Task(Task.MAP, time)
            tasks[taskid] = task
        events.append(time)
    elif 'END' in operation:
        if '_REDUCE' in desc:
            reduce = tasks[taskid]
            reduce.end = time
            shuffle = tasks[taskid + '_r']
            shuffle.end = float(fields[5])
            events.append(shuffle.end)
        else:
            map = tasks[taskid]
            map.end = time
        events.append(time)

finput.close()

foutput = open(sys.argv[2], 'w')
foutput.write("time,n_maps,n_reduces,n_shuffles\n")

for time in events:
    map = 0
    reduce = 0
    shuffle = 0
    for task in tasks.values():
        if time >= task.start and time <= task.end:
            if task.type == Task.MAP:
                map += 1
            elif task.type == Task.REDUCE:
                reduce += 1
            else:
                shuffle += 1

    foutput.write( '%.2f,%d,%d,%d\n' % (time, map, reduce, shuffle) )
foutput.close()
