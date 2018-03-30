#! /usr/bin/python

import sys
import datetime
from datetime import timedelta


fileName = sys.argv[1]
tag = sys.argv[2]
if len(sys.argv) < 3:
    print
    'Please provide input and output files'
    exit(-1)

end = {}
start = {}
availab = {}
host=[]
time_order = {}
num_host = 30

finput = open(sys.argv[1], 'r')
foutput = open(sys.argv[2], 'w')
finput.readline()

for line in finput:
    fields = line.strip().split('\t')
    node_id = int(fields[2])
    if int(fields[2]) not in host:
        host.append(node_id)
        num = len(host)
        time_order = float(fields[6])
        if num > num_host:
            break
    node_id = num
    event_type = int(fields[5])
    start = (float(fields[6]) - time_order)
    end = (float(fields[7]) - time_order)
    newtime = end - start
    events = (start,end,event_type)
    node = (node_id,events)
    print node

    foutput.write( '%d,%d,%f,%f\n' % (node_id,event_type,start,end) )





finput.close()
foutput.close()

#foutput = open(sys.argv[2], 'w')
#foutput.write("node_id,event_type,start,end\n")
#
#for time in events:
#    node_id = 0
#    event_type = 0
#    start = 0
#    end = 0
#    for task in tasks.values():
#        if time >= task.start and time <= task.end:
#            if task.type == Task.MAP:
#                map += 1
#            elif task.type == Task.REDUCE:
#                reduce += 1
#            else:
#                shuffle += 1
#
#    foutput.write( '%.2f,%d,%d,%d\n' % (time, map, reduce, shuffle) )
#foutput.close()


#teste tempo 
#import datetime
#from datetime import timedelta
#
#
#value1 = datetime.datetime.fromtimestamp(timestamp_in)
#value2 = datetime.datetime.fromtimestamp(timestamp_out)
#newtime = value2 - value1
#print(value1.strftime('%Y-%m-%d %H:%M:%S'))
#print(value2.strftime('%Y-%m-%d %H:%M:%S'))
#print 'Newtime', newtime
#print timedelta.total_seconds(newtime)
#


