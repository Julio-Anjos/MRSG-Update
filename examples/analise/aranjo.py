finput = open('tasks-mra.csv', 'r')
	
#f2 = open('file2.txt', 'w')
end = { }
start = { }
duration = { }
phase = { }
tasks = { }

for line in finput:
   fields = line.strip().split(',')			   
   if 'START' in fields[4]:
			start[fields[0]] = float(fields[3])
   elif 'END' in fields[4]:
   	end[fields[0]] = float(fields[3])
   	duration = float(end[fields[0]]) - float(start[fields[0]])
   	tasks[fields[0]] = duration	

finput.close()
	
foutput = open('tasks.txt', 'w')

for taskid in tasks:

	foutput.write(taskid +','+ str(tasks[taskid]) + '\n' )
	
#	foutput.write( '%.2f,%d,%d,%d\n' % (time, map, reduce, shuffle) )

   # var = float(fields[1]) %d, %.3f,%.3f,
   #print fields[0], fields[1]  phase[fields[0]], start[fields[0]] , end[fields[0]],

foutput.close()

 

