finput = open('tasks-mra.csv', 'r')
duration = { }
tasks = { }
		
#f2 = open('file2.txt', 'w')

for line in finput:
   fields = line.strip().split(',')
   if 'START' in fields[4]:
			tasks[fields[0]] = float(fields[3])
   elif 'END' in fields[4]:
			duration = float(fields[3]) - float(tasks[fields[0]])
			tasks[fields[0]] = duration
finput.close()

foutput = open('tasks-mra.txt', 'w')
for taskid in tasks:
     foutput.write(taskid +','+ str(tasks[taskid]) + '\n' )



   # var = float(fields[1])
   #print fields[0], fields[1]

foutput.close()
#f2.close()
 #  f2.write(fields[0] + '\n')
