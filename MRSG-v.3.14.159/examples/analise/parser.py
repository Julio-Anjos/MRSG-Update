f = open('tasks-mrsg.csv', 'r')
f1 = open('tasks-mrsg.csv', 'r')
for line in f1:
		fields = line.strip().split(',')

		
#f2 = open('file2.txt', 'w')

for line in f:
   fields = line.strip().split(',')
  

   if 'test' in fields[0]:
     print 'found id'

   print fields
   
	 

   # var = float(fields[1])
   #print fields[0], fields[1]

f.close()
#f2.close()
 #  f2.write(fields[0] + '\n')
