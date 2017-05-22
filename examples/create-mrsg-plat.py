#!/usr/bin/python

import sys
import string
import random

if len(sys.argv) < 7:
	print 'Usage:', sys.argv[0], 'platform_file.xml num_workers cores_per_node_min[:numCores_max] cpu_min[:cpu_max] latency_min[:latency_max] bw_min[:bw_max]'
	print 'Ex. Homogeneous :', sys.argv[0], 'plat.xml 5 2 1e9 1e-4 1.25e8'
	print 'Or Heterogeneous:', sys.argv[0], 'plat-cpu_var.xml 10 2 4e9:7e9 1e-4 1.25e8'
	print 'Or Heterogeneous:', sys.argv[0], 'plat-BW_var.xml 10 2 7e9 1e-4 1.25e6:1.25e8'
	print 'Or Heterogeneous:', sys.argv[0], 'plat-Lat_var.xml 10 2 7e9 1e-4:1e-2 1.25e8'
	print 'Or Heterogeneous:', sys.argv[0], 'plat-net_var.xml 10 2 7e9 1e-4:1e-2 1.25e6:1.25e8'
	print 'Or Heterogeneous:', sys.argv[0], 'plat-ALL_var.xml 10 2 4e9:7e9 1e-4:1e-2 1.25e6:1.25e8'
# speed (cpu_min):the peak number FLOPS the CPU can manage. Expressed in flop/s.
# core: The number of core of this host (by default, 1). If you specify the amount of cores, the speed parameter is the speed of each core.
#	print 'Distribution_name',, sys.argv[0], 'uniform, beta, expo, gamma, gauss, logn, weibull'
	sys.exit(1)

# Command line arguments.
outFileName = sys.argv[1]
numNodes = int(sys.argv[2]) + 1
numCores = string.split(sys.argv[3], ':')
for i in range(len(numCores)):
	numCores[i] = int(numCores[i])
cpu = string.split(sys.argv[4], ':')
for i in range(len(cpu)):
	cpu[i] = float(cpu[i])
latency = string.split(sys.argv[5], ':')
for i in range(len(latency)):
	latency[i] = float(latency[i])
bandwidth = string.split(sys.argv[6], ':')
for i in range(len(bandwidth)):
	bandwidth[i] = float(bandwidth[i])


# Header
output = open(outFileName, 'w')
output.write('<?xml version=\'1.0\'?>\n')
output.write('<!DOCTYPE platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd">\n')
output.write('<platform version="4">\n')
output.write('  <AS id="AS1" routing="Full">\n')

random.seed()

#Storage definition
diskSize = "500GB" #sys.argv[7]
bWrite = "60MBps" #sys.argv[8]
bRead = "200MBps" #sys.argv[9]
bConnection = "220MBps" #sys.argv[10]
output.write('\t<storage_type id="single_HDD" model="linear_no_lat" size="'+str(diskSize)+'" content_type="txt_unix">\n')
output.write('\t\t<model_prop id="Bwrite" value="'+str(bWrite)+'" />\n')
output.write('\t\t<model_prop id="Bread" value="'+str(bRead)+'" />\n')
output.write('\t\t<model_prop id="Bconnection" value="'+str(bConnection)+'" />\n')
output.write('\t</storage_type>\n')

for i in range(numNodes):
	output.write('\t<storage id="Disk'+ str(i) +'" typeId="single_HDD"' +
	' content_type="txt_unix" attach="MRSG_Host'+str(i)+'"/>\n')
# Nodes definition.
output.write('\n')
if len(cpu) == 1 and len(numCores) == 1:
	for i in range(numNodes):
		output.write('\t<host id="MRSG_Host' + str(i) + '" speed="' + str(cpu[0]) + 'f" core="' + str(numCores[0]) + '">\n')
 		if i==0:
			for j in range(numNodes):
				output.write('\t\t<mount storageId="Disk'+str(j)+'" name="/home"/>\n')
		else:
			output.write('\t\t<mount storageId="Disk'+str(i)+'" name="/home"/>\n')
		output.write('\t</host>\n')

elif len(numCores) == 1:
	for i in range(numNodes):
		rCPU = random.uniform(cpu[0], cpu[1])
		output.write('\t<host id="MRSG_Host' + str(i) + '" speed="' + str(rCPU) + 'f" core="' + str(numCores[0]) + '" />\n')

elif len(cpu) == 1:
	for i in range(numNodes):
		rCor = random.randrange(numCores[0], numCores[1],2)
		output.write('\t<host id="MRSG_Host' + str(i) + '" speed="' + str(cpu[0]) + 'f" core="' + str(rCor) + '" />\n')


else:
	for i in range(numNodes):
		rCPU = random.uniform(cpu[0], cpu[1])
		rCor = random.randrange(numCores[0], numCores[1], 2)
		output.write('\t<host id="MRSG_Host' + str(i) + '" speed="' + str(rCPU) + 'f" core="' + str(rCor) + '" />\n')




# Links definition.
output.write('\n')
if len(bandwidth) == 1:
	for i in range(1,numNodes):
		 output.write('\t<link id="l' + str(i) + '" bandwidth="' + str(bandwidth[0]) + 'Bps" latency="' + str(latency[0]) + 's" />\n')

elif len(latency) ==1:
	for i in range(1,numNodes):
		rBW = random.uniform (bandwidth[0], bandwidth[1])
		output.write('\t<link id="l' + str(i) + '" bandwidth="' + str(rBW) + 'Bps" latency="' + str(latency[0]) + 's" />\n')


elif (len(bandwidth) == 1 and len(latency) ==1):
	for i in range(1,numNodes):
		output.write('\t<link id="l' + str(i) + '" bandwidth="' + str(bandwidth[0]) + 'Bps" latency="' + str(latency[0]) + 's" />\n')

else:
	for i in range(1,numNodes):
		rBW = random.uniform (bandwidth[0], bandwidth[1])
		rLat = random.uniform (latency[0], latency[1])
		output.write('\t<link id="l' + str(i) + '" bandwidth="' + str(rBW) + 'Bps" latency="' + str(rLat) + 's" />\n')
#if else:
#	for i in range(1,numNodes):
#		rBW = random.uniform (bandwidth[0], bandwidth[1])
#		output.write('\t<link id="l' + str(i) + '" bandwidth="' + str(rBW) + '" latency="' + latency + '" />\n')

# Topology (paths) definition.
output.write('\n')
for src in range(numNodes):
	for dst in range(src+1,numNodes):
		if src != dst:
			output.write('\t<route src="MRSG_Host' + str(src) + '" dst="MRSG_Host' + str(dst) + '">\n')
			if (src == 0):
				output.write('\t\t<link_ctn id="l' + str(dst) + '"/>\n')
			elif (dst == 0):
				output.write('\t\t<link_ctn id="l' + str(src) + '"/>\n')
			else:
				output.write('\t\t<link_ctn id="l' + str(src) + '"/>\n')
				output.write('\t\t<link_ctn id="l' + str(dst) + '"/>\n')
			output.write('\t</route>\n')

# Footer
output.write('\n  </AS>\n')
output.write('</platform>\n')
output.close()
