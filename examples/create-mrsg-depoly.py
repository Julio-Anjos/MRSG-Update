#!/usr/bin/python

from xml.sax.handler import ContentHandler
from xml.sax import make_parser
import sys
import string

class PlatformHandler(ContentHandler):
	def __init__(self, outFileName):
		self.master = True
		self.outFileName = outFileName
	def startDocument(self):
		self.output = open(self.outFileName,'w')
		self.output.write('<?xml version=\'1.0\'?>\n')
		self.output.write('<!DOCTYPE platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid.dtd">\n')
		self.output.write('<platform version="3">\n')
	def startElement(self, name, attrs):
		if name == 'host':
			self.printToFile(attrs.get('id'))
		elif name == 'cluster':
			prefix = attrs.get('prefix')
			radical = attrs.get('radical').split('-')
			suffix = attrs.get('suffix')
			for i in range(int(radical[0]),int(radical[1])):
				self.printToFile(prefix + str(i) + suffix)
	def printToFile(self,hostID):
		if self.master:
			self.output.write('\t<process host="' + hostID + '" function="master_mrsg"/>\n')
			self.master = False
		else:
			self.output.write('\t<process host="' + hostID + '" function="worker_mrsg"/>\n')
	def endDocument(self):
		self.output.write('</platform>\n')
		self.output.close()

if len(sys.argv) < 2:
	print 'Usage:', sys.argv[0], 'platform_file.xml'
	sys.exit(1)

inFileName = sys.argv[1]
outFileName = 'd-' + inFileName

plat = PlatformHandler(outFileName)
saxparser = make_parser()
saxparser.setContentHandler(plat)
saxparser.setFeature('http://xml.org/sax/features/external-general-entities',False)

datasource = open(inFileName,'r')
saxparser.parse(datasource)
