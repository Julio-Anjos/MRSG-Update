#!/bin/bash

#making 
cd ..
make clean all
cd examples/
make clean all
./hello_mrsg.bin $1 $2 $3

#clearing files
rm -f *.bin *.plist
cd ..
rm -f  libmrsg.a *.o 

