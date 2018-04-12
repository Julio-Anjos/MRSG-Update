#!/bin/bash

cd ..
make clean all
cd examples/
make clean all
./hello_mrsg.bin
