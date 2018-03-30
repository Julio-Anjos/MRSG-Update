#!/bin/bash

export LD_LIBRARY_PATH=$HOME/simgrid-3.11.1/lib

./hello_mrsg.bin --cfg=surf/precision:0.0000000000001 2>&1 | $HOME/simgrid-3.11.1/bin/simgrid-colorizer
#./hello_mrsg.bin --cfg=maxmin/precision:0.0000000001 2>&1 | $HOME/simgrid-3.11.1/bin/simgrid-colorizer

