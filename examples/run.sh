#!/bin/bash
export LD_LIBRARY_PATH=$HOME/simgrid-3.14.159/lib
cd ..
make clean all
cd examples/
make clean all
#python create-mrsg-plat.py yh.xml 2000 2 5e9 1e-4 1.25e8
#python create-mrsg-plat.py fb.xml 3000 2 5e9 1e-4 1.25e8
#python create-mrsg-depoly.py yh.xml
#python create-mrsg-depoly.py fb.xml
#./hello_mrsg.bin --cfg=tracing:no  cc.xml d-cc.xml cc.conf 2>&1| $HOME/simgrid-3.14.159/bin/simgrid-colorizer  > cc.txt
#./hello_mrsg.bin --cfg=tracing:no  yh.xml d-yh.xml yh_206.conf 2>&1| $HOME/simgrid-3.14.159/bin/colorize > yh_206.txt
#./hello_mrsg.bin --cfg=tracing:no  yh.xml d-yh.xml yh_568.conf  2>&1| $HOME/simgrid-3.14.159/bin/colorize > yh_568.txt
#./hello_mrsg.bin --cfg=tracing:no  fb.xml d-fb.xml fb.conf  2>&1| $HOME/simgrid-3.14.159/bin/colorize > fb.txt
./hello_mrsg.bin --cfg=tracing:no  teste.xml d-teste.xml teste.conf  2>&1| $HOME/simgrid-3.14.159/bin/colorize
