#!/bin/sh
CC = g++ -std=c++11
CFLAGS = -Wall -g3 
#-O3

INSTALL_PATH = $$HOME/simgrid
INCLUDES = -Iinclude -I$(INSTALL_PATH)/include
DEFS = -L$(INSTALL_PATH)/lib
LDADD = -lm -lsimgrid

BIN = libmrsg.a
OBJ_C = dfs_mrsg.o master_mrsg.o worker_mrsg.o user_mrsg.o
OBJ_CPP = common_mrsg.o simcore_mrsg.o task_mrsg.o


all: $(BIN)

$(BIN): $(OBJ_C) $(OBJ_CPP)
	ar rcs $(BIN) $(OBJ_C) $(OBJ_CPP)
#	$(CC) $(INCLUDES) $(DEFS) $(CFLAGS) $(LDADD) -o $@ $^

%.o: src/%.cpp include/*.h include/*.hpp
	$(CC) $(INCLUDES) $(DEFS) $(CFLAGS) -c -o $@ $<


verbose: clean
	$(eval CFLAGS += -DVERBOSE)

debug: clean
	$(eval CFLAGS += -O0)

final: clean
	$(eval CFLAGS += -O2)

check:
	@grep --color=auto -A4 -n -E "/[/*](FIXME|TODO)" include/*.h src/*.c

clean:
	rm -vf $(BIN) *.o *.log *.trace

.SUFFIXES:
.PHONY: all check clean debug final verbose
