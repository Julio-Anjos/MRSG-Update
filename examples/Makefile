CC = gcc
CFLAGS = -Wall -lpthread -g3 -O3

INSTALL_PATH = $$HOME/simgrid-3.11.1
INCLUDES = -I../include -I$(INSTALL_PATH)/include 
DEFS = -L$(INSTALL_PATH)/lib
LDADD = -lm -lsimgrid

SRC=$(wildcard *.c)
BIN=$(SRC:.c=.bin)

all: $(BIN)

%.bin: %.c
	$(CC) $(INCLUDES) $(DEFS) -o $@ $^ ../libmrsg.a $(LDADD)

clean:
	rm -vf *.bin *.csv *.trace *.plist

.PHONY: clean
