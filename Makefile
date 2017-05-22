CC = gcc
CFLAGS = -Wall -g3 
#-O3

INSTALL_PATH = $$HOME/simgrid-3.14.159
INCLUDES = -Iinclude -I$(INSTALL_PATH)/include
DEFS = -L$(INSTALL_PATH)/lib
LDADD = -lm -lsimgrid

BIN = libmrsg.a
OBJ = common_mrsg.o simcore_mrsg.o dfs_mrsg.o master_mrsg.o worker_mrsg.o user_mrsg.o

all: $(BIN)

$(BIN): $(OBJ)
	ar rcs $(BIN) $(OBJ)
#	$(CC) $(INCLUDES) $(DEFS) $(CFLAGS) $(LDADD) -o $@ $^

%.o: src/%.c include/*.h
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
