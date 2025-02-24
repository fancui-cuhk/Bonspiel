CPPC=/usr/bin/g++
CC=/usr/bin/gcc
STD=-std=c++20
CFLAGS=-Wall -g

.SUFFIXES: .o .cpp .h

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/ ./transport/ ./utils/ ./raft/src/
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system -I./transport -I./utils -I./raft/include

CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Wall -O0
LDFLAGS = -Wall -L./libs -pthread -lrt -O0 -ljemalloc -lnuma 

#CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Wall -O2 -g -ggdb -fno-omit-frame-pointer
#LDFLAGS = -Wall -L./libs -pthread -lrt -O2 -ljemalloc -lnuma -fno-omit-frame-pointer

#CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Wall -O0 -g -ggdb -fsanitize=address -fno-omit-frame-pointer
#LDFLAGS = -Wall -L./libs -pthread -lrt -O0 -ljemalloc -lnuma -fsanitize=address -static-libasan -fno-omit-frame-pointer
LDFLAGS += $(CFLAGS)

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
CS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.c))
OBJS = $(CS:.c=.o) $(CPPS:.cpp=.o)
DEPS = $(CS:.c=.d) $(CPPS:.cpp=.d)

all:rundb

rundb : $(OBJS)
	$(CPPC) -o $@ $^ $(LDFLAGS)

-include $(OBJS:%.o=%.d)

%.d: %.cpp
	$(CPPC) -MM -MT $*.o -MF $@ $(CFLAGS) $(STD) $<

%.d: %.c
	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp %.d
	$(CPPC) -c $(CFLAGS) $(STD) -o $@ $<

%.o: %.c %.d
	$(CC) -c $(CFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f rundb *.o */*.o *.d */*.d */*/*.o */*/*.d
