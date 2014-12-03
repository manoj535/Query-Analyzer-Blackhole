# we can set this in the environment or hard code it here
QA_MYSQL_HOME := /usr/local/mysql/
export QA_MYSQL_HOME

# we use the embedded server so we may need to build it from scratch
inc      := $(QA_MYSQL_HOME)/include 
lib      := $(QA_MYSQL_HOME)/lib 

CPP      := g++
CPPFLAGS := -I$(inc) -D_THREAD_SAFE -D_REENTRANT -Wno-write-strings -fpermissive 
CFLAGS   := -g -W -Wall
LDFLAGS  := -static-libgcc 
# You can change -lmysqld to -lmysqlclient to use the
# client/server library
LDLIBS   := -L$(lib) -lmysqld -lz -lm -ldl -lcrypt -lstdc++ -lrt

ifneq (,$(shell grep FreeBSD /COPYRIGHT 2>/dev/null))
# FreeBSD
LDFLAGS += -pthread
else
# Assume Linux
LDLIBS += -lpthread
endif

all: query-analyzer

queryAnalyzer.o: queryAnalyzer.cpp
	$(CPP) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

query-analyzer: queryAnalyzer.o
	cd ./plugin && make -f ./makefile
	$(CPP) $(LDFLAGS) queryAnalyzer.o $(LDLIBS) -o $@

all: $(targets)
clean: 
	rm -f query-analyzer *.o *.core
	cd ./plugin && make clean
