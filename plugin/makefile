# we can set this in the environment or hard code it here
#QA_MYSQL_HOME := /home/prabinb/Downloads/source/mysql-5.6.10/

# we use the embedded server so we may need to build it from scratch
QA_MYSQL_TEST := /usr/src/mysql/mysql-5.6.10/
inc      := $(QA_MYSQL_TEST)/sql $(QA_MYSQL_TEST)/include $(QA_MYSQL_TEST)/regex /usr/src/mysql/mysql-5.6.10/include/
lib      := /usr/src/mysql/mysql-5.6.10/libmysqld
inc_params=$(foreach d, $(inc), -I$d)

CC       := g++
CPPFLAGS := -g $(inc_params) -D_THREAD_SAFE -D_REENTRANT  -Dblackhole_EXPORTS -DHAVE_CONFIG_H -DMYSQL_DYNAMIC_PLUGIN -fno-rtti  -fPIC
#CPPFLAGS := -g $(inc_params) -DMYSQL_DYNAMIC_PLUGIN -fPIC
#CXX_DEFINES = -Dblackhole_EXPORTS -DHAVE_CONFIG_H -DMYSQL_DYNAMIC_PLUGIN
CFLAGS   := -g -W -Wall 
LDFLAGS  := -static-libgcc
# You can change -lmysqld to -lmysqlclient to use the
# client/server library
LDLIBS    = -L$(lib) -lmysqld -lz -lm -ldl -lcrypt -lstdc++ -lrt 

ifneq (,$(shell grep FreeBSD /COPYRIGHT 2>/dev/null))
# FreeBSD
LDFLAGS += -pthread
else
# Assume Linux
LDLIBS += -lpthread
LDFLAGS += -shared
endif

# This works for simple one-file test programs
sources := $(wildcard *.cc)
objects := $(patsubst %c,%o,$(sources))
targets := $(basename $(sources))

#ha_blackhole.so : ha_blackhole.o  
#	$(CPP) $(LDFLAGS) ha_blackhole.o $(LDLIBS) -o $@
#ha_blackhole.o: ha_blackhole.cc
#	$(CPP) $(CFLAGS) $(CPPFLAGS) ha_blackhole.cc -c -o $@ $<

#all:
	#g++ -shared $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) $(LDLIBS) -o ha_blackhole.so -fPIC ha_blackhole.cc
all: $(targets)

clean:
	rm -f $(targets) $(objects) *.core *.so
