# we use the embedded server so we may need to build it from scratch
QA_MYSQL_TEST := /usr/src/mysql/mysql-5.6.10/
inc      := $(QA_MYSQL_TEST)/sql $(QA_MYSQL_TEST)/include $(QA_MYSQL_TEST)/regex /usr/src/mysql/mysql-5.6.10/include/
lib      := /usr/src/mysql/mysql-5.6.10/libmysqld
inc_params=$(foreach d, $(inc), -I$d)

CC       := g++
CPPFLAGS := -g $(inc_params) -D_THREAD_SAFE -D_REENTRANT  -Dblackhole_EXPORTS -DHAVE_CONFIG_H -DMYSQL_DYNAMIC_PLUGIN -fno-rtti  -fPIC
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

all: $(targets)

clean:
	rm -f $(targets) $(objects) *.core *.so
