CC=g++
CXX=g++
RANLIB=ranlib

LIBSRC=MapReduceFramework.cpp Barrier.cpp
LIBOBJ=MapReduceFramework.o Barrier.o

INCS=-I.
CFLAGS = -Wall -std=c++11 -g -pthread $(INCS)
CXXFLAGS = -Wall -std=c++11 -g -pthread $(INCS)

LIBMAPREDUCE = libMapReduceFramework.a
TARGETS = $(LIBMAPREDUCE)

TAR=tar
TARFLAGS=-cvf
TARNAME=ex3.tar
TARSRCS=$(LIBSRC) Makefile README Barrier.h

all: $(TARGETS)

$(TARGETS): $(LIBOBJ)
	$(AR) $(ARFLAGS) $@ $^
	$(RANLIB) $@

clean:
	$(RM) $(TARGETS) $(LIBUTHREADS) $(OBJ) $(LIBOBJ) *~ *core

depend:
	makedepend -- $(CFLAGS) -- $(SRC) $(LIBSRC)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)
