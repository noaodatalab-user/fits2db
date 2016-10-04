#///////////////////////////////////////////////////////////////////////////////
#//
#//  Makefile for the Data Lab Package Applications
#//
#///////////////////////////////////////////////////////////////////////////////

# primary dependencies

NAME       	= fits2db
VERSION    	= 1.0
PLATFORM        := $(shell uname -s)
PLMACH          := $(shell uname -m)
HERE            := $(shell /bin/pwd)
BINDIR          := ./
LIBDIR          := ./
INCDIR          := ./


ifeq ($(PLATFORM),Darwin)
    ifeq  ($(PLMACH),x86_64)
        CARCH   = 
    else
        CARCH   = 
    endif
else
    CLIBS       = -lm -lpthread
    CARCH       = 
    LIBCURL	=
endif
        
DEPLIBS         = -lcfitsio -lpthread -lm
CLIBS           = -lm -lc
CFLAGS          = -g -Wall $(CARCH) -D$(PLATFORM) $(CINCS)
LIBCFITSIO	= -lcfitsio


# includes, flags and libraries
CC 	    = gcc
CINCS  	    = -I$(HERE) -I./ -I../include -I/usr/include/cfitsio -I/usr/local/include

# list of source and include files

C_SRCS 	    = fits2db.c
C_OBJS 	    = fits2db.o
C_INCS 	    =

C_TASKS	    = fits2db

TARGETS	    = $(C_TASKS)

SRCS	    = $(C_SRCS)
OBJS	    = $(C_OBJS)
HOST_LIBS   = $(LIBCFITSIO) -lpthread -lm
LIBS        = -L/usr/local/lib $(HOST_LIBS)


all: 
	make fits2db

World:

install: all

objs:   $(OBJS)


# Targets

c_progs:    $(C_TASKS)
spp_progs:  $(SPP_TASKS)
f77_progs:  $(F77_TASKS)

distclean:
	make clean
	/bin/rm -rf *.fits *.xml

clean:
	/bin/rm -rf .make.state .nse_depinfo *.[aeo] *.dSYM
	/bin/rm -rf $(TARGETS)

everything:
	make clean
	make all
	make install

help: HELP

install: all 


####################################
#  LIBDLAPPS dependency libraries.
####################################

lib:
	(cd lib ; make all)

lib$(NAME).a:
	(cd lib ; make all)


###########################
#  C Test programs
###########################

fits2db: fits2db.o
	$(CC) $(CFLAGS) -o fits2db fits2db.o $(LIBS)
	/bin/rm -rf fits2db.dSYM



###############################################################################
# Leave this stuff alone.
###############################################################################

$(STATICLIB): $(C_SRCS:%.c=Static/%.o)
	/usr/bin/ar rv $@ $?
Static/%.o: %.c $(C_INCS)
	/usr/bin/gcc $(CINCS) $(CFLAGS) -g -c $< -o $@
Static:
	/bin/mkdir $@
	chmod 777 $@

$(SHAREDLIB): $(C_SRCS:%.c=Shared/%.o)
	/usr/bin/ld -shared -o $@ $? -lc -ldl
Shared/%.o: %.c $(C_INCS)
	/usr/bin/gcc $(CINCS) $(CFLAGS) -fpic -shared -c $< -o $@
Shared:
	/bin/mkdir $@
	chmod 777 $@
