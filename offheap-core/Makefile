TARGET=libjpawMap.so
CC=gcc
DEBUG_OR_OPT=-O3 -m64 -std=c99 -Wall -Wshadow -Wpointer-arith -Wcast-qual -Wstrict-prototypes -Wmissing-prototypes
CFLAGS=-c -Wall -fPIC -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux $(DEBUG_OR_OPT)
LDFLAGS=-fPIC -shared $(DEBUG_OR_OPT) -L$(HOME)/lib -llz4

SRCDIR=src/main/c
OBJDIR=target/o
TARGETDIR=target/lib
TARGETLIB=$(TARGETDIR)/$(TARGET)
OBJECTS=$(OBJDIR)/jpawMap.o $(OBJDIR)/jpawTransaction.o
INCLUDES=$(SRCDIR)/jpawMap.h $(SRCDIR)/jpawTransaction.h $(SRCDIR)/globalMethods.h

all: $(TARGETLIB)

$(TARGETLIB): $(OBJECTS)
	mkdir -p $(TARGETDIR)
	$(CC) $(LDFLAGS) $(OBJECTS) -o $(TARGETLIB)
	cp $(TARGETLIB) $(HOME)/lib

$(OBJDIR)/jpawMap.o: $(SRCDIR)/jpawMap.c $(INCLUDES)
	mkdir -p $(OBJDIR)
	$(CC) $(CFLAGS) $< -o $@

$(OBJDIR)/jpawTransaction.o: $(SRCDIR)/jpawTransaction.c $(INCLUDES)
	mkdir -p $(OBJDIR)
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -rf $(OBJDIR) $(TARGETDIR)
