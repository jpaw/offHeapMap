TARGET=libjpawMap.so
CC=gcc
DEBUG_OR_OPT=-g
CFLAGS=-c -Wall -fPIC -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux $(DEBUG_OR_OPT)
LDFLAGS=-fPIC -shared $(DEBUG_OR_OPT)

SRCDIR=src/main/c
OBJDIR=target/o
TARGETDIR=target/lib
TARGETLIB=$(TARGETDIR)/$(TARGET)
OBJECTS=$(OBJDIR)/jpawMap.o

all: $(TARGETLIB)

$(TARGETLIB): $(OBJECTS)
	mkdir -p $(TARGETDIR)
	$(CC) $(LDFLAGS) $(OBJECTS) -o $(TARGETLIB)
	cp $(TARGETLIB) $(HOME)/lib

$(OBJDIR)/jpawMap.o: $(SRCDIR)/jpawMap.c $(SRCDIR)/jpawMap.h
	mkdir -p $(OBJDIR)
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -rf $(OBJDIR) $(TARGETDIR)
