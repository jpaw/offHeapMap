all: libjpawMap.so

libjpawMap.so: jpawMap.h jpawMap.c
	gcc -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -shared -fPIC -o libjpawMap.so jpawMap.c
	cp libjpawMap.so $(HOME)/lib

clean:
	rm -rf *.so