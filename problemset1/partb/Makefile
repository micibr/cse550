
CC = gcc
CFLAGS = -Wall -g -std=c17
EXE = 550amtedserver
 
all: $(EXE)
 
$(EXE): 550amtedserver.c
	$(CC) $(CFLAGS) -o $@ $< -lpthread
 
clean:
	rm -f $(EXE)