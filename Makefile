CXX ?= g++
CXXFLAGS ?= -O2 -Wall -Wextra -pthread

all: ft_client ft_server

ft_client: src/ft_client.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

ft_server: src/ft_server.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -f ft_client ft_server

.PHONY: all clean

