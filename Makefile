all: compile escript

compile:
	./rebar compile skip_deps=true

clean:
	./rebar clean skip_deps=true

test:
	./rebar eunit skip_deps=true

escript: compile
	./rebar escriptize escript_name=deltazip skip_deps=true

.PHONY: all compile clean test
