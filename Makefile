all: compile escript

compile:
	./rebar compile skip_deps=true

clean:
	./rebar clean skip_deps=true

test:
	./rebar eunit skip_deps=true

escript: compile
	./rebar escriptize escript_name=deltazip skip_deps=true

dialyze: compile
	dialyzer ebin/*.beam

.PHONY: all compile clean test escript_name=deltazip dialyze
