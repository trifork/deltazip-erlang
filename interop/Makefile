ERL_ROOT := $(shell erl -noshell -eval 'io:format("~s\n", [code:root_dir()]), init:stop().')
IC_ROOT := $(shell ls -1d ${ERL_ROOT}/lib/ic-*/)
IC_JAR := $(shell ls -1 ${ERL_ROOT}/lib/ic-*/priv/ic.jar)
JI_JAR := $(shell ls -1 ${ERL_ROOT}/lib/jinterface-*/priv/OtpErlang.jar)
CLASSPATH := ${IC_JAR}:${JI_JAR}
DUMMY := $(shell echo Hello)

all: deps build

deps:
	-mkdir deps
	-git clone git://github.com/krestenkrab/triq.git deps/triq
	(cd deps/triq && ./rebar compile)

build: build-java build-erlang

build-java: report_paths
	-mkdir generated target
	erlc '+{be,java}' -o generated -I "${IC_ROOT}/include" deltazip.idl
	#javac -classpath "${CLASSPATH}" -d target generated/erlang/*.java
	javac -classpath "${CLASSPATH}:target" -d target generated/*.java generated/*/*.java generated/*/*/*.java  -Xlint:deprecation
	javac -classpath "${CLASSPATH}:target" -d target *.java

build-erlang:
	-mkdir target
	erlc '+{be,erlang}' -o generated deltazip.idl
	erlc  -o target generated/oe_deltazip.erl
	erlc  -o target -pa deps/triq/ebin -I deps *.erl

run:
	${MAKE} run-java &
	${MAKE} run-erlang

run-java:
	java -classpath "${CLASSPATH}:target" DeltaZipExerciserServer deltazip_java cookie 

run-erlang:
	erl -noshell -noinput -pa target deps/triq/ebin ../ebin -sname 'deltazip-erlang' -setcookie cookie -eval 'deltazip_exerciser:run(), init:stop().'

console-erlang:
	erl -pa target deps/triq/ebin ../ebin -sname 'deltazip-erlang' -eval 'c:l(deltazip_exerciser), c:l(deltazip_exerciser_statem).'

clean:
#	DeltaZipExerciser
	-rm -r generated/ target/

report_paths:
	@echo "ERL_ROOT is ${ERL_ROOT}"
	@echo "IC_ROOT is ${IC_ROOT}"
	@echo "CLASSPATH is ${CLASSPATH}"

.PHONY: all clean report_paths
.PHONY: build build-java build-erlang
.PHONY: run run-java run-erlang

# Not 'deps'.
