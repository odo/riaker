all: deps compile

shell:
	erl -pz ebin deps/*/ebin

compile:
	rebar compile skip_deps=true

deps:
	rebar get-deps

clean:
	rebar clean

test:
	rebar skip_deps=true eunit

start:
	erl -pz ebin deps/*/ebin -name riaker -eval "application:start(sasl), application:start(riaker)."

xref: compile
	rebar xref skip_deps=true