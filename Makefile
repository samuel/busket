ERL ?= erl
APP := busket

.PHONY: deps

all: deps
	@./rebar compile

deps:
	@./rebar get-deps

clean:
	@./rebar clean

distclean: clean
	@./rebar delete-deps

release: clean all
	@./rebar generate

docs:
	@erl -noshell -run edoc_run application '$(APP)' '"."' '[]'
