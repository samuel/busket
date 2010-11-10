ERL ?= erl
APP := busket

.PHONY: deps

all: deps
	@./rebar get-deps
	@./rebar compile

deps:
	@./rebar get-deps

clean:
	@./rebar clean || true

distclean: clean
	@./rebar delete-deps

release: clean all
	@./rebar generate

docs:
	@erl -noshell -run edoc_run application '$(APP)' '"."' '[]'
