.PHONY: all clean eunit test install

all:
	@./rebar compile

clean:
	@./rebar clean

eunit:
	@./rebar eunit -v

test:
	@./rebar test -v

install:
	@./rebar install
