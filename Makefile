## -*- makefile -*-
SHELL := /bin/bash
include vsn.mk
.PHONY: all test edoc plt dialyzer tags clean dist install uninstall

PLT=".dialyzer_plt"
ERL_LIB := $(shell erl -noshell -eval 'io:format("~s",[code:lib_dir()]),erlang:halt()' \
		2> /dev/null)
APP_FULLNAME := $(APP_NAME)-$(APP_VSN)

ifdef LOG
	MAKE_ARGS =
else
	MAKE_ARGS ='NOLOG=true'
endif

all: compile

compile:
	(mkdir -p ./ebin)
	(cd src;$(MAKE) $(MAKE_ARGS))

test_compile: 
	(mkdir -p ./ebin)
	(cd src;$(MAKE) TEST=true $(MAKE_ARGS))

test: clean unit_test comm_test 
	@#(make clean)

unit_test: test_compile 
	(erl -noinput -pa ./ebin -eval "eunit:test(\"./ebin\", []), init:stop()")

comm_test: test_compile 
	(mkdir -p ./test/log)
	@echo "pwd is `pwd`"
	(erl -s ct_run script_start -logdir `pwd`/test/log -include `pwd`/include -pa `pwd`/ebin -pa `pwd`/test-dir . -spec ./test/test.spec -s erlang halt)

edoc: 
	(mkdir -p ./edoc)
	(cd src; $(MAKE) edoc)

plt : 
	(./scripts/gen_plt.sh -a sasl -a compiler)

dialyzer: clean
	(cd src;$(MAKE) DEBUG=true)
	(dialyzer --plt $(PLT) -Werror_handling -Wrace_conditions  -r .)

tags :
	(ctags -R .)

clean:
	(rm -rf ./ebin/*; rm -rf ./edoc/*)
	(rm -rf ./test/log)
	(rm -rf ./test/*.beam)
	(cd src;$(MAKE) clean)

dist: clean
	(cd .. \
	&& tar czf $(APP_FULLNAME).tar.gz \
		$(APP_NAME)/src $(APP_NAME)/include \
		$(APP_NAME)/Makefile $(APP_NAME)/vsn.mk)
	(mv ../$(APP_FULLNAME)-$(APP_VSN).tar.gz .)

install: all
ifeq ($(ERL_LIB), )
	@echo "please install Erlang/OTP"
else
	@echo "install..."
	(mkdir -p $(ERL_LIB)/$(APP_FULLNAME) && \
		cp -rf ./ebin ./include ./src $(ERL_LIB)/$(APP_FULLNAME))
endif

uninstall:
	@echo "uninstall the lib..."
	(rm -rf $(ERL_LIB)/$(APP_FULLNAME))
