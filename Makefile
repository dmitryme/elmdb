PROJECT = elmdb
PROJECT_DESCRIPTION = elmdb
PROJECT_VERSION = 0.1.0

# Whitespace to be used when creating files from templates.
SP = 2

include $(if $(ERLANG_MK_FILENAME),$(ERLANG_MK_FILENAME),erlang.mk)
