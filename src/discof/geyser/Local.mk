ifdef FD_HAS_HOSTED
$(call add-objs,fd_geyser_tile,fd_discof)
$(call make-unit-test,test_geyser_tile,test_geyser_tile,fd_discof fd_disco fd_flamenco fd_waltz fd_tango fd_ballet fd_util)
endif
