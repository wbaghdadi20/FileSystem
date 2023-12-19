# Target library
lib := libfs.a
targets	:= disk fs
objs	:= disk.o fs.o
CC	:= gcc

CFLAGS	:= -Wall -Wextra -Werror
CFLAGS	+= -g

ifneq ($(V),1)
Q = @
endif

all: $(lib)

# all: $(targets)
deps := $(patsubst %.o,%.d,$(objs))
-include $(deps)

$(targets): $(objs)
	@echo "CC $@"
	$(Q)$(CC) $(CFLAGS) -o $@ $<

%.o: %.c
	@echo "CC $@"
	$(Q)$(CC) $(CFLAGS) -c -o $@ $<

$(lib): $(objs)
	@echo "CC $@"
	$(Q) ar rcs  $@ $^

clean:
	@echo "clean"
	$(Q)rm -f $(deps) $(targets) $(objs) $(lib)
