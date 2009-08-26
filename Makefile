PYTHON     ?= $(shell test -f bin/python && echo bin/python || which python)
PYVERS      = $(shell $(PYTHON) -c 'import sys; print "%s.%s" % sys.version_info[0:2]')
VIRTUALENV ?= $(shell test -x `which virtualenv` && which virtualenv || \
                      test -x `which virtualenv-$(PYVERS)` && \
                          which virtualenv-$(PYVERS))
VIRTUALENV += --no-site-packages
SETUP       = $(PYTHON) ./setup.py
PLATFORM    = $(shell $(PYTHON) -c "from pkg_resources import get_build_platform; print get_build_platform()")
EGG         = $(shell $(SETUP) --fullname)-py$(PYVERS).egg
SOURCES     = $(shell find . -type f -name \*.py)

.PHONY: test dev clean extraclean

all: egg
egg: dist/$(EGG)

dist/$(EGG):
	$(SETUP) bdist_egg

test:
	$(SETUP) test

coverage: coverage/index.html
coverage/index.html: .coverage
	coverage -b -i -d $@

.coverage: $(SOURCES)
	-coverage -e -x setup.py test

env: .Python
.Python:
	$(VIRTUALENV) .

dev: .Python setup.py
	$(SETUP) develop

tags: TAGS.gz

TAGS.gz: TAGS
	gzip $^

TAGS: $(SOURCES)
	ctags -eR .

clean:
	$(SETUP) clean
	rm -rf *.egg-info build dist

extraclean: clean
	rm -rf bin include lib .Python
