PYTHON          := $(shell which python)
PYTHON_VERSION  := $(shell $(PYTHON) -c 'import sys; print "%s.%s" % sys.version_info[0:2]')
VIRTUALENV_OPTS := --no-site-packages
VIRTUALENV      := $(shell which virtualenv-$(PYTHON_VERSION)) $(VIRTUALENV_OPTS)

all:
	python setup.py bdist_egg

clean:
	rm -rf *.egg-info build dist

.Python:
	$(VIRTUALENV) .

extraclean: clean
	rm -rf bin include lib .Python
