# Makefile for yamc 
# uses version from git with commit hash

help:
	@echo "make <target>"
	@echo "build	build yamc-influxdb."
	@echo "clean	clean all temporary directories."
	@echo ""

build:
	python setup.py bdist_wheel	

clean:
	rm -fr dist
	rm -fr *.egg-info

