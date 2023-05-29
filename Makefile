# Makefile for yamc 
# uses version from git with commit hash

help:
	@echo "make <target>"
	@echo "build	build yamc-influxdb."
	@echo "clean	clean all temporary directories."
	@echo ""

build:
	python setup.py egg_info sdist	

clean:
	rm -fr dist
	rm -fr *.egg-info

