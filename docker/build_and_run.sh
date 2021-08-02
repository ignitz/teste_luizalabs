#!/bin/bash

docker build -f linux_amd64.Dockerfile -t luizalabs . \
&& docker run --rm -v $PWD/output:/app/output luizalabs