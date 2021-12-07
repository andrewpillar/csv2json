#!/bin/sh

set -e

[ ! -d bin ] && mkdir bin

set -x

go build -o bin/csv2json
