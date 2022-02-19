#!/bin/sh

set -e

[ ! -d bin ] && mkdir bin

set -x

go test -cover
go build -o bin/csv2json
