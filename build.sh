#!/bin/sh

bname="strings2bbolt"
bdir="./cmd/${bname}"
oname="${bdir}/${bname}"

mkdir -p "${bdir}"

go \
	build \
	-v \
	./...

go \
	build \
	-v \
	-o "${oname}" \
	"${bdir}"
