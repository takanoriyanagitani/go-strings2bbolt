#!/bin/bash

bin='./cmd/strings2bbolt/strings2bbolt'

input1(){
	echo helo
	echo wrld
}

input2(){
	yes |
		head -1048576 |
		awk '{print NR}'
}

run_native(){
	cat /dev/stdin |
		"${bin}"
}

echo input 1
time input1 | run_native
echo

echo input 2
time input2 | run_native
echo
