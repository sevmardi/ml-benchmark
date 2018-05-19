#!/bin/bash
# run this to load the MNIST8M data

echo "Loading MNIST8M data"
MNIST8M = /data/vw/mnist-8m
mkdir -p /data/vw/mnist-8m/parts

cd $(MNIST8M)
echo "Uncompressing MNIST8M data"
bunzip2 -c mnist8m.bz2 > mnist8m.libsvm

echo "Splitting MNIST8M data"

split -l 100000 mnist8m.libsvm parts/part
	j=0
	for i in {a..z}{a..z};do 
		jj=`printf "%02d" $j`
		mv parts/part$i parts/part$jj
		j=$((j+1))
		if [ $j -gt 80 ]; then break; fi
	done
fi


cd ${MNIST8M}/parts
processmnist8m.ssc