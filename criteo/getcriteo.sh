
cd /data/vw/criteo-display-advertising-dataset

mkdir -p parts

split -a 2 -d -l 500000 train.txt parts/train
split -a 2 -d -l 500000 test.txt parts/test





