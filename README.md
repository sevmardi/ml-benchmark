# A Simple/Limited benchmark for scalability, speed and accuracy of machine learning libraries for classification. 
[![Build Status](https://travis-ci.org/sevmardi/ml-benchmark.svg?branch=master)](https://travis-ci.org/sevmardi/ml-benchmark.svg?branch=master) [![GitHub issues](https://img.shields.io/github/issues/IoTers/Click-Through-Rate-Prediction.svg)](https://github.com/sevmardi/ml-benchmark/issues)

## Introduction

Experiments using primarily Apache Spark

 ![ML-Benchmark](images/ml-benchmark.png)

## Data
The raw data can be accessed here: http://labs.criteo.com/2013/12/download-terabyte-click-logs/

Download the data set

```
for i in 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23; do
	curl -O http://azuremlsampleexperiments.blob.core.windows.net/criteo/day_${i}.gz
	aws s3 mv  day_${i}.gz s3://criteo-display-ctr-dataset/released/
done
````


## Requirements
Coming soon! 

## Dependencies



## Setup

## Results



## References/Inspiration
![benchm-ml](https://github.com/szilard/benchm-ml)
![riteo-1tb-benchmark](https://github.com/rambler-digital-solutions/criteo-1tb-benchmark)

## Copyright
See [LICENSE](LICENSE) for details.
