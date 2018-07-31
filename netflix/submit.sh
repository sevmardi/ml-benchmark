#!/bin/bash

cd solution_1
spark-submit --executor-memory 10g recommender.py
# spark-submit --executor-memory 3g movielens.py
