#!/bin/bash

cd solution_3
# spark-submit --executor-memory 10g recommender.py
spark-submit --executor-memory 3g kaggle.py
# spark-submit --executor-memory 3g movielens.py
