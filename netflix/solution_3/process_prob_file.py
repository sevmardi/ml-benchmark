import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from collections import OrderedDict

prob_file = open("data/netflix/probe.txt", 'r')
lines = prob_file.read().split("\n")
movie_id_dict = {}
prev_index = 0
prev_value = 1


value_list = []

for i in range(len(lines)):
    if(":" in lines[i]):
        movie_id_dict[str(prev_value)] = i - prev_index - 1
        prev_index = i
        prev_value = int(lines[i].split(":")[0])
    else:
        value_list.append(lines[i])
movie_id_dict[str(prev_value)] = len(lines) - prev_index - 1
ordered_dict = OrderedDict(sorted(movie_id_dict.items()))


# create np array based on the ordered dict
movie_np = []
for k, v in ordered_dict.items():
    temp_arr = np.full((1, v), int(k))
    movie_np = np.append(movie_np, temp_arr)


df = pd.DataFrame(value_list, columns=['user_id'])
df['movie_id'] = movie_np.astype(int)
