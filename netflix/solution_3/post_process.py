import pandas as pd
import numpy as np


# here we grab the processed netfilx files
df_train = pd.read_csv('processed_part4.txt')
df_prob['rating'] = np.nan
print(df_prob.head(10))

# here we grab the processed prob file 
df_probe = pd.read_csv('processed_probe.txt')

print("length of train df")
print(len(df_train))

print()

df_probe['movie_id'] = df_probe['movie_id'].astype(int)

# print(len(df_prob))

keys = ['user_id', 'movie_id']
i1 = df_train.set_index(keys).index
i2 = df_probe.set_index(keys).index

df_pure_train =  df_train[~i1.isin(i2)]
df_pure_train.to_csv("processed_pure_train_4.txt", encoding='utf-8', index=False)

# now we get the probe file
i3 = df_pure_train.set_index(keys).index
df_pure_probe =df_train[~i1.isin(i3)]

df_pure_probe.to_csv("processed_pure_probe_4.txt", encoding='utf-8', index=False)