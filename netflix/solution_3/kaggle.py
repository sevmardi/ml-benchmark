import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


df1 = pd.read_csv('../data/netflix_kaggle/combined_data_1.txt',
                  header=None, names=['user_id', 'rating'], usecols=[0, 1])
print('part 1 shape')
print(df1.shape)
print("top 10 rows")
print(df1.head(10))


df2 = pd.read_csv('../data/netflix_kaggle/combined_data_2.txt',
                  header=None, names=['user_id', 'rating'], usecols=[0, 1])
print('part 2 shape')
print(df2.shape)
print("top 10 rows")
print(df2.head(10))

df3 = pd.read_csv('../data/netflix_kaggle/combined_data_3.txt',
                  header=None, names=['user_id', 'rating'], usecols=[0, 1])
print('part 3 shape')
print(df3.shape)
print("top 10 rows")
print(df3.head(10))

df4 = pd.read_csv('../data/netflix_kaggle/combined_data_4.txt',
                  header=None, names=['user_id', 'rating'], usecols=[0, 1])
print('part 4 shape')
print(df4.shape)
print("top 10 rows")
print(df4.head(10))

# Check if the Rating column is null, if so it means it is 'movie id' row, count them
movie_count = df1.isnull().sum()[1]
print('number of movies in part_1 : {}'.format(movie_count))

# Count total number of rows and subtract movies' count
print('number of Users in part_1 : {}'.format(
    df1['user_id'].nunique() - movie_count))


# Count total number of observations
num_observations = df1['user_id'].count() - movie_count
print('number of observations in part_1 : {}'.format(num_observations))


# Trying to see the uniformity in the data
print(df1.groupby('rating').count() * 100 / num_observations)

# 85% of the data has  3 or more than rating


### Use below code to process each part one by one, also keep updating the movie_id variable

df_nan = pd.DataFrame(pd.isnull(df1.rating))
df_nan = df_nan[df_nan['rating']==True]
print(df_nan.shape)

# create a single array with movie id - size ( difference of index) and value ( 1,2,3 etc)

movie_np = []

## We keep changing this variable by manually looking up the movie_id in one of those 4 data files
## As of now, this is to process the 4th part
movie_id = 13368

for i,j in zip(df_nan.index[1:],df_nan.index[:-1]):
#     print(i,j)
    temp_arr = np.full((1,i-j-1), movie_id)
    movie_np = np.append(movie_np,temp_arr)
    movie_id += 1

# last movie id

print(df_nan.iloc[-1, 0])

r = np.full((1,len(df1) - df_nan.index[-1] -1), movie_id)
#print(temp_arr)
movie_np = np.append(movie_np,r)
print(len(movie_np))

### Append the movie_np array as a column to the dataframe
###
df1 = df1[pd.notnull(df1['rating'])]
#Add the movie_id column
df1['movie_id'] = movie_np.astype(int)
df1['user_id'] = df1['user_id'].astype(int)
print(df1.columns)

print(df1.iloc[::5000000,:])

new_cols = df1.columns.tolist()
new_cols = new_cols[:1]+new_cols[-1:]+new_cols[1:2]
df1 = df1[new_cols]

print("persist the processed file ")
df1.to_csv("processed_part4.txt", encoding='utf-8', index=False)


## Here we bring data into the forma of a matrix
## BUT, we don't use it
df_p = pd.pivot_table(df1,values='rating', index='movie_id', columns='user_id')
print(df_p.shape)

