import os
import numpy as np
import re
import time
import resource
from sklearn.datasets import load_files
from sklearn.metrics import accuracy_score, roc_auc_score

resource.setrlimit(resource.RLIMIT_NPROC, (1, 1))

# read the data
print('loading.. ') 
start =  time.time()

path_to_movies = os.path.expanduser('/home/s2077086/data/nlp_for_movie_review/aclImdb')
review_train = load_files(os.path.join(path_to_movies, 'train'))

text_train, y_train = review_train.data, review_train.target

print('data was loaded!')
print('now converting to vw format...') 


def to_vw_format(document, label=None):
    return str(label or '') + ' |text ' + ' '.join(re.findall('\w{3,}', document.lower())) + '\n'


to_vw_format(str(text_train[1]), 1 if y_train[0] == 1 else -1)


# spliting the data
print('splitting the data... ') 

train_size = int(0.7 * len(text_train))
train, train_labels = text_train[:train_size], y_train[:train_share]
valid, valid_labels = text_train[train_size:], y_train[train_share:]

# Convert and save in vowpal wabbit format
print('saving vw file .. ') 
with open('movie_reviews_train.vw', 'w') as vw_train_data:
    for text, target in zip(train, train_labels):
        vw_train_data.write(to_vw_format(str(text), 1 if target == 1 else -1))

with open('movie_reviews_valid.vw', 'w') as vw_train_data:
	for text, target in zip(valid, valid_labels):
		vw_train_data.write(to_vw_format(str(text), 1 if target == 1 else -1))


#time 
print('\ntime taken %s seconds ' % str(time.time() - start))
elapsed_time = time.time() - start
print(time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))