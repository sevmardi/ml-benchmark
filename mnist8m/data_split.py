import os
import random
import math

data_dir = 'data/mnist-8m/mnist8m'
train_file = 'mnist8m.scale'
num_part = 3360
# https://github.com/xcgoner/ps-lite-new/blob/70b106de3be190cc41555c0754596e53eca365b5/examples/LR_proximal/script/data_split.py

def get_data(filename, is_shuffle=True):
    samples = []
    with open(filename, 'r') as f:
        for line in f:
            samples.append(line)
    if is_shuffle:
        random.shuffle(samples)
    return samples

train_dir = os.path.join(data_dir, 'mnist')

if not os.path.isdir(train_dir):
    os.mkdir(train_dir)

print('generating train data...')
samples = get_data(os.path.join(data_dir, train_file))
num_train = len(samples)
print(num_train)
index = 0
part_size = int(math.ceil(float(num_train) / num_part))
print(part_size)
for part in range(num_part):
    with open(os.path.join(train_dir, 'part-00{}'.format(part + 1)), 'w') as f:
        for j in range(0, part_size):
            if index < num_train:
                f.write(samples[index])
                index += 1

print('done.')