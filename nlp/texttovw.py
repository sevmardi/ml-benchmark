import os
import random

def texttoVW(lines):
    return ' '.join([l.strip() for l in lines]).replace(':', 'COLON').replace('|', 'PIPE')


def fileToVW(inputfile):
    return texttoVW(open(inputfile, 'r').readline())

# print(fileToVW('../data/review_polarity/txt_sentoken/neg/cv000_29416.txt')[:50])


def read_text_files_in_dirctory(dir):
    return [fileToVW(dir + os.sep + f) for f in os.listdir(dir) if f.endswith('.txt')]

examples = ['+1 | ' + s for s in read_text_files_in_dirctory('../data/review_polarity/txt_sentoken/pos')] + \
    ['-1 | ' + s for s in read_text_files_in_dirctory(
        '../data/review_polarity/txt_sentoken/neg')]

print("%d total examples read" % len(examples))


random.seed(1234)

random.shuffle(examples)  # this does in-place shuffling

# print out the labels of the first 50 examples to be sure they're sane:
print("".join(s[0] for s in examples[:50]))

def write_to_vw_file(filename, examples):
	with open(filename, 'w') as h:
		for ex in examples:
			print >> h, ex

write_to_vw_file('../data/review_polarity/sentiment.tr', examples[:1600])
write_to_vw_file('../data/review_polarity/sentiment.te', examples[1600:])
