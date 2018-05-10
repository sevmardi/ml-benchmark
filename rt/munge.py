import csv
import re

location_train = "../data/rt/train.tsv"
location_test = "../data/rt/test.tsv"

location_train_vw = "../data/rt/rotten.train.vw"  # will be created
location_test_vw = "../data/rt/rotten.test.vw"  # will be created


def clean(s):
    return "".join(re.findall(r'\w+', s, flags=re.UNICODE | re.LOCALE)).lower()


# creates Vowpal Wabbit-formatted file from tsv file

def to_vw(location_input_file, location_output_file, test=False):
    print("\nReading:", location_input_file,
          "\nWriting:", location_output_file)
    with open(location_input_file) as infile, open(location_output_file, "wb") as outfile:
        reader = csv.DictReader(infile, delimiter="\t")
        # for every line
        for row in reader:
            if test:
                label = "1"
            else:
                label = str(int(row['Sentiment']))

            phrase = clean(row['Phrase'])
            outfile.write(label + " '" + row['PhraseId'] + " |f " + phrase + " |a " + "word_count:" + str(phrase.count(" ") + 1) + "\n")


to_vw(location_train, location_train_vw)
# to_vw(location_test, location_test_vw, test=True)
