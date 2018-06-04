import argparse
import csv


def construct_line(line, header):
    string_vw = "{0} | ".format(line.pop(0))
    features_list = list(zip(header, line))
    features_list = ['{0}:{1}'.format(u, v) for u, v in features_list]
    string_vw += " ".join(features_list)
    string_vw += "\n"

    return string_vw

parser = argparse.ArgumentParser(description='Convert CSV to VW format.')
parser.add_argument("input_file", help="path to csv file")
parser.add_argument("output_file", help="path to output vw file")
args = parser.parse_args()


with open(args.input_file, encoding='utf-8') as input_file:
    with open(args.output_file, mode='a', encoding='utf-8') as output_file:
        reader = csv.reader(input_file)
        header = next(reader)
        del(header[0])
        for row in reader:
            output_file.write(construct_line(row, header))
