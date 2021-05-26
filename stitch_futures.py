#!/usr/bin/env python3

import sys
import argparse
import csv
import os
import datetime

def parse_date(x):
    return datetime.datetime.strptime(x, '%Y%m%d').date()

def read_file(f):
    reader = csv.reader(f, delimiter=',')
    ticker = None
    next(reader)
    result = { 'bars' : [] }

    for line in reader:
        result['bars'].append(line)
        if ticker is None:
            ticker =  line[0]
            result['ticker'] = ticker

    return result

def write_to_file(writer, bars, ticker):
    for bar in bars:
        if ticker is not None:
            bar[0] = ticker
        writer.writerow(bar)


def main():
    parser = argparse.ArgumentParser(description='Stitch futures')
    parser.add_argument('-i', '--input-directory', action='store', dest='input_directory', help='Input directory', required=True)
    parser.add_argument('-o', '--output-file', action='store', dest='output_file', help='Output filename', required=True)
    parser.add_argument('-d', '--stitch-delta', action='store', dest='stitch_delta', help='Offset at which stitching occurs (days)', required=False)
    parser.add_argument('-t', '--ticker', action='store', dest='replace_ticker', help='Replace ticker')

    args = parser.parse_args()

    input_directory = args.input_directory
    output_file = args.output_file
    delta = int(args.stitch_delta)

    ticker = args.replace_ticker

    data = []
    for filename in os.listdir(input_directory):
        full_name = os.path.join(input_directory, filename)
        print("Reading {}".format(full_name))
        with open(full_name, 'r') as f:
            data.append(read_file(f))

        
    for f in data:
        print("Cutting off trailing data: {}".format(f['ticker']))
        end_date = parse_date(f['bars'][-1][2])
        cutoff_date = datetime.date.fromordinal(end_date.toordinal() - delta)
        cutoff_date_num = cutoff_date.year * 10000 + cutoff_date.month * 100 + cutoff_date.day

        f['bars'] = [s for s in f['bars'] if int(s[2]) <= cutoff_date_num]
        f['end_date'] = cutoff_date

    data.sort(key=lambda x: x['end_date'])

    for i in range(1, len(data)):
        print("Cutting off starting data: {}".format(data[i]['ticker']))
        start_date = parse_date(data[i - 1]['bars'][-1][2])
        start_date_num = start_date.year * 10000 + start_date.month * 100 + start_date.day
        data[i]['bars'] = [s for s in data[i]['bars'] if int(s[2]) > start_date_num]

    with open(args.output_file, 'w+') as f:
        writer = csv.writer(f)
        writer.writerow(['<TICKER>', '<PER>', '<DATE>', '<TIME>', '<OPEN>', '<HIGH>', '<LOW>', '<CLOSE>', '<VOLUME>'])
        for d in data:
            write_to_file(writer, d['bars'], ticker)
        

if __name__ == '__main__':
    main()
