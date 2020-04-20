#!/usr/bin/env python3

import sys
import argparse
import zmq
import io
import json
import csv
import datetime
import struct


def main():
    parser = argparse.ArgumentParser(description='QHP client')
    parser.add_argument('-o', '--output-file', action='store', dest='output_file', help='Output filename', required=True)
    parser.add_argument('-p', '--timeframe', action='store', dest='timeframe', help='Data timeframe', required=True)
    parser.add_argument('-q', '--qhp', action='store', dest='qhp', help='QHP endpoint', required=True)
    parser.add_argument('-y', '--symbol', action='store', dest='symbol', help='Symbol to download', required=True)
    parser.add_argument('-f', '--from', action='store', dest='from_', help='Starting date', required=True)
    parser.add_argument('-t', '--to', action='store', dest='to', help='Ending date', required=True)
    parser.add_argument('-d', '--time-delta', action='store', dest='time_delta', help='Add given time delta (in seconds)', required=False)

    args = parser.parse_args()

    period = args.timeframe
    symbol = args.symbol
    filename = args.output_file

    ctx = zmq.Context.instance()
    s = ctx.socket(zmq.REQ)
    s.connect(args.qhp)

    start_time = datetime.datetime.strptime(args.from_, "%Y%m%d")
    end_time = datetime.datetime.strptime(args.to, "%Y%m%d")

    timedelta = datetime.timedelta()
    if args.time_delta:
        timedelta = datetime.timedelta(seconds=int(args.time_delta))

    rq = {
        "ticker" : symbol,
        "from" : start_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "to" : end_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "timeframe" : period
    }

    s.send_multipart([bytes(json.dumps(rq), "utf-8")])
    parts = s.recv_multipart()

    if parts[0] != b'OK':
       print("Error:", parts[1])

    line_count = 0
    with open(args.output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['<TICKER>', '<PER>', '<DATE>', '<TIME>', '<OPEN>', '<HIGH>', '<LOW>', '<CLOSE>', '<VOLUME>'])
        for line in struct.iter_unpack("<qddddQ", parts[1]):
            line_count += 1

            timestamp = int(line[0])
            open_ = float(line[1])
            high = float(line[2])
            low = float(line[3])
            close = float(line[4])
            volume = int(line[5])
            dt = datetime.datetime.utcfromtimestamp(timestamp) + timedelta

            writer.writerow([symbol, period, dt.strftime('%Y%m%d'), dt.strftime('%H%M%S'), str(open_), str(high), str(low), str(close), str(volume)])


    print("Written {} lines".format(line_count))

if __name__ == '__main__':
    main()

