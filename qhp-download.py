#!/usr/bin/env python3

import sys
import argparse
import zmq
import io
import json
import csv
import datetime
import struct

def timeframe_to_seconds(tf):
    if tf == 'M1':
        return 60
    elif tf == 'M5':
        return 5 * 60
    elif tf == 'M15':
        return 15 * 60
    elif tf == 'H1':
        return 3600
    elif tf == 'D':
        return 86400
    elif tf == 'W':
        return 7 * 86400
    else:
        raise ValueError('Invalid value')

class BarAggregator:
    def __init__(self, timeframe):
        self.open_ = 0
        self.high = 0
        self.low = 0
        self.close = 0
        self.volume = 0
        self.timestamp = None
        self.current_bar_number = None
        self.timeframe = timeframe

    def push_bar(self, timestamp, open_, high, low, close, volume):
        bar_number = timestamp.timestamp() // self.timeframe
        if bar_number != self.current_bar_number:
            b_open = self.open_
            b_high = self.high
            b_low = self.low
            b_close = self.close
            b_volume = self.volume
            b_timestamp = self.timestamp

            self.open_ = open_
            self.high = high
            self.low = low
            self.close = close
            self.volume = volume
            self.timestamp = timestamp
            prev_bar_number = self.current_bar_number
            self.current_bar_number = bar_number

            if prev_bar_number is not None:
                return (b_timestamp, b_open, b_high, b_low, b_close, b_volume)
        else:
            self.high = max(high, self.high)
            self.low = min(low, self.low)
            self.close = close
            self.volume += volume
            return None

    def get_bar(self):
        b_open = self.open_
        b_high = self.high
        b_low = self.low
        b_close = self.close
        b_volume = self.volume
        b_timestamp = self.timestamp

        return (b_timestamp, b_open, b_high, b_low, b_close, b_volume)

def main():
    parser = argparse.ArgumentParser(description='QHP client')
    parser.add_argument('-o', '--output-file', action='store', dest='output_file', help='Output filename', required=True)
    parser.add_argument('-p', '--timeframe', action='store', dest='timeframe', help='Data timeframe', required=True)
    parser.add_argument('-q', '--qhp', action='store', dest='qhp', help='QHP endpoint', required=True)
    parser.add_argument('-y', '--symbol', action='store', dest='symbol', help='Symbol to download', required=True)
    parser.add_argument('-f', '--from', action='store', dest='from_', help='Starting date', required=True)
    parser.add_argument('-t', '--to', action='store', dest='to', help='Ending date', required=True)
    parser.add_argument('-r', '--rescale', action='store', dest='rescale', help='Rescale to timeframe')
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

    agg = None
    if args.rescale:
        agg = BarAggregator(int(args.rescale))

    rq = {
        "ticker" : symbol,
        "from" : start_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "to" : end_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "timeframe" : period
    }

    s.send_multipart([bytes(json.dumps(rq), "utf-8")])
    parts = s.recv_multipart()

    print(parts[0])
    if parts[0] != b'OK':
       print("Error:", parts[1])


    line_count = 0
    with open(args.output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['<TICKER>', '<PER>', '<DATE>', '<TIME>', '<OPEN>', '<HIGH>', '<LOW>', '<CLOSE>', '<VOLUME>'])
        for line in struct.iter_unpack("<qddddQ", parts[1]):

            timestamp = int(line[0])
            open_ = float(line[1])
            high = float(line[2])
            low = float(line[3])
            close = float(line[4])
            volume = int(line[5])
            dt = datetime.datetime.utcfromtimestamp(timestamp) + timedelta

            if agg:
                mbar = agg.push_bar(dt, open_, high, low, close, volume)
                if mbar is not None:
                    line_count += 1
                    writer.writerow([symbol, agg.timeframe, mbar[0].strftime('%Y%m%d'), mbar[0].strftime('%H%M%S'), str(mbar[1]), str(mbar[2]), str(mbar[3]), str(mbar[4]), str(mbar[5])])
            else:
                line_count += 1
                writer.writerow([symbol, period, dt.strftime('%Y%m%d'), dt.strftime('%H%M%S'), str(open_), str(high), str(low), str(close), str(volume)])


        if agg:
            mbar = agg.get_bar()
            if mbar is not None:
                line_count += 1
                writer.writerow([symbol, agg.timeframe, mbar[0].strftime('%Y%m%d'), mbar[0].strftime('%H%M%S'), str(mbar[1]), str(mbar[2]), str(mbar[3]), str(mbar[4]), str(mbar[5])])
        

    print("Written {} lines".format(line_count))

if __name__ == '__main__':
    main()

