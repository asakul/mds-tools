#!/usr/bin/env python3

import sys
import argparse
import zmq
import io
import json
import csv
import datetime
import struct
from pytz import timezone

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
            if self.current_bar_number is not None:
                b_timestamp = datetime.datetime.fromtimestamp(self.current_bar_number * self.timeframe)

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
        b_timestamp = datetime.datetime.fromtimestamp(self.timeframe * ( self.timestamp.timestamp() // self.timeframe))

        return (b_timestamp, b_open, b_high, b_low, b_close, b_volume)

def get_data(qhp, ticker, start_time, end_time, period, tz, timedelta):
    rq = {
        "ticker" : ticker,
        "from" : start_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "to" : end_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "timeframe" : period
    }

    qhp.send_multipart([bytes(json.dumps(rq), "utf-8")])
    resp = qhp.recv()

    if resp != b'OK':
        errmsg = qhp.recv_string()
        return None

    bar_count = 0
    result = []
    while True:
        if qhp.getsockopt(zmq.RCVMORE) == 0:
            break
        rawdata = qhp.recv()
        for line in struct.iter_unpack("<qddddQ", rawdata):

            timestamp = int(line[0])
            open_ = float(line[1])
            high = float(line[2])
            low = float(line[3])
            close = float(line[4])
            volume = int(line[5])
            dt = datetime.datetime.fromtimestamp(timestamp, tz) + timedelta

            bar_count += 1
            result.append((dt, open_, high, low, close, volume))

    return result

def write_to_file(writer, bars, ticker, period):
    for bar in bars:
        writer.writerow([ticker, period, bar[0].strftime("%Y%m%d"), bar[0].strftime("%H%M%S"), bar[1], bar[2], bar[3], bar[4], bar[5]])

def make_tickers_list(base, start_time, end_time, futures_interval):
    result = []
    month = start_time.date().month
    year = start_time.date().year

    while True:
        if month % futures_interval == 0:
            result.append(base + '-' + str(month) + '.' + str(year)[-2:])
            if month > end_time.date().month and year >= end_time.date().year:
                break

        month += 1
        if month > 12:
            month = 1
            year += 1

    return result
            

def main():
    parser = argparse.ArgumentParser(description='QHP client')
    parser.add_argument('-o', '--output-file', action='store', dest='output_file', help='Output filename', required=True)
    parser.add_argument('-p', '--timeframe', action='store', dest='timeframe', help='Data timeframe', required=True)
    parser.add_argument('-q', '--qhp', action='store', dest='qhp', help='QHP endpoint', required=True)
    parser.add_argument('-y', '--symbol', action='store', dest='symbol', help='Base symbol', required=True)
    parser.add_argument('-f', '--from', action='store', dest='from_', help='Starting date', required=True)
    parser.add_argument('-t', '--to', action='store', dest='to', help='Ending date', required=True)
    parser.add_argument('-r', '--rescale', action='store', dest='rescale', help='Rescale to timeframe')
    parser.add_argument('-d', '--time-delta', action='store', dest='time_delta', help='Add given time delta (in seconds)', required=False)
    parser.add_argument('-i', '--futures-interval', action='store', dest='futures_interval', help='Futures interval between exprations in month', required=True)
    parser.add_argument('-s', '--stitch-delta', action='store', dest='stitch_delta', help='Futures interval between exprations in month', required=True)
    parser.add_argument('-e', '--replace-ticker', action='store', dest='replace_ticker', help='Replace ticker id in file', required=False)

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

    delta = int(args.stitch_delta)

    agg = None
    if args.rescale:
        agg = BarAggregator(int(args.rescale))

    data = {}
    tickers = make_tickers_list(symbol, start_time, end_time, int(args.futures_interval))
    print("Tickers: {}".format(tickers))
    for ticker in tickers:
        print("Requesting data: {}".format(ticker))
        bars = get_data(s, ticker, start_time, end_time, period, timezone('UTC'), timedelta)

        if len(bars) > 0:
            data[ticker] = { 'bars' : bars }
            print("Cutting off trailing data: {}".format(ticker))
            end_date = data[ticker]['bars'][-1][0]
            cutoff_date = datetime.date.fromordinal(end_date.toordinal() - delta)
            #cutoff_date_num = cutoff_date.year * 10000 + cutoff_date.month * 100 + cutoff_date.day

            data[ticker]['bars'] = [s for s in data[ticker]['bars'] if s[0].date() <= cutoff_date]
            data[ticker]['end_date'] = cutoff_date

    prev_ticker = None
    for k, v in sorted(data.items(), key=lambda x: x[1]['end_date']):
        print("Cutting off starting data: {}".format(k))
        if prev_ticker is not None:
            start_date = data[prev_ticker]['bars'][-1][0]
            v['bars'] = [s for s in data[k]['bars'] if s[0] > start_date]
        prev_ticker = k
        
    with open(args.output_file, 'w+') as f:
        writer = csv.writer(f)
        writer.writerow(['<TICKER>', '<PER>', '<DATE>', '<TIME>', '<OPEN>', '<HIGH>', '<LOW>', '<CLOSE>', '<VOLUME>'])
        for k, v in sorted(data.items(), key=lambda x: x[1]['end_date']):
            ticker = args.replace_ticker
            if ticker is None:
                ticker = k
            write_to_file(writer, v['bars'], k, period)

if __name__ == '__main__':
    main()

