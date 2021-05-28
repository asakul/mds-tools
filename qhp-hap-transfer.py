#!/usr/bin/env python3

import sys
import argparse
import zmq
import io
import json
import csv
import datetime
import struct
import re
import dateutil.tz

def sec_from_period(period):
    if period == "M1":
        return 60
    elif period == "M5":
        return 60 * 5
    elif period == "M15":
        return 60 * 15
    elif period == "M30":
        return 60 * 30
    elif period == "H1":
        return 60 * 60
    elif period == "D":
        return 86400

def request_ticker_list(socket):
    rq = {
        "get_sec_list" : True,
    }

    socket.send_multipart([bytes(json.dumps(rq), "utf-8")])
    resp = socket.recv()

    if resp != b'OK':
        errmsg = s.recv_string()
        print("Error:", errmsg)
        sys.exit(1)


    rawdata = b''
    while True:
        if socket.getsockopt(zmq.RCVMORE) == 0:
            break
        rawdata += socket.recv()

    s = rawdata.decode('utf-8')
    tickers = s.split(',')
    
    print("Got {} tickers".format(len(tickers)))
    return tickers

def get_month_by_code(code):
    try:
        mon = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'].index(code)
    except ValueError:
        return None
    return mon + 1

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

def upload_data(hap, data, ticker, period):
    print("Uploading ticker: {}".format(ticker))
    serialized_bars = io.BytesIO()
    min_dt = None
    max_dt = None

    for bar in data:
        dt = bar[0]
        serialized_bars.write(struct.pack("<qddddQ", int(dt.timestamp()), bar[1], bar[2], bar[3], bar[4], bar[5]))
        if min_dt is None:
            min_dt = dt
        else:
            if dt < min_dt:
                min_dt = dt

        if max_dt is None:
            max_dt = dt
        else:
            if dt > max_dt:
                max_dt = dt

    rq = {
        "ticker" : ticker,
        "start_time" : min_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "end_time" : max_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "timeframe_sec" : sec_from_period(period)
    }

    raw_data = serialized_bars.getvalue()

    hap.send_multipart([bytes(json.dumps(rq), "utf-8"), raw_data])
    parts = hap.recv_multipart()
    if parts[0] != b'OK':
        return False
    return True

def convert_ticker(s, data):
    if s.startswith("SPBFUT#"):
        last_ts = data[-1][0]
        year = int(s[-1])
        current_year = last_ts.date().year - 2000
        current_year_in_dec = current_year % 10
        current_dec = current_year - current_year_in_dec
        current_mon = last_ts.date().month
        mon = get_month_by_code(s[-2])
        if year >= current_year_in_dec:
            return s[:-2] + ".{}-{}".format(mon, current_dec + year)
        else:
            return s[:-2] + ".{}-{}".format(mon, current_dec + 10 + year)
            
    else:
        return s

def load_blacklist(filename):
    result = []
    with open(filename, 'r') as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            if line != "":
                result.append(re.compile(line))

    return result

def allow_ticker(blacklist, ticker):
    for rx in blacklist:
        if rx.match(ticker):
            return False
    return True 


def main():
    parser = argparse.ArgumentParser(description='QHP-HAP transfer agent')
    parser.add_argument('-q', '--qhp', action='store', dest='qhp', help='QHP endpoint', required=True)
    parser.add_argument('-a', '--hap', action='store', dest='hap', help='HAP endpoint', required=True)
    parser.add_argument('-f', '--from', action='store', dest='from_', help='Starting date', required=True)
    parser.add_argument('-t', '--to', action='store', dest='to', help='Ending date', required=True)
    parser.add_argument('-p', '--period', action='store', dest='period', help='Timeframe', required=True)
    parser.add_argument('-d', '--time-delta', action='store', dest='time_delta', help='Add given time delta (in seconds)')
    parser.add_argument('-z', '--timezone', action='store', dest='timezone', help='Timezone')
    parser.add_argument('-b', '--blacklist-file', action='store', dest='blacklist_file', help='File with blacklisted tickers')

    args = parser.parse_args()

    start_time = datetime.datetime.strptime(args.from_, "%Y%m%d")
    end_time = datetime.datetime.strptime(args.to, "%Y%m%d")

    ctx = zmq.Context.instance()
    qhp = ctx.socket(zmq.REQ)
    qhp.connect(args.qhp)

    hap = ctx.socket(zmq.REQ)
    hap.connect(args.hap)

    tickers = request_ticker_list(qhp)

    tz = dateutil.tz.gettz('UTC')
    if args.timezone is not None:
        tz = dateutil.tz.gettz(args.timezone)

    timedelta = datetime.timedelta(seconds=0)
    if args.time_delta is not None:
        timedelta = datetime.timedelta(seconds=int(args.time_delta))

    blacklist = []
    if args.blacklist_file is not None:
        blacklist = load_blacklist(args.blacklist_file)
            
        
    max_retries = 3
    for ticker in tickers:
        for trynum in range(0, max_retries):
            if allow_ticker(blacklist, ticker):
                print("Requesting ticker from QHP: {}".format(ticker))
                data = get_data(qhp, ticker, start_time, end_time, args.period, tz, timedelta)
                if data is not None:
                    if len(data) > 0:
                        upload_data(hap, data, convert_ticker(ticker, data), args.period)
                    break
                else:
                    print("Timeout, retry {} of {}".format(trynum + 1, max_retries))
            else:
                print("Skipping blacklisted ticker: {}".format(ticker))
                

if __name__ == '__main__':
    main()
