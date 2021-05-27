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

from pytz import timezone

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


def get_month_code(month):
    if month < 1 or month > 12:
        return None
    codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
    return codes[month - 1]

def main():
    parser = argparse.ArgumentParser(description='Finam quote downloader')
    parser.add_argument('-i', '--input-file', action='store', dest='input_file', help='Input filename', required=True)
    parser.add_argument('-p', '--timeframe', action='store', dest='timeframe', help='Data timeframe', required=True)
    parser.add_argument('-o', '--hap', action='store', dest='hap', help='HAP endpoint', required=True)
    parser.add_argument('-y', '--hap-symbol', action='store', dest='hap_symbol', help='HAP symbol', required=True)
    parser.add_argument('-d', '--time-delta', action='store', dest='time_delta', help='Time delta (seconds)')
    parser.add_argument('-f', '--force-from', action='store', dest='force_from', help='Force period start')
    parser.add_argument('-t', '--force-to', action='store', dest='force_to', help='Force period end')
    parser.add_argument('-z', '--timezone', action='store', dest='timezone', help='Timestamps timezone')


    args = parser.parse_args()

    period = args.timeframe

    utc_tz = timezone('UTC')
    if args.timezone is None:
        tz = utc_tz
    else:
        tz = timezone(args.timezone)

    out_symbol = args.hap_symbol

    ctx = zmq.Context.instance()
    s = ctx.socket(zmq.REQ)
    s.connect(args.hap)
    serialized_bars = io.BytesIO()
    min_dt = None
    max_dt = None
    time_delta = datetime.timedelta(hours=0)
    if args.time_delta is not None:
        time_delta = datetime.timedelta(seconds=int(args.time_delta))
        print('Applying delta:', time_delta)
    line_count = 0
    ticker = None
    with open(args.input_file, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for line in reader:
            line_count += 1
            if ticker is None:
                ticker = line[0]
            elif ticker != line[0]:
                print('Different tickers in file, aborting')
                break
                
            date = line[2]
            time = line[3]
            open_ = line[4]
            high = line[5]
            low = line[6]
            close = line[7]
            volume = line[8]

            year = int(date[0:4])
            month = int(date[4:6])
            day = int(date[6:8])
            hour = int(time[0:2])
            minute = int(time[2:4])
            second = int(time[4:6])

            dt = datetime.datetime(year, month, day, hour, minute, second, 0, utc_tz).astimezone(tz) - time_delta

            dt = dt.astimezone(utc_tz)
            serialized_bars.write(struct.pack("<qddddQ", int(dt.timestamp()), float(open_), float(high), float(low), float(close), int(volume)))

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

    if args.force_from is not None:
        min_dt = datetime.datetime.strptime(args.force_from, "%Y%m%d")

    if args.force_to is not None:
        max_dt = datetime.datetime.strptime(args.force_to, "%Y%m%d")

    out_ticker = out_symbol

    if out_symbol[0] == '@':
        base = out_symbol[1:]
        matches = re.match('^([^-]+)-(\\d+)\\.(\\d+)$', ticker)
        if not matches:
            print('Invalid ticker id in file')
            return
        year_code = matches.group(3)[-1]
        month_code = get_month_code(int(matches.group(2)))

        out_ticker = base + month_code + year_code

    elif out_symbol[0] == '~':
        base = out_symbol[1:]
        matches = re.match('^([^-]+)-(\\d+)\\.(\\d+)$', ticker)
        if not matches:
            print('Invalid ticker id in file')
            return
        year_code = matches.group(3)
        month_code = matches.group(2)

        out_ticker = base + "-" + month_code + '.' + year_code

        print("Resulting ticker: {}".format(out_ticker))

    rq = {
        "ticker" : out_ticker,
        "start_time" : min_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "end_time" : max_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "timeframe_sec" : sec_from_period(period)
    }

    print("Read {} lines".format(line_count))
    raw_data = serialized_bars.getvalue()
    print("Sending {} bytes".format(len(raw_data)))

    s.send_multipart([bytes(json.dumps(rq), "utf-8"), raw_data])
    parts = s.recv_multipart()
    print("Response:", parts)
    return True


if __name__ == '__main__':
    ret = main()
    if ret is None:
        sys.exit(1)
    

