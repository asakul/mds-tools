#!/usr/bin/env python3

import sys
import argparse
import zmq
import io
import json
import csv
import datetime
import struct


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

def main():
    parser = argparse.ArgumentParser(description='Finam quote downloader')
    parser.add_argument('-i', '--input-file', action='store', dest='input_file', help='Input filename', required=True)
    parser.add_argument('-p', '--timeframe', action='store', dest='timeframe', help='Data timeframe', required=True)
    parser.add_argument('-o', '--hap', action='store', dest='hap', help='HAP endpoint', required=True)
    parser.add_argument('-y', '--hap-symbol', action='store', dest='hap_symbol', help='HAP symbol', required=True)
    parser.add_argument('-d', '--time-delta', action='store', dest='time_delta', help='Time delta (hours)')
    parser.add_argument('-f', '--force-from', action='store', dest='force_from', help='Force period start')
    parser.add_argument('-t', '--force-to', action='store', dest='force_to', help='Force period end')


    args = parser.parse_args()

    period = args.timeframe

    out_symbol = args.hap_symbol

    ctx = zmq.Context.instance()
    s = ctx.socket(zmq.REQ)
    s.connect(args.hap)
    serialized_bars = io.BytesIO()
    min_dt = None
    max_dt = None
    time_delta = datetime.timedelta(hours=0)
    if args.time_delta is not None:
        time_delta = datetime.timedelta(hours=int(args.time_delta))
        print('Applying delta:', time_delta)
    line_count = 0
    with open(args.input_file, 'r') as f:
        reader = csv.reader(f, delimiter=';')
        next(reader)
        for line in reader:
            line_count += 1
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

            dt = datetime.datetime(year, month, day, hour, minute, second, 0, datetime.timezone.utc) - time_delta

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

    rq = {
        "ticker" : out_symbol,
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


if __name__ == '__main__':
    main()

