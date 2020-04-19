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
    if period == "1min":
        return 60
    elif period == "5min":
        return 60 * 5
    elif period == "15min":
        return 60 * 15
    elif period == "30min":
        return 60 * 30
    elif period == "hour":
        return 60 * 60
    elif period == "daily":
        return 86400

def main():
    parser = argparse.ArgumentParser(description='Finam quote downloader')
    parser.add_argument('-o', '--hap', action='store', dest='hap', help='HAP endpoint')
    parser.add_argument('-y', '--hap-symbol', action='store', dest='hap_symbol', help='HAP symbol')

    period = "15min"

    args = parser.parse_args()

    out_symbol = args.hap_symbol

    ctx = zmq.Context.instance()
    s = ctx.socket(zmq.REQ)
    s.connect(args.hap)
    serialized_bars = io.BytesIO()
    min_dt = None
    max_dt = None
    for i in range(0, 10):
        date = "2020418"
        time = "10{:02d}00".format(i)
        open_ = 1
        high = 2
        low = 3
        close = 4
        volume = 1200
        dt = datetime.datetime.strptime(date + "_" + time, "%Y%m%d_%H%M%S") - datetime.timedelta(hours=3) # Convert to UTC

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

    rq = {
        "ticker" : out_symbol,
        "start_time" : min_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "end_time" : max_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "timeframe_sec" : sec_from_period(period)
    }

    s.send_multipart([bytes(json.dumps(rq), "utf-8"), serialized_bars.getvalue()])
    parts = s.recv_multipart()
    print(parts)


if __name__ == '__main__':
    main()

