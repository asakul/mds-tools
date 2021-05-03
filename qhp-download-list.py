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
    parser.add_argument('-q', '--qhp', action='store', dest='qhp', help='QHP endpoint', required=True)

    args = parser.parse_args()

    ctx = zmq.Context.instance()
    s = ctx.socket(zmq.REQ)
    s.connect(args.qhp)

    rq = {
        "get_sec_list" : True,
    }

    s.send_multipart([bytes(json.dumps(rq), "utf-8")])
    resp = s.recv()

    if resp != b'OK':
        errmsg = s.recv_string()
        print("Error:", errmsg)
        sys.exit(1)


    rawdata = b''
    while True:
        if s.getsockopt(zmq.RCVMORE) == 0:
            break
        rawdata += s.recv()

    s = rawdata.decode('utf-8')
    tickers = s.split(',')
    for ticker in tickers:
        print(ticker)

if __name__ == '__main__':
    main()

