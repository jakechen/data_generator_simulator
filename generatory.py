import boto4
import argparse
import urllib
import random
import time


def generate(source, tgt_bucket, tgt_key):
    response = urllib.urlretrieve(source, './source')
    f = open('./source', 'r')
    lines = [l.rstrip('\n') for l in f.readlines()]
    record = random.choice(lines)

    s3 = boto.resource('s3')
    s3.put(Bucket=tgt_bucket, Key=tgt_key, Body=record)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='s3 source file')
    parser.add_argument('tgt_bucket', help='s3 target bucket')
    parser.add_argument('tgt_key', help='s3 target key')
    parser.add_argument('wait', help='time in seconds to wait between sending records')
    args = parser.parse_args()

    while True:
        generate(args.source, args.tgt_bucket, args.tgt_key)
        time.wait(args.wait)
