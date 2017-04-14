import boto3
import argparse
import urllib
import random
import time


def generate(source, tgt_bucket, tgt_key, wait=0):
    # Get source file
    urllib.urlretrieve(source, './source')
    f = open('./source', 'r')
    lines = [l.rstrip('\n') for l in f.readlines()]
    
    if wait==0:
        record = random.choice(lines)
    
        s3 = boto3.resource('s3')
        s3_obj = s3.Object(tgt_bucket, tgt_key)
        s3_obj.put(Body=record)        
        print "successfully wrote record to {}/{}".format(tgt_bucket, tgt_key)

    else:
        while True:
            record = random.choice(lines)
        
            s3 = boto3.resource('s3')
            s3_obj = s3.Object(tgt_bucket, tgt_key)
            s3_obj.put(Body=record)
            print "successfully wrote record to {}/{}".format(tgt_bucket, tgt_key)
            
            time.sleep(wait)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='s3 source file')
    parser.add_argument('tgt_bucket', help='s3 target bucket')
    parser.add_argument('tgt_key', help='s3 target key')
    parser.add_argument('wait', default=0, help='time in seconds to wait between sending records')
    args = parser.parse_args()

    generate(args.source, args.tgt_bucket, args.tgt_key, args.wait)
