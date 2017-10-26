import mxnet as mx
import numpy as np
import boto3
import argparse
import time

s3 = boto3.client('s3')

mnist = mx.test_utils.get_mnist()

def generate(sample_size, tgt_bucket, tgt_key, wait):
    X_test = mnist['test_data']

    if float(wait)==0.0:
        np.random.shuffle(X_test)
        X_test[:sample_size].tofile('./x_samples.csv', ',')
        s3.upload_file(
            './x_samples.csv',
            tgt_bucket,
            tgt_key
        )
        print('outputted file to s3://{}/{}'.format(tgt_bucket, tgt_key))

    else:
        while True:
            np.random.shuffle(X_test)
            X_test[:sample_size].tofile('./x_samples.csv', ',')
            s3.upload_file(
                './x_samples.csv',
                tgt_bucket,
                tgt_key
            )
            print('outputted file to s3://{}/{}'.format(tgt_bucket, tgt_key))
            
            time.sleep(float(wait))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('tgt_bucket', help='s3 target bucket')
    parser.add_argument('--tgt_key', 
                        default='x_samples.csv', 
                        help='s3 target key')
    parser.add_argument('--wait', 
                        type=float, 
                        default=0, 
                        help='time in seconds to wait between sending records, 0 means only send once')
    parser.add_argument('--sample_size', 
                        default=5, 
                        help='number of mnist samples to send')
    args = parser.parse_args()

    generate(args.sample_size, args.tgt_bucket, args.tgt_key, args.wait)
