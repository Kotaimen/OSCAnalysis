# -*- encoding: utf-8 -*-
import os
import tempfile
import json
import configparser
import boto3

# Use requests bundled with boto3 so we don't need package lambda function
try:
    import requests
except ImportError:
    import botocore.vendored.requests as requests

print('Loading function')

# === Constants ===
OSM_REPLICATOIN_ROOT = 'http://planet.openstreetmap.org/replication'
S3_PREFIX = 'openstreetmap/replication'

# === Globals ===
s3 = boto3.client('s3')
session = requests

bucket_name = os.getenv('BUCKET_NAME')
assert bucket_name is not None


def get_latest_sequence(root_url, interval):
    """#Fri Nov 10 03:02:09 UTC 2017
    sequenceNumber=45236
    timestamp=2017-11-10T03\:00\:00Z
    """
    parser = configparser.ConfigParser()
    state_url = '{}/{}/state.txt'.format(root_url, interval)
    response = session.get(state_url)

    # HACK: pretend its a config file
    config_file = '[state]\n{}'.format(response.text)
    parser.read_string(config_file)

    sequence = parser.getint('state', 'sequenceNumber')

    # replace "\:" in state.txt so we got proper timestamp
    timestamp = parser.get('state', 'timestamp').replace(r'\:', r':')

    print('Interval {} sequence #{} at {}'.format(interval, sequence, timestamp))

    return sequence, timestamp


def upload_url_to_s3(session, url, bucket, key):
    with tempfile.SpooledTemporaryFile() as fp:
        print('Downloading from {}'.format(url))
        response = session.get(url, stream=True)
        for chunk in response.iter_content(chunk_size=128):
            fp.write(chunk)
        fp.seek(0)
        print('Uploading to s3://{}/{}'.format(bucket, key))
        s3.upload_fileobj(fp, bucket, key)


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event))

    interval = event['interval']
    # bucket_name = event['bucket']

    sequence, timestamp = \
        get_latest_sequence(OSM_REPLICATOIN_ROOT, interval=interval)

    # path schema is 000/000/000
    path = '%09d' % sequence
    path = '/'.join([path[:3], path[3:6], path[6:]])

    # prepare resources
    osc_url = '{}/{}/{}.osc.gz'.format(OSM_REPLICATOIN_ROOT, interval, path)
    osc_key = '{}/{}/{}.osc.gz'.format(S3_PREFIX, interval, path)

    upload_url_to_s3(session, osc_url, bucket_name, osc_key)
