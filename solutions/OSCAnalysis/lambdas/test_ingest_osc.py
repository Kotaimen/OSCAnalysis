# -*- encoding: utf-8 -*-

import os
import sys
import json

import boto3

import requests
import urllib3
import requests_mock
import moto

# insert lambda package path
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'ingest_osc'))

# inject lambda envrionment variables
os.environ['BUCKET_NAME'] = 'fatray'

from ingest_osc import lambda_handler

STATE_TXT = '''#Fri Nov 10 03:02:09 UTC 2017
sequenceNumber=1
timestamp=2000-1-11T17\:31\:00Z
'''
HOUR_ONE = open('test_data/hour001.osc.gz', 'rb').read()


@moto.mock_s3
@requests_mock.Mocker(kw='mock')
def test_lambda_handler(**args):
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='fatray')

    args['mock'].get(
        'http://planet.openstreetmap.org/replication/hour/state.txt',
        text=STATE_TXT)
    args['mock'].get(
        'http://planet.openstreetmap.org/replication/hour/000/000/001.osc.gz',
        content=HOUR_ONE)

    lambda_handler(
        dict(interval='hour'),
        {}
    )
