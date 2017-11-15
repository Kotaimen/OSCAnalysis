# -*- encoding: utf-8 -*-

import boto3
import time
import os
import configparser

# use requests bundled with boto3 so we don't need package lambda function
import botocore.vendored.requests as requests

print('Loading function')

OSC_STATE_ROOT = 'http://planet.openstreetmap.org/replication/'


def get_latest_sequence(root_url, interval='minute'):
    """#Fri Nov 10 03:02:09 UTC 2017
    sequenceNumber=45236
    timestamp=2017-11-10T03\:00\:00Z
    """
    parser = configparser.ConfigParser()
    response = requests.get('%s%s/state.txt' % (root_url, interval))

    # HACK: pretend its a config file
    config_file = '[state]\n%s' % response.text
    parser.read_string(config_file)

    sequence = parser.getint('state', 'sequenceNumber')

    timestamp = parser.get('state', 'timestamp').replace(r'\:', r':')

    print('Sequence #%d at %s' % (sequence, timestamp))

    return sequence, timestamp


def write_state(root_url, interval, table_name, sequence, timestamp):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    # path format is 000/000/000
    path = '%09d' % sequence
    path = '/'.join([path[:3], path[3:6], path[6:]])

    print('Writing #%d at %s' % (sequence, timestamp))

    table.put_item(
        Item={
            'seq': sequence,
            'timestamp': timestamp,
            'url': '%s%s/%s.osc.gz' % (root_url, interval, path),
            'ttl': int(time.time())
        },
        # prevent overwrite existing item
        ConditionExpression="attribute_not_exists(seq)"
    )


def lambda_handler(event, context):

    INTERVAL = 'minute'
    sequence, timestamp = get_latest_sequence(OSC_STATE_ROOT, interval=INTERVAL)

    table_name = os.getenv('TABLE_NAME', None)
    assert table_name is not None

    write_state(OSC_STATE_ROOT, INTERVAL, table_name,
                sequence, timestamp)
