# -*- encoding: utf-8 -*-

import boto3
import io
import gzip
import json
import os
import xml.etree.cElementTree as etree
# use requests bundled with boto3 so we don't need package lambda function
import botocore.vendored.requests as requests

print('Loading function')


def parse_osc(fp):
    """ Document schema:
      - change: one of modify|delete|create
      - feature: one of node|way|relation
      - id: big int
      - version: big int
      - timestamp: "2012-09-12T23:49:25Z"
      - uid: int
      - user: string
      - changeset: int
      - tags: map of tags
      - lon: float (only on nodes)
      - lat: float (only on nodes)
      - node_ref: array of referenced nodes (only on ways)
      - node_member: map of member->role (only on relations)
      - way_member: map of member->role (only on relations)
    """
    context = etree.iterparse(fp)
    for (event, change) in context:
        if change.tag not in ('modify', 'delete', 'create'):
            continue

        for feature in change:
            doc = dict(
                change=change.tag,
                feature=feature.tag,
                id=int(feature.attrib['id']),
                version=int(feature.attrib['version']),
                timestamp=feature.attrib['timestamp'].replace('T', ' ')[:-1],
                uid=int(feature.attrib['uid']),
                user=feature.attrib['user'],
                changeset=int(feature.attrib['changeset']),
            )

            doc['tags'] = dict((e.attrib['k'], e.attrib['v']) for e in
                               feature.findall('tag'))

            if feature.tag == 'node':
                doc['lon'] = float(feature.attrib['lon'])
                doc['lat'] = float(feature.attrib['lat'])
            elif feature.tag == 'way':
                doc['node_ref'] = list(int(e.attrib['ref']) for e in
                                       feature.findall('nd'))
            elif feature.tag == 'relation':
                doc['node_member'] = dict(
                    (int(e.attrib['ref']), e.attrib['role']) for e in
                    feature.findall("member[@type='node']"))
                doc['way_member'] = dict(
                    (int(e.attrib['ref']), e.attrib['role']) for e in
                    feature.findall("member[@type='way']"))
            yield doc
        else:
            change.clear()


def serialize_doc(doc):
    data = json.dumps(doc, ensure_ascii=True, indent=None).encode('ascii')
    data += b'\n'
    return data


BATCH_SIZE_LIMIT = 4 * 1024 * 1024 - 1000  # be safe


def make_batch(fp, batch_limit=500, size_limit=BATCH_SIZE_LIMIT):
    records = list()
    size = 0
    for n, doc in enumerate(parse_osc(fp)):

        data = serialize_doc(doc)

        size += len(data)

        records.append(
            dict(Data=data)
        )

        if (n % batch_limit) == 0 or (size > size_limit):
            print('Record batch #', n)
            yield records
            records = list()
            size = 0


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))

    stream_name = os.getenv('FIREHOSE_STREAM_NAME', None)
    assert stream_name is not None

    client = boto3.client('firehose')

    for record in event['Records']:
        print(record['eventID'], record['eventName'])
        url = record['dynamodb']['NewImage']['url']['S']

        print('Downloading from:', url)
        response = requests.get(url)

        with gzip.GzipFile(fileobj=io.BytesIO(response.content),
                           mode='r') as fp:
            for record_batch in make_batch(fp):
                client.put_record_batch(
                    DeliveryStreamName=stream_name,
                    Records=record_batch
                )

    return 'Successfully processed {} records.'.format(len(event['Records']))

# with gzip.GzipFile('sampledata/236.osc.gz', mode='r') as fp:
#     for b in make_batch(fp):
#         print(len(b))
