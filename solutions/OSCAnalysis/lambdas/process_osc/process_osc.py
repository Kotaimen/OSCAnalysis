# -*- encoding: utf-8 -*-

import json
import tempfile
import gzip
import urllib.parse
import xml.etree.cElementTree as etree

import boto3

print('Loading function')

# === Globals ===
s3 = boto3.client('s3')


def split_changes(fp):
    """ Split OSC change into json documents

    OSC XML Schema:
        <change>
            <feature attrib=...>
                <tags attrib=... >
    Output doc schema:
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
                timestamp=feature.attrib['timestamp'],
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
    """Serialize JSON doc as utf-8"""
    # one liner json
    data = json.dumps(doc, ensure_ascii=True, indent=None)
    payload = b'%b\n' % data.encode('ascii')
    return payload


def make_batch(fp, batch_limit=500,
               batch_size_limit=4194304,
               record_size_limit=1024000):
    """ Make Firehose record batch, which must be either:
     - less than 500 records
     - less than 4MB in total size
     - each record must be smaller than 1000KB

    Batch schema:
        [
            {
                "Data": <payload>
            },
            ...
        ]
    """
    records = list()
    size = 0
    for n, doc in enumerate(split_changes(fp)):

        payload = serialize_doc(doc)
        payload_size = len(payload)

        if payload_size > record_size_limit:
            print('Drooping oversize record #%d (%d bytes)' % (n, payload_size))
            continue

        if (n > 0 and (n % batch_limit) == 0) or \
                (size + len(payload)) > batch_size_limit:
            print('Record #%d (%s bytes)' % (n, size))
            yield records
            records = list()
            size = 0

        records.append(
            dict(Data=payload)
        )
        size += payload_size
    else:
        print('Record #%d (%s)' % (n, size))
        yield records


def on_object_created(s3_object):
    with tempfile.TemporaryFile(mode='w+b') as fp:
        # XXX: lambda temporary disk size is is 512MB
        s3.download_fileobj(s3_object['bucket'], s3_object['key'], fp)
        fp.seek(0)
        with gzip.GzipFile(fileobj=fp, mode='r') as gzfp:
            for record_batch in make_batch(gzfp):
                pass


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event))

    for record in event['Records']:
        messages = json.loads(record['Sns']['Message'])

        for message in messages['Records']:

            if message['eventSource'] != 'aws:s3' or \
                    not message['eventName'].startswith('ObjectCreated:'):
                print('Skipdping non s3 object creation message %r' % message)
                continue

            payload = message['s3']['object']
            payload['bucket'] = message['s3']['bucket']['name']
            payload['key'] = urllib.parse.unquote_plus(payload['key'])

            print('Received s3 object creation event')
            print(payload)
            on_object_created(payload)
