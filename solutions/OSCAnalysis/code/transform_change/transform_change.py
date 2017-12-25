# -*- encoding: utf-8 -*-
from __future__ import print_function

import base64
import json

print('Loading function')


def transform_doc(doc):

    doc['timestamp'] = doc['timestamp'].replace('T', ' ')[:-1]

    yield doc


def serialize_doc(doc):
    """Serialize JSON doc as utf-8"""
    data = json.dumps(doc, ensure_ascii=False, indent=None)
    payload = b'%b\n' % data.encode('utf-8')
    return payload


def lambda_handler(event, context):
    output = []

    for record in event['records']:
        print(record['recordId'])
        payload = base64.b64decode(record['data'])

        # Do custom processing on the payload here
        json_payload = json.loads(payload.decode('utf-8'))

        transformed_payload = b''.join(
            map(lambda doc: serialize_doc(doc),
                transform_doc(json_payload)))

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(transformed_payload).decode('ascii')
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}

