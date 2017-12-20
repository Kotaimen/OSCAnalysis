# -*- encoding: utf-8 -*-
from __future__ import print_function

import base64
import json
import xml.etree.cElementTree as etree

print('Loading function')


def transform_change(xml_fragment):
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
    change = etree.fromstring(xml_fragment)

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


def lambda_handler(event, context):
    output = []

    for record in event['records']:
        print(record['recordId'])
        payload = base64.b64decode(record['data'])

        # Do custom processing on the payload here
        xml_fragment = payload.decode('utf-8')
        json_fragment = '\n'.join(
            map(lambda doc: json.dumps(doc, ensure_ascii=True, indent=None),
                transform_change(xml_fragment)))

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(
                json_fragment.encode('ascii')).decode('ascii')
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}
