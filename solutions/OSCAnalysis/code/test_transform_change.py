# -*- encoding: utf-8 -*-

import os
import json

from transform_change import transform_doc, serialize_doc

def test_transfrom_change():

    with open('test_data/000_org.txt', 'r') as fp , open('test_data/000_transformed.txt', 'wb') as ofp:
        for line in fp:
            for doc in transform_doc(json.loads(line)):
                ofp.write(serialize_doc(doc))
