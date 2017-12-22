# -*- encoding: utf-8 -*-

import moto
import os
import gzip

os.environ['FIREHOSE_STREAM_NAME'] = 'xray'

mock = moto.mock_kinesis()
mock.start()

from download_osc import make_batch, serialize_doc


def test_parse_osc_xml():
    with gzip.GzipFile(filename='test_data/000.osc.gz') as fp, \
            open('test_data/000_org.txt', 'wb') as ofp:
        for batch in make_batch(fp):
            for record in batch:
                ofp.write(record['Data'])


mock.stop()
