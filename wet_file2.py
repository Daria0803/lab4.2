import requests
from warcio import ArchiveIterator
import xml.etree.ElementTree as ET

f = open('test_wet.txt', 'w')
f.close()

# wet_url = 'https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2021-04/segments/1610704847953.98/wet/CC-MAIN-20210128134124-20210128164124-00799.warc.wet.gz'
wet_url = 'https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2021-04/segments/1610703495901.0/wet/CC-MAIN-20210115134101-20210115164101-00001.warc.wet.gz'
r = requests.get(wet_url, stream=True)
records = ArchiveIterator(r.raw)

record = next(records)
assert record.rec_type == 'warcinfo'


i = 0
add = ET.Element('add')

with open('file.xml', 'w') as f:
    f.write('<?xml version="1.0"?>')
    ET.ElementTree(add).write(f, encoding="unicode")

while i <= 2:
    # if next(records, 'STOP') == 'STOP':
    #    break
    # else:
        record = next(records, 'STOP')
        URL = record.rec_headers.get_header('WARC-Target-URI')
        text = record.content_stream().read().decode('utf-8')

        doc = ET.SubElement(add, 'doc')
        ET.SubElement(doc, 'field', name='URL').text = URL

        with open('file.xml', 'w') as f:
            ET.ElementTree(add).write(f, encoding="unicode")


        ET.SubElement(doc, 'field', name='content').text = text

        with open('file.xml', 'wb') as f:
            ET.ElementTree(add).write(f)

        i += 1
        print(i)
