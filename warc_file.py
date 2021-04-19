import json
import requests
from warcio import ArchiveIterator

warc_url = 'https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-24/segments/' \
           '1590347387219.0/warc/CC-MAIN-20200525032636-20200525062636-00381.warc.gz'
wet_url = warc_url.replace('/warc/', '/wet/').replace('warc.gz', 'warc.wet.gz')
wat_url = warc_url.replace('/warc/', '/wat/').replace('warc.gz', 'warc.wat.gz')

r = requests.get(warc_url, stream=True)
records = ArchiveIterator(r.raw)

record = next(records)

print(record.rec_type)
print('--------------------------------------')

a = record.content_stream().read()

print(a.decode('utf-8'))
'''
print('**************************************')
print('details about the request to the server')
print('**************************************')
record = next(records)

print(record.rec_type)
print('--------------------------------------')
print(record.rec_headers)
print('--------------------------------------')
print(record.rec_headers.headers)
print('--------------------------------------')
print(record.http_headers)
print('--------------------------------------')
print(record.http_headers.headers)
print('--------------------------------------')

a = record.content_stream().read()
print(a)
'''
r = requests.get(wat_url, stream=True)
records = ArchiveIterator(r.raw)

record = next(records)
print(record.rec_type)
a = record.content_stream().read()
print(a.decode('utf-8'))
record = next(records)
print(record.rec_type)
print(record.rec_headers.headers)
print(record.http_headers)
a = record.content_stream().read()
data = json.loads(a.decode('utf-8'))
print(data)