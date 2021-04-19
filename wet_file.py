import requests
import xmltodict
from warcio import ArchiveIterator
import xml.etree.ElementTree as ET

# warc_url = 'https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-24/segments/' \
#           '1590347387219.0/warc/CC-MAIN-20200525032636-20200525062636-00381.warc.gz'
# wet_url = warc_url.replace('/warc/', '/wet/').replace('warc.gz', 'warc.wet.gz')

f = open('test_wet.txt', 'w')
f.close()

# wet_url = 'https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2021-04/segments/1610704847953.98/wet/CC-MAIN-20210128134124-20210128164124-00799.warc.wet.gz'
wet_url = 'https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2021-04/segments/1610703495901.0/wet/CC-MAIN-20210115134101-20210115164101-00001.warc.wet.gz'
r = requests.get(wet_url, stream=True)
records = ArchiveIterator(r.raw)

record = next(records)
assert record.rec_type == 'warcinfo'
# skip the warcinfo
# record = next(records)
# This shows the source page, WARC-Target-URI and other metadata
# record.rec_headers.headers
#text = record.content_stream().read()

'''
f = open('test_wet.txt', 'wb')
f.write(text)
f.close()
'''
my_dict = {
    'response': {
        'status': 'good',
        'last_updated': '2014-02-16T23:10:12Z',
    }
}
''' Вывод URL'''
# record = next(records)
# print(record.rec_headers.headers[1][1])
# print(record.rec_headers.get_header('WARC-Target-URI'))
# !!!!!!!!!!!!!!!!!!!!!!!

# f = open('test_wet.txt', 'wb')
i = 0
while i <= 3:
    # if next(records, 'STOP') == 'STOP':
    #    break
    # else:
        # f = open('file'+str(i)+'.xml', 'w')
        record = next(records, 'STOP')
        URL = record.rec_headers.get_header('WARC-Target-URI')
        text = record.content_stream().read().decode('utf-8')

        add = ET.Element('add')
        doc = ET.SubElement(add, 'doc')
        ET.SubElement(doc, 'field', name='id').text = '/home/dasha/example/solr-8.8.0/test/test_wet_7364/file' + str(i)\
                                                      + '.txt'
        ET.SubElement(doc, 'field', name='url').text = URL
        # ET.SubElement(doc, 'field', name='content').text = text
        # tree = ET.ElementTree(add)
        with open('file' + str(i) + '.xml', 'w') as f:
            f.write('<?xml version="1.0"?>')
            ET.ElementTree(add).write(f, encoding="unicode")
            f.close()
        ET.SubElement(doc, 'field', name='content').text = text
        with open('file' + str(i) + '.xml', 'wb') as f:
            ET.ElementTree(add).write(f)
            f.close()
        # tree.write('file' + str(i) + '.xml')

        '''
        f = open('file' + str(i) + '.xml', 'w')
        f.write('<?xml version="1.0"?>')
        f.write('<add>')
        f.write('   <doc>')
        f.write('      <field name="URL">'+URL+'</field>')
        f.write('      <field name="content">')
        f.close()
        f = open('file' + str(i) + '.xml', 'wb')
        f.write(text)
        f.close()
        f = open('file' + str(i) + '.xml', 'w')
        f.write('      </field>')
        f.write('   </doc>')
        f.write('</add>')
        # f.write(text)
        f.close()
        '''
        i += 1
        print(i)


# !!!!!!!!!!!!!!!!!!!!!!!!
'''
print(text.decode('utf-8'))
print('-------------------------------------------------')
record = next(records)
record.rec_headers.headers
text = record.content_stream().read()
print(text.decode('utf-8'))
print('-------------------------------------------------')
if not next(records):
    record = next(records)
    record.rec_headers.headers
    text = record.content_stream().read()
    print(text.decode('utf-8'))
'''