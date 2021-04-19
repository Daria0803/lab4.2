#! /usr/bin/env python3


import xml.etree.ElementTree as ET

GC = '/mss4/db/ena/genome_collections/gc.xml'
context = ET.iterparse(GC, events=('end', ))
index = 0
for event, elem in context:
    if elem.tag == 'ASSEMBLY':
        index += 1
        filename = format(str(index) + ".xml")
        with open(filename, 'wb') as f:
            f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
            f.write(ET.tostring(elem))
