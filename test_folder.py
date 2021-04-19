#! /usr/bin/env python3


from lxml import etree as ET
import xml.etree.ElementTree as ET1
import pickle
import solr


def data_loader():
    GC = 'gc.xml'
    # GC = '/mss4/db/ena/genome_collections/gc.xml'
    direct_fields = ('TITLE', 'DESCRIPTION', 'NAME', 'ASSEMBLY_LEVEL', 'GENOME_REPRESENTATION')

    content = ET.iterparse(GC, events=('end',))

    j = 0
    for event, elem in content:
        if elem.tag == 'ASSEMBLY':
            assembly = {}
            j += 1
            assembly['id'] = 'home/dasha/example/solr-8.8.0/test/doc' + str(j)
            for (k, v) in elem.attrib.items():
                assembly[k] = v

            assembly['ids'] = []
            for i in elem.find('IDENTIFIERS').findall('*'):
                assembly['ids'].append(i.text)

            for d in direct_fields:
                try:
                    assembly[d.lower()] = elem.find(d).text
                except AttributeError:
                    pass

            if elem.find('TAXON') is not None:
                assembly['taxon'] = {}
                for d in elem.find('TAXON').findall('*'):
                    assembly['taxon'][d.tag.lower()] = d.text

            for d in ('SAMPLE_REF', 'STUDY_REF'):
                if elem.find(d) is not None:
                    assembly[d.lower()] = []
                    for s in elem.find(f"{d}/IDENTIFIERS"):
                        assembly[d.lower()].append(s.text)

            if elem.find('WGS_SET') is not None:
                assembly['wgs_set'] = {'prefix': elem.find('WGS_SET/PREFIX').text,
                                       'version': elem.find('WGS_SET/VERSION').text}

            assembly['cromosomes'] = []
            for chr in elem.findall('CHROMOSOMES/CHROMOSOME'):
                cur_chr = {}
                for c in chr.findall('*'):
                    cur_chr[c.tag.lower()] = c.text
                assembly['cromosomes'].append(cur_chr)

            assembly['assembly_links'] = []
            for l in elem.findall('ASSEMBLY_LINKS/ASSEMBLY_LINK/URL_LINK'):
                assembly['assembly_links'].append({'label': l.find('LABEL').text, 'url': l.find('URL').text})

            assembly['assembly_attributes'] = {}
            for a in elem.findall('ASSEMBLY_ATTRIBUTES/ASSEMBLY_ATTRIBUTE'):
                assembly['assembly_attributes'][a.find('TAG').text] = a.find('VALUE').text

            yield assembly
            elem.clear()


def main():
    i = 0
    n = 0

    s = solr.Solr('http://localhost:8983/solr/test-gc')

    for a in data_loader():

        i += 1

        add = ET.Element('add')
        doc = ET.SubElement(add, 'doc')
        ET.SubElement(doc, 'field', name='isParent').text = '1'
        # assembly_set = ET.SubElement(doc, 'assembly_set')
        # assembly_set = ET.Element('assembly_set')
        # ET.SubElement(assembly_set, 'field', name='id').text = '/home/dasha/example/solr-8.8.0/test/file' + str(i) \
        #                                                        + '.xml'
        # assembly = ET.SubElement(assembly_set, 'assembly', accession=a['accession'], alias=a['alias'],
        #                          center_name=a['center_name'])

        ET.SubElement(doc, 'field', name='id').text = a['accession']
        ET.SubElement(doc, 'field', name='accession').text = a['accession']
        ET.SubElement(doc, 'field', name='alias').text = a['alias']
        ET.SubElement(doc, 'field', name='center_name').text = a['center_name']
        ET.SubElement(doc, 'field', name='title').text = a['title']
        ET.SubElement(doc, 'field', name='description').text = a['description']
        ET.SubElement(doc, 'field', name='name').text = a['name']
        ET.SubElement(doc, 'field', name='assembly_level').text = a['assembly_level']
        ET.SubElement(doc, 'field', name='genome_representation').text = a['genome_representation']
        # taxon = ET.SubElement(doc, 'field', name='taxon')
        # doc1 = ET.SubElement(taxon, 'doc')
        # ET.SubElement(doc1, 'field', name='id').text = a['taxon']['taxon_id']
        ET.SubElement(doc, 'field', name='taxon_id').text = a['taxon']['taxon_id']
        ET.SubElement(doc, 'field', name='scientific_name').text = a['taxon']['scientific_name']
        if 'common_name' in a['taxon']:
            ET.SubElement(doc, 'field', name='common_name').text = a['taxon']['common_name']
        if 'sample_ref' in a:
            for k in a['sample_ref']:
                ET.SubElement(doc, 'field', name='sample_ref').text = a['sample_ref'][0]
        if 'study_ref' in a:
            ET.SubElement(doc, 'field', name='study_ref').text = a['study_ref'][0]
        if 'wgs_set' in a:
            wgs_set = ET.SubElement(doc, 'field', name='wgs_set')
            doc2 = ET.SubElement(wgs_set, 'doc')
            ET.SubElement(doc2, 'field', name='id').text = a['accession'] + 'wgs_set' + a['wgs_set']['prefix']
            ET.SubElement(doc2, 'field', name='prefix').text = a['wgs_set']['prefix']
            ET.SubElement(doc2, 'field', name='version').text = a['wgs_set']['version']

        chromosomes = ET.SubElement(doc, 'field', name='chromosomes')
        doc3 = ET.SubElement(chromosomes, 'doc')
        ET.SubElement(doc3, 'field', name='id').text = a['accession'] + 'chromosomes'
        n = 1
        for k in a['cromosomes']:
            chromosome = ET.SubElement(doc3, 'field', name='chromosome')
            doc3n = ET.SubElement(chromosome, 'doc')
            ET.SubElement(doc3n, 'field', name='id').text = a['accession'] + 'chromosome' + str(n)
            n += 1
            if 'name' in k:
                ET.SubElement(doc3n, 'field', name='name').text = k['name']
            if 'type' in k:
                ET.SubElement(doc3n, 'field', name='type').text = k['type']

        assembly_links = ET.SubElement(doc, 'field', name='assembly_links')
        doc4 = ET.SubElement(assembly_links, 'doc')
        ET.SubElement(doc4, 'field', name='id').text = a['accession'] + 'assembly_links'
        n = 1
        for k in a['assembly_links']:
            assembly_link = ET.SubElement(doc4, 'field', name='assembly_link')
            doc4n = ET.SubElement(assembly_link, 'doc')
            ET.SubElement(doc4n, 'field', name='id').text = a['accession'] + 'assembly_link' + str(n)
            n += 1
            ET.SubElement(doc4n, 'field', name='label').text = k['label']
            ET.SubElement(doc4n, 'field', name='url').text = k['url']

        assembly_attributes = ET.SubElement(doc, 'field', name='assembly_attributes')
        doc5 = ET.SubElement(assembly_attributes, 'doc')
        ET.SubElement(doc5, 'field', name='id').text = a['accession'] + 'assembly_attributes'
        n = 1
        for k in a['assembly_attributes']:
            assembly_attribute = ET.SubElement(doc5, 'field', name='assembly_attribute')
            doc5n = ET.SubElement(assembly_attribute, 'doc')
            ET.SubElement(doc5n, 'field', name='id').text = a['accession'] + 'assembly_attribute' + str(n)
            n += 1
            ET.SubElement(doc5n, 'field', name='tag').text = k
            ET.SubElement(doc5n, 'field', name='value').text = a['assembly_attributes'][k]
        '''
        if 'sample_ref' in a:
            sample_ref = ET.SubElement(doc, 'field', name='sample_ref')
            doc2 = ET.SubElement(sample_ref, 'doc')
            identifiers = ET.SubElement(doc2, 'field', name='identifiers')
            doc02 = ET.SubElement(identifiers, 'doc')
            ET.SubElement(doc02, 'field', name='id').text = a['sample_ref'][0]
        if 'study_ref' in a:
            study_ref = ET.SubElement(doc, 'field', name='study_ref')
            doc3 = ET.SubElement(study_ref, 'doc')
            identifiers = ET.SubElement(doc3, 'field', name='identifiers')
            doc03 = ET.SubElement(identifiers, 'doc')
            ET.SubElement(doc03, 'field', name='id').text = a['study_ref'][0]
        '''
        # identifiers = ET.SubElement(assembly, 'identifiers')
        # ET.SubElement(identifiers, 'primary_id').text = a['accession']
        # ET.SubElement(identifiers, 'submitter_id', namespace=a['center_name']).text = a['name']
        # ET.SubElement(assembly, 'title').text = a['title']
        # ET.SubElement(assembly, 'description').text = a['description']
        # ET.SubElement(assembly, 'name').text = a['name']
        # ET.SubElement(assembly, 'assembly_level').text = a['assembly_level']
        # ET.SubElement(assembly, 'genome_representation').text = a['genome_representation']
        # taxon = ET.SubElement(assembly, 'taxon')
        # ET.SubElement(taxon, 'taxon_id').text = a['taxon']['taxon_id']
        # ET.SubElement(taxon, 'scientific_name').text = a['taxon']['scientific_name']
        '''
        if 'common_name' in a['taxon']:
            ET.SubElement(taxon, 'common_name').text = a['taxon']['common_name']
        if 'sample_ref' in a:
            sample_ref = ET.SubElement(assembly, 'sample_ref')
            identifiers = ET.SubElement(sample_ref, 'identifiers')
            ET.SubElement(identifiers, 'primary_id').text = a['sample_ref'][0]
        if 'study_ref' in a:
            study_ref = ET.SubElement(assembly, 'study_ref')
            identifiers = ET.SubElement(study_ref, 'identifiers')
            ET.SubElement(identifiers, 'primary_id').text = a['study_ref'][0]
        chromosomes = ET.SubElement(assembly, 'chromosomes')
        for k in a['cromosomes']:
            chromosome = ET.SubElement(chromosomes, 'chromosome')
            if 'name' in k:
                ET.SubElement(chromosome, 'name').text = k['name']
            if 'type' in k:
                ET.SubElement(chromosome, 'type').text = k['type']
        assembly_links = ET.SubElement(assembly, 'assembly_links')
        for k in a['assembly_links']:
            assembly_link = ET.SubElement(assembly_links, 'assembly_link')
            url_link = ET.SubElement(assembly_link, 'url_link')
            ET.SubElement(url_link, 'label').text = k['label']
            ET.SubElement(url_link, 'url').text = k['url']
        assembly_attributes = ET.SubElement(assembly, 'assembly_attributes')
        for k in a['assembly_attributes']:
            assembly_attribute = ET.SubElement(assembly_attributes, 'assembly_attribute')
            ET.SubElement(assembly_attribute, 'tag').text = k
            ET.SubElement(assembly_attribute, 'value').text = a['assembly_attributes'][k]
        '''
        with open('file.xml', 'w') as f:
            f.write('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>')
            # ET.ElementTree(assembly_set).write(f)  # , encoding="unicode"

        # ET.SubElement(doc, 'field', name='content').text = text
        with open('file.xml', 'wb') as f:
            ET.ElementTree(add).write(f)
            # f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
            # pickle.dump(a, f)

        s.add(f, commit=True)
        f.close()
        '''
        print(a['ids'])
        print(a['accession'])
        print(a['name'])
        print(a['title'])
        print(a['assembly_level'])
        print(a['genome_representation'])
        print(a['description'])
        print(a['cromosomes'])
        print(a['taxon'])
        print(a['taxon']['scientific_name'])
        print(a['assembly_links'])
        print(a['assembly_attributes'])
        print(a['sample_ref'])
        # print(a['wgs_set'])
        print('----------------------------------')
        '''
        '''
        for key in a:
            print(key, '->', a[key])
        print('----------------------------------')
        '''
        # print(a['accession'])
        # print(a)
        print(i)
        '''
        i += 1
        filename = format(str(i) + ".xml")
        with open(filename, 'wb') as f:
            # f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
            pickle.dump(a, f)
        '''

if __name__ == "__main__":
    main()

# import os
# print(os.getcwd())
