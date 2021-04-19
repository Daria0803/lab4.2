#! /usr/bin/env python3


from lxml import etree as ET
import xml.etree.ElementTree as ET1
import pickle
# from __future__ import print_function
# import solr
import pysolr


def data_loader():
    # GC = 'gc.xml'
    GC = '/mss4/db/ena/genome_collections/gc.xml'
    direct_fields = ('TITLE', 'DESCRIPTION', 'NAME', 'ASSEMBLY_LEVEL', 'GENOME_REPRESENTATION')

    content = ET.iterparse(GC, events=('end',))

    j = 3
    for event, elem in content:
        if elem.tag == 'ASSEMBLY':
            assembly = {}
            j += 1
            assembly['id'] = '/home/dasha/example/solr-8.8.0/test/test_wet_7364/file' + str(j) + '.txt'
            for (k, v) in elem.attrib.items():
                assembly[k] = v
            # assembly['id'] = assembly['accession']

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
    z = 1
    # s = solr.Solr('http://localhost:8983/solr/test-gc')
    s = pysolr.Solr('http://localhost:8983/solr/test-gc')

    for a in data_loader():
        # print(a)
        x = []
        print(z)
        z += 1
        doc = dict(
            id=a["accession"],
            accession=a["accession"],
            alias=a["alias"],
            center_name=a["center_name"],
            title=a["title"],
            description=a["description"],
            name=a["name"],
            assembly_level=a["assembly_level"],
            genome_representation=a["genome_representation"],
            taxon_id=a["taxon"]["taxon_id"],
            scientific_name=a["taxon"]["scientific_name"],
        )
        test = [
          {
            "id": "1",
            "title": "Solr adds block join support",
            "content_type": "parentDocument",
            "comments": [{
                "id": "2",
                "content": "SolrCloud supports it too!"
              },
              {
                "id": "3",
                "content": "New filter syntax"
              }
            ]
          }
        ]
        if 'sample_ref' in a:
            doc['sample_ref'] = a["sample_ref"]
        if 'study_ref' in a:
            doc['study_ref'] = a["study_ref"]
        if 'common_name' in a['taxon']:
            doc['common_name'] = a["taxon"]["common_name"]

        a['isparent'] = 1
        a['taxon']['id'] = a['accession'] + 'taxon'
        a['assembly_attributes']['id'] = a['accession'] + 'assembly_attribute'
        if 'wgs_set' in a:
            a['wgs_set']['id'] = a['accession'] + 'wgs_set'

        n = 1
        for i in a['cromosomes']:
            i['id'] = a['accession'] + 'chromosome' + str(n)
            n += 1

        n = 1
        for i in a['assembly_links']:
            i['id'] = a['accession'] + 'assembly_link' + str(n)
            n += 1


        # doc['chromosomes'] = a['cromosomes']
        # print(doc)
        # s.add(doc, commit=True)
        a.pop('assembly_attributes', None)
        a.pop('ids', None)
        x.append(a)
        s.add(x, commit=True)

        '''
        for key in a:
            print(key, '->', a[key])
        print('----------------------------------')
        '''
        # solr.add([a])

        # add a document to the index
        # s.add(a)

        '''
        i += 1

        # add = ET.Element('add')
        # doc = ET.SubElement(add, 'doc')
        # assembly_set = ET.SubElement(doc, 'assembly_set')
        assembly_set = ET.Element('assembly_set')
        # ET.SubElement(assembly_set, 'field', name='id').text = '/home/dasha/example/solr-8.8.0/test/file' + str(i) \
        #                                                        + '.xml'
        assembly = ET.SubElement(assembly_set, 'assembly', accession=a['accession'], alias=a['alias'],
                                 center_name=a['center_name'])

        identifiers = ET.SubElement(assembly, 'identifiers')
        ET.SubElement(identifiers, 'primary_id').text = a['accession']
        ET.SubElement(identifiers, 'submitter_id', namespace=a['center_name']).text = a['name']
        ET.SubElement(assembly, 'title').text = a['title']
        ET.SubElement(assembly, 'description').text = a['description']
        ET.SubElement(assembly, 'name').text = a['name']
        ET.SubElement(assembly, 'assembly_level').text = a['assembly_level']
        ET.SubElement(assembly, 'genome_representation').text = a['genome_representation']
        taxon = ET.SubElement(assembly, 'taxon')
        ET.SubElement(taxon, 'taxon_id').text = a['taxon']['taxon_id']
        ET.SubElement(taxon, 'scientific_name').text = a['taxon']['scientific_name']
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

        with open('file' + str(i) + '.xml', 'w') as f:
            f.write('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>')
            # ET.ElementTree(assembly_set).write(f)  # , encoding="unicode"
            f.close()
        # ET.SubElement(doc, 'field', name='content').text = text
        with open('file' + str(i) + '.xml', 'wb') as f:
            ET.ElementTree(assembly_set).write(f)
            # f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
            # pickle.dump(a, f)
            f.close()
        '''

    # results = solr.search(q='genome')
    # print("Saw {0} result(s).".format(len(results)))

if __name__ == "__main__":
    main()
