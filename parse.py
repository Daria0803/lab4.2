#! /usr/bin/env python3


from lxml import etree as ET
import pickle

def data_loader():
	GC = 'gc.xml'
	# GC = '/mss4/db/ena/genome_collections/gc.xml'
	direct_fields = ('TITLE', 'DESCRIPTION', 'NAME', 'ASSEMBLY_LEVEL', 'GENOME_REPRESENTATION')

	content = ET.iterparse(GC, events=('end',))

	for event, elem in content:
		if elem.tag == 'ASSEMBLY':
			assembly = {}
			for (k,v) in elem.attrib.items():
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
				assembly['wgs_set'] = {'prefix': elem.find('WGS_SET/PREFIX').text, 'version': elem.find('WGS_SET/VERSION').text}

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

	for a in data_loader():
		for key in a:
		    print(key, '->', a[key])
		print('-----------------')
		# for i in a['sample_ref']:
			# print(i)
		print(a['cromosomes'])
		# print(a['sample_ref'])
		# print(a['accession'])
		# print(a['title'])
		# print(a)
	
if __name__ == "__main__":
    main()


