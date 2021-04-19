import solr

# create a connection to a solr server
s = solr.Solr('http://localhost:8983/solr/test-gc')

# add a document to the index
doc = [
        {
            'ID': "1",
            "title": "Solr adds block join support",
            "content_type": "parentDocument",
            "comments": [{
                "ID": "2",
                "content": "SolrCloud supports it too!"
            },
                {
                    "ID": "3",
                    "content": "New filter syntax"
                }
            ]
        },
        {
            "ID": "4",
            "title": "New Lucene and Solr release is out",
            "content_type": "parentDocument",
            "_childDocuments_": [
                {
                    "ID": "5",
                    "comments": "Lots of new features"
                }
            ]
        }
]
s.add(doc, commit=True)

# do a search
# response = s.select('title:lucene')
# for hit in response.results:
#     print(hit['title'])
