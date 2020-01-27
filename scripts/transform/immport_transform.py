mappings = {
"Accession": "identifier",
"Title": "name",
"Start Date": "datePublished",
"Detailed Description": "description",
"_id": "url",
"PI": lambda value {
            "creator": [{
                "@type": "Person",
                "name": individual.split(" - ")[0],
                "affiliation": individual.split(" - ")[1]
            } for individual in value.split('; '),
"Condition Studied": lambda value {
            "keywords": value.split(', ')
            },  # Could possibly go into variableMeasured, but is more subjectMeasured than variable...
"DOI": lambda value {
"sameAs": f"https://www.doi.org/{value}"
},
"Download Packages": lambda value {
            "distribution": [{
                "@type": "DataDownload",
                "contentUrl": value}],
"Contract/Grant": lambda value {
            "funder": [{
                "@type": "Organization",
                "name": value
            }],
"Pubmed Id": lambda value {
"citation": < call citation function over Pubmed ID array; also get funding info >
}
# --- ignored ---
# "Gender Included",
# "Subjects Number",
# "Data Completeness",
# "Brief Description",
# "Objectives",
# "Endpoints",
}
