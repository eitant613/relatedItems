WITH {fileName} AS url
CALL apoc.load.json(url) YIELD value
UNWIND value.docs AS row

MATCH (n:Book{id:row.id})
MERGE (s:SuperBook {title_t:n.title_t})
MERGE (s)<-[:HAS_SUPER]-(n)
