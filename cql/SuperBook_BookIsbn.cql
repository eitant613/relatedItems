WITH {fileName} AS url
CALL apoc.load.json(url) YIELD value
UNWIND value.docs AS row

MATCH (n:Book{id:row.id})
WITH n, row
UNWIND row.ISBN as isbn
    MATCH (n)-[:HAS_SUPER]->(s:SuperBook)
    MATCH (i:Isbn {isbn:isbn})
    MERGE (s)-[:HAS_ISBN]->(i)
WITH n,row
UNWIND row.EISBN as eisbn
    MATCH (n)-[:HAS_SUPER]->(s:SuperBook)
    MATCH (e:Eisbn {eisbn:eisbn})
    MERGE (s)-[:HAS_EISBN]->(e)
