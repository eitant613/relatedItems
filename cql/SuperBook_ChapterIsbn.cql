WITH {fileName} AS url
CALL apoc.load.json(url) YIELD value
UNWIND value.docs AS row

MATCH (:BookChapter{id:row.id})-[:HAS_ISBN]->(i)<-[:HAS_ISBN]-(s:SuperBook)
with s, row
UNWIND row.ISBN as isbn
    match (i:Isbn {isbn:isbn})
    MERGE (s)-[:HAS_ISBN]->(i)
WITH s,row
UNWIND row.EISBN as eisbn
    match (e:Eisbn {eisbn:eisbn})
    MERGE (s)-[:HAS_EISBN]->(e)