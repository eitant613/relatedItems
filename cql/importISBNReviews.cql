WITH {fileName} AS url
CALL apoc.load.json(url) YIELD value
UNWIND value.docs AS row

MERGE (n:BookReview{id:row.id})
FOREACH (eisbn IN row.EISBN |
    MERGE (eisbn_n:Eisbn {eisbn:eisbn})
    MERGE (n)-[r:HAS_EISBN]->(eisbn_n))
FOREACH (isbn IN row.ISBN |
    MERGE (isbn_n:Isbn {isbn:isbn})
    MERGE (n)-[r:HAS_ISBN]->(isbn_n))

