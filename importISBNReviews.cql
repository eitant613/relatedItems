WITH {fileName} AS url
CALL apoc.load.json(url) YIELD value
UNWIND value.docs AS row

MERGE (n:Book_Review2{id:row.id})
FOREACH (eisbn IN row.EISBN |
    MERGE (eisbn_n:EISBN {eisbn:eisbn})
        ON CREATE SET
            eisbn_n.Title_t=n.Title_t,
            eisbn_n.Author_t=n.Author_t
    MERGE (n)-[r:HAS_EISBN]->(eisbn_n))
FOREACH (isbn IN row.ISBN |
    MERGE (isbn_n:ISBN {isbn:isbn})
        ON CREATE SET
            isbn_n.Title_t=n.Title_t,
            isbn_n.Author_t=n.Author_t
    MERGE (n)-[r:HAS_ISBN]->(isbn_n))
