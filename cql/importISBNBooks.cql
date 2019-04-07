WITH {fileName} AS url
CALL apoc.load.json(url) YIELD value
UNWIND value.docs AS row

MERGE (n:Book{id:row.id})
FOREACH (eisbn IN row.EISBN |
    MERGE (eisbn_n:Eisbn {eisbn:eisbn})
        ON CREATE SET
            eisbn_n.title_t=n.title_t,
            eisbn_n.author_t=n.author_t
    MERGE (n)-[r:HAS_EISBN]->(eisbn_n))
FOREACH (isbn IN row.ISBN |
    MERGE (isbn_n:Isbn {isbn:isbn})
        ON CREATE SET
            isbn_n.title_t=n.title_t,
            isbn_n.author_t=n.author_t
    MERGE (n)-[r:HAS_ISBN]->(isbn_n))

