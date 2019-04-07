CALL apoc.periodic.iterate(
"MATCH (r:BookReview) where (r.orig_title_length>20) return r",
"MATCH (b:Book)-[:HAS_SUPER]->(s:SuperBook) WHERE (b.norm_title_length>20) and ((b.norm_title in [r.norm_title1,r.norm_title2]) OR (r.orig_title in [b.norm_title,b.orig_title])) MERGE (s)-[:HAS_BOOK_REVIEW]->(r)",
{batchSize:50, iterateList:true, parallel:true})
