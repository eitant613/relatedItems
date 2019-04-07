CALL apoc.periodic.iterate(
"MATCH (r:BookReview) where (r.orig_title_length<21) return r",
"MATCH (b:Book)-[:HAS_SUPER]->(s:SuperBook) WHERE (b.norm_title_length<21) and (b.norm_author in [r.norm_author1,r.norm_author2]) and (b.norm_title in [r.norm_title1,r.norm_title2]) MERGE (s)-[:HAS_BOOK_REVIEW]->(r)",
{batchSize:50, iterateList:true, parallel:true})