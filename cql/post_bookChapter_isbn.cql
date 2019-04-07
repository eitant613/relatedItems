call apoc.periodic.iterate(
'MATCH (c:BookChapter)-[:HAS_ISBN]->(i:Isbn)<-[:HAS_ISBN]-(s:SuperBook) return s,c',
'MERGE (s)-[:HAS_CHAPTER]->(c)',
{batchSize:50, iterateList:true, parallel:false})