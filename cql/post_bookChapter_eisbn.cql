call apoc.periodic.iterate(
'MATCH (c:BookChapter)-[:HAS_EISBN]->(e:Eisbn)<-[:HAS_EISBN]-(s:SuperBook) return s,c',
'MERGE (s)-[:HAS_CHAPTER]->(c)',
{batchSize:50, iterateList:true, parallel:false})