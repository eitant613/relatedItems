from neo4j import GraphDatabase
import logging
import os
import logging
import time
import pprint
import traceback
import sys



booksPathWin = "c:\\eitan\\"
booksPath = "/pc_test_data/eldad/neo4j/data/related/books/"
bookChapPath = "/pc_test_data/eldad/neo4j/data/related/book_chapters/"
bookRevPath = "/pc_test_data/eldad/neo4j/data/related/book_reviews/"
uri = "bolt://bo0001.qos.pc.dc04.hosted.exlibrisgroup.com:7687"
user = "neo4j"
password = "qos"
filesPerDir = 100
errorCount = 0
maxError = 20

logging.basicConfig(filename='import.log',level=logging.INFO)

class importToDb(object):

    def __init__(self, uri, user, password):
        try:
            logging.info('Starting init')
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
        except Exception as e:
            print("ERROR: Failed to connect to Neo4j " + uri + " "+user+"/"+password)
            logging.error("ERROR: Failed to connect to Neo4j " + uri + " "+user+"/"+password)
            raise e

    def close(self):
        self._driver.close()
        logging.debug('Closed')

    def openSession(self, cypher,message):
        with self._driver.session() as session:
            sessionWrite = session.write_transaction(self._write_data, cypher, message)

    def readDir(self,path):
        fileList = []
        for root, dirs, files in os.walk(path+"."):
            for filename in files:
                 if (not filename.endswith("done")):
                    logging.info('Reading File: '+filename)
                    return(filename)
                    #fileList.append(filename)
        logging.info('No more files in: ' + booksPath)

    def processBooks(self, dbSession, importCql, importISBNCql, filePath, type, fileString):
        #fileString can be removed when moving to Linux
        global errorCount
        try:
            file = open(importCql, 'r')
            cypherImport = " ".join(file.readlines())
            file.close()
            file = open(importISBNCql, 'r')
            cypherISBN = " ".join(file.readlines())
            file.close()
        except Exception as e:
            print("ERROR: Missing or invalid CQL file")
            logging.error("ERROR: Missing or invalid CQL file")
            file.close()
            raise e

        # fileList=test.readDir(filePath)
        # while (fileList):
        #print("Started to process "+type)
        logging.info("Started to process "+type)
        print("   ", end='') #for mark of what file is being processed
        for i in range(filesPerDir):
            fileList = fileString + str(i) + ".json"
            start = time.time()
            print('\r\r\r', end='')
            print("Processing "+type+"   "+str(i+1), end='')
            try:
                dbSession.openSession(cypherImport, "file:///" + filePath + fileList)
                dbSession.openSession(cypherISBN, "file:///" + filePath + fileList)
                end = time.time()
                logging.info(
                    "Time elapsed to process " + fileList + " " + type + " import:" + str(round((end - start) * 1000)) + " ms")
            except Exception as e:
                errorCount = errorCount + 1
                print("")
                print("ERROR: Failed to load or execute Cypher for "+type+" "+str(errorCount)+" "+str(maxError))
                logging.error("ERROR: Failed to load or execute Cypher for "+type+" "+str(errorCount)+" "+str(maxError))
                if ( errorCount > maxError ):
                    raise e
        print("")
            # os.rename(booksPathWin + fileList, booksPathWin + fileList + ".done")
            # fileList = test.readDir(booksPathWin)


    @staticmethod
    def _write_data(tx, cypher,message):
        #result = tx.run("create (n:eitan) set n.file={message} return n", message=message)
        result = tx.run(cypher, fileName=message)
        for item in result:
            print(item, sep='/n')
            logging.info(item)

if __name__ == "__main__":
    try:
        test = importToDb(uri, user, password)
        startBook = time.time()
        test.processBooks(test, "importBooks.cql", "importISBNBooks.cql", booksPath, "Books", "related_items_harvest_Book_")
        endBook = time.time()
        print("Total Elapsed Time for Book Import:" + str(round((endBook - startBook) * 1000)) + " ms")

        startChapter = time.time()
        test.processBooks(test, "importBookChapters.cql", "importISBNChapters.cql", bookChapPath, "Book Chapters", "related_items_harvest_BookChapter_")
        endChapter = time.time()
        print("Total Elapsed Time for Chapter Import:" + str(round((endChapter - startChapter) * 1000)) + " ms")

        startReview = time.time()
        test.processBooks(test, "importBookReviews.cql", "importISBNReviews.cql", bookRevPath, "Book Reviews", "related_items_harvest_BookReview_")
        endReview = time.time()
        print("Total Elapsed Time for Review Import:" + str(round((endReview - startReview) * 1000)) + " ms")

        logging.info("Total Elapsed Time for Book Import:" + str(round((endBook - startBook) * 1000)) + " ms")
        logging.info("Total Elapsed Time for Chapter Import:" + str(round((endChapter - startChapter) * 1000)) + " ms")
        logging.info("Total Elapsed Time for Review Import:" + str(round((endReview - startReview) * 1000)) + " ms")

    except Exception:
        logging.exception('Related Items Import Failed')
        sys.exit('ERROR: Related Items Import Failed');
        exit('ERROR: Related Items Import Failed')

