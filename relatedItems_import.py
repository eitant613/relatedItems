from __future__ import print_function
from neo4j import GraphDatabase
import os
import logging
import time
import traceback
import sys
import yaml
import getopt
import threading
import shutil
from datetime import datetime

filesPerDir = 5
errorCount = 0
maxError = 20
errorCountTotal=0
processCount=0
maxThread=5
lastErrorTime=round(time.time())
startTime=time.time()


def setup_logger(logger_name, log_file, level, format, output):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter(format)
    fileHandler = logging.FileHandler(log_file, mode='a')
    fileHandler.setFormatter(formatter)
    if(output):
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(fileHandler)
    if (output):
        l.addHandler(streamHandler)
    return (logging.getLogger(logger_name))

logDir = './log/'
if not os.path.exists(logDir):
    os.makedirs(logDir)
logFileName = logDir+datetime.now().strftime('import_SB_%Y_%m_%d.log')
logger_1 = setup_logger('defaultLog', logFileName, logging.INFO, '%(asctime)s - %(levelname)s - %(message)s', True)
logFailedName = logDir+datetime.now().strftime('failedfiles_SB_%Y_%m_%d_%H%M.log')

logProgressName = logDir + datetime.now().strftime('progress_SB_%Y_%m_%d.log')
logProgress = setup_logger('logProgress', logProgressName, logging.INFO, '%(asctime)s - %(levelname)s - %(message)s', False)

class importToDb(object):

    def __init__(self, uri, user, password):
        try:
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
        except Exception as e:
            logger_1.error("ERROR: Failed to connect to Neo4j " + uri + " "+user+"/"+password)
            raise e

    def close(self):
        self._driver.close()
        logger_1.debug('Closed')

    #Process that will process the files according to input of the requried CQL's for processing, path to the files and file type.
    def processFiles(self, dbSession, cypherImport, cypherISBN, shortFileName, filePath, fileType):
        fileName=filePath+shortFileName
        logger_1.info("Started to process "+fileName)
        start = time.time()
        try:
            dbSession.openSession(cypherImport, "file:///" + fileName)
            dbSession.openSession(cypherISBN, "file:///" + fileName)
            incrementProcCount()
            end = time.time()
            logger_1.info("Time elapsed to process " + fileName + " import:" + str(round((end - start) * 1000)) + " ms")
            dst_dir=filePath+"done"
            if not os.path.exists(dst_dir):
                os.makedirs(dst_dir)
            shutil.copy2(fileName,dst_dir)
            if os.path.exists(dst_dir+"/"+shortFileName):
                os.remove(fileName)
            else:
               logger_1.error("Could not move file: "+fileName)
        except Exception as e:
            incrementErrCount()
            logger_1.error("ERROR: Failed to load or execute Cypher for "+fileName+" "+str(errorCount)+" "+str(maxError))
            try:
                failedFilesLog.info(fileName)
            except NameError:
                failedFilesLog = setup_logger('failedFileLog', logFailedName, logging.INFO, '%(message)s', False)
                failedFilesLog.info(fileName)
                if (errorCount > maxError):
                    raise e
                else:
                    pass
        finally:
            dbSession.closeSession()

    #Function that will open session to neo4j and call next function to execute the query
    def openSession(self, cypher,message):
        with self._driver.session() as session:
            sessionWrite = session.write_transaction(self._write_data, cypher, message)

    def closeSession(self):
        with self._driver.session() as session:
            session.close();


    #Staic method to execute the query and return results
    @staticmethod
    def _write_data(tx, cypher,message):
        result = tx.run(cypher, fileName=message)
        for item in result:
            logger_1.info(item)

#Will traverse the given directory and return the first file that was not processed yet.
def readDir(path):
    fileList = []
    for root, dirs, files in os.walk(path+"."):
        for filename in files:
             if ('nfs' not in filename):
                fileList.append(filename)
        del dirs[:]
    logger_1.info('There are '+str(len(fileList))+' files to process in: ' + path)
    if (len(fileList)==0):
        logger_1.info('No files in: ' + path)
    else:
        return (fileList)



#General function to print the correct usage of the Python script
#General function to print the correct usage of the Python script
def usage():
    print("%s" % sys.argv[0])
    print("Usage: -n [number of threads to execute] -t [type: all, book, review or chapter]")
    print("Help: Default will run on all the files in the directory and on all the types with one thread")
    sys.exit(2)

lock1 = threading.Lock()
def incrementProcCount():
    global processCount
    lock1.acquire()
    processCount+=1
    lock1.release()

lock2 = threading.Lock()
def incrementErrCount():
    global errorCount
    global lastErrorTime
    global errorCountTotal
    lock2.acquire()
    errorCountTotal += 1
    timeNow=round(time.time())
    if ((timeNow-lastErrorTime)>20):
        errorCount = 1
        lastErrorTime=timeNow
    else:
        lastErrorTime = timeNow
        errorCount+=1
    lock2.release()

def threadFinished(threadPool):
    while (len(threadPool) >= maxThread):
        time.sleep(0.01)
        for t in threadPool:
            if not t.isAlive():
                threadPool.remove(t)
    return threadPool

def globalMonitor(message,totalCount):
    import datetime
    if (len(message)>0):
        logProgress.info(message)
    else:
        totalTime=str(datetime.timedelta(seconds=round(time.time()-startTime)))
        progressPercent = round(((processCount + errorCountTotal) / totalCount * 100), 2)
        logProgress.info('Passed: ' + str(processCount) + ' Failed: ' + str(errorCountTotal) + ' Out of: ' + str(totalCount) + ' processed: ' + str(progressPercent) + '% in ' + totalTime)
        #logProgress.info('Passed: '+str(processCount)+' Failed: '+str(errorCountTotal)+ ' Out of: '+str(totalCount)+'running for: '+totalTime)

def threadExecution(dbSessionPool, importCql, importISBNCql, filePath, fileType):
    try:
        file = open(importCql, 'r')
        cypherImport = " ".join(file.readlines())
        file.close()
        file = open(importISBNCql, 'r')
        cypherISBN = " ".join(file.readlines())
        file.close()
    except Exception as e:
        logger_1.error("ERROR: Missing or invalid CQL file")
        file.close()
        raise e
    threadPool = []
    if os.name=='nt':
        logger_1.info('C:\eitan\\' + fileType)
        fileList = readDir('C:\eitan\\'+fileType)
    else:
        logger_1.info('for linux')
        fileList = readDir(filePath)
    try:
        for fileIter in fileList:
            #fileName = filePath + fileIter
            if (errorCount > maxError):
                raise Exception ('Max Errors')
            threadPool = threadFinished(threadPool)
            if (len(threadPool) < maxThread):
                if (processCount%10==0):
                    globalMonitor('',len(fileList))
                threadName = threading.Thread(target=dbSessionPool.processFiles, args=(dbSessionPool, cypherImport, cypherISBN, fileIter, filePath, fileType))
                threadPool.append(threadName)
                #logger_1.info('Starting thread ' + threadName.getName() + ' on file ' + fileIter)
                threadName.start()
        for t in threadPool:
            t.join()
    except Exception as e:
        logger_1.error("ERROR: Too many errors STOP script, Failed files: "+str(errorCount))
        for t in threadPool:
            t.join()
        raise e
    return (len(fileList))

if __name__ == "__main__":
    recType='all'
    # Read Configuration from YAML file
    try:
        with open("relatedItemsImport.yml", 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
        uri = cfg['neo4j']['host']
        user = cfg['neo4j']['user']
        password = cfg['neo4j']['passwd']
        booksPath = cfg['directories']['booksPath']
        bookChapPath = cfg['directories']['bookChapterPath']
        bookRevPath = cfg['directories']['bookReviewPath']
        cqlPath = cfg['directories']['cql']
    except Exception:
        logger_1.exception('Failed to read YAML file')
        sys.exit(1);

    #Use argumetns from command line if exist
    if (len(sys.argv)>1):
        try:
            myopts, args = getopt.getopt(sys.argv[1:], "n:t:h")
        except getopt.GetoptError as e:
            logger_1.error(str(e))
            usage()
            sys.exit(2)
        if (myopts == []):
            usage()
        for o, a in myopts:
            if (o == '-n'):
                try:
                    maxThread = int(a)
                except ValueError:
                    logger_1.error(a+" is not a number")
                    usage()
            elif ((o == '-t') and (a in ['all','book','chapter','review'])):
                recType = a
            else:
                usage()
    else:
        logger_1.info("Running with default settings")

    logger_1.info('Starting Batch processing ' + datetime.now().strftime('%d/%m/%Y %H:%M'))
    logProgress.info('Starting Batch processing ' + datetime.now().strftime('%d/%m/%Y %H:%M'))

    try:
        neo4jConnection = importToDb(uri, user, password)
        if(recType in ['all','book']):
            startBook = time.time()
            numFilesBooks=threadExecution(neo4jConnection, cqlPath + "/importBooks.cql", cqlPath + "/importISBNBooks.cql",booksPath, "Books")
            endBook = time.time()
            logger_1.info("Total Time Book Import on: "+str(numFilesBooks)+" files " + str(round((endBook - startBook) * 1000)) + " ms "+str(errorCountTotal)+" Failed "+str(processCount)+" Passed")
            errorCountTotal=0
            processCount=0
        if(recType in ['all','chapter']):
            startChapter = time.time()
            numFilesChap=threadExecution(neo4jConnection, cqlPath + "/importBookChapters.cql", cqlPath + "/importISBNChapters.cql",bookChapPath, "BookChapters")
            endChapter = time.time()
            logger_1.info("Total Elapsed Time for Chapter Import on: "+str(numFilesChap)+" files " + str(round((endChapter - startChapter) * 1000)) + " ms "+str(errorCountTotal)+" Failed "+str(processCount)+" Passed")
            errorCountTotal=0
            processCount=0
        if (recType in ['all', 'review']):
            startReview = time.time()
            numFilesRev=threadExecution(neo4jConnection, cqlPath + "/importBookReviews.cql", cqlPath + "/importISBNReviews.cql", bookRevPath, "BookReviews")
            endReview = time.time()
            logger_1.info("Total Elapsed Time for Review Import on: "+str(numFilesRev)+" files " + str(round((endReview - startReview) * 1000)) + " ms "+str(errorCountTotal)+" Failed "+str(processCount)+" Passed")
            errorCountTotal=0
            processCount=0
    except Exception:
        logger_1.exception('Related Items Import Failed '+str(errorCountTotal)+' Failed '+str(processCount)+' Passed')
        sys.exit(1);
    finally:
        neo4jConnection.close
