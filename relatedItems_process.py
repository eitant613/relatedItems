#from __future__ import print_function
from neo4j import GraphDatabase
import logging
import time
import sys
import yaml
import getopt
import threading
import os
from datetime import datetime
from datetime import timedelta

errorCount = 0
maxError = 3
processCount=0
maxThread=1

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
logFileName = logDir+datetime.now().strftime('postprocess_%Y_%m_%d.log')
logger_1 = setup_logger('defaultLog', logFileName, logging.INFO, '%(asctime)s - %(levelname)s - %(message)s', True)

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
    def processFiles(self, dbSession, cqlName):
        try:
            with open(cqlName, 'r') as f:
                cypherExec = " ".join(f.readlines())
        except Exception as e:
            incrementErrCount()
            logger_1.error("ERROR: Missing or invalid Cypher file " + cqlName + " " + str(errorCount) + " " + str(maxError))
            if (errorCount > maxError):
                raise e
            else:
                return

        logger_1.info("Started to process "+cqlName)
        start = time.time()
        try:
            dbSession.openSession(cypherExec)
            incrementProcCount()
            totalTime = str(timedelta(seconds=round(time.time() - start)))
            logger_1.info("Time elapsed to process " + cqlName + " took: " + totalTime)
        except Exception as e:
            incrementErrCount()
            logger_1.error("ERROR: Failed to load or execute Cypher for "+cqlName+" "+str(errorCount)+" "+str(maxError))
            if ( errorCount > maxError ):
                raise e
        finally:
            dbSession.closeSession()

    #Function that will open session to neo4j and call next function to execute the query
    def openSession(self, cypher):
        with self._driver.session() as session:
            sessionWrite = session.write_transaction(self._write_data, cypher)

    def closeSession(self):
        with self._driver.session() as session:
            session.close();

    #Staic method to execute the query and return results
    @staticmethod
    def _write_data(tx, cypher):
        result = tx.run(cypher)
        resultSummary=result.summary().counters
        logger_1.info(resultSummary)
        for item in result:
            logger_1.info(item)


#General function to print the correct usage of the Python script
def usage():
    print("%s" % sys.argv[0])
    print("Usage: %s -c [specific SQL to execute] -l [name of file with list of CQL each in new line] -n [Number of threads optional only if CQL's can be executed in parallel")
    print("Help: Must give either name of CQL in the CQL directory set in the yaml file or a file name with list of CQL's to execue each in a new line")
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
    lock2.acquire()
    errorCount+=1
    lock2.release()

def threadFinished(threadPool):
    while (len(threadPool) >= maxThread):
        time.sleep(0.01)
        for t in threadPool:
            if not t.isAlive():
                threadPool.remove(t)
    return threadPool

def threadExecution(dbSessionPool, cqlList):
    threadPool = []
    try:
        for cqlIter in cqlList:
            if (errorCount > maxError):
                raise Exception ('Max Errors')
            cqlIter = cqlPath + "/" + cqlIter
            threadPool=threadFinished(threadPool)
            if (len(threadPool) < maxThread):
                threadName = threading.Thread(target=dbSessionPool.processFiles, args=(dbSessionPool, cqlIter))
                threadPool.append(threadName)
                #logger_1.info('Starting thread ' + threadName.getName() + ' on file ' + cqlIter)
                threadName.start()
        for t in threadPool:
            t.join()
    except Exception as e:
        logger_1.error("ERROR: Too many errors STOP script, Failed files: "+str(errorCount))
        for t in threadPool:
            t.join()
        raise e
    return (len(cqlList))

if __name__ == "__main__":
    recType='all'
    # Read Configuration from YAML file
    try:
        with open("relatedItemsImport.yml", 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
        uri = cfg['neo4j']['host']
        user = cfg['neo4j']['user']
        password = cfg['neo4j']['passwd']
        cqlPath = cfg['directories']['cql']
    except Exception:
        logger_1.exception('Failed to read YAML file')
        sys.exit(1);

    #Use argumetns from command line if exist
    hasMin = False
    if (len(sys.argv)>1):
        try:
            myopts, args = getopt.getopt(sys.argv[1:], "c:l:n:h")
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
            elif (o == '-c'):
                cqlName = a
                hasMin=True
                maxThread = 1
            elif (o == '-l'):
                cqlFile = a
                hasMin=True
            else:
                usage()
    else:
        usage()

    if (not hasMin):
        usage()

    logger_1.info('Starting Post processing ' + datetime.now().strftime('%d/%m/%Y %H:%M'))
    try:
        cqlList = []
        neo4jConnection = importToDb(uri, user, password)
        try: cqlName
        except NameError: cqlName=False
        try: cqlFile
        except NameError: cqlFile=False

        if (cqlName):
            cqlList.append(cqlName)
            print(cqlList)
        if (cqlFile):
            try:
                file = open(cqlFile, 'r')
                for line in file:
                    cqlList.append(line.strip('\n'))
                file.close()
                while '' in cqlList:
                    cqlList.remove('')
            except Exception as e:
                logger_1.error("ERROR: Missing or invalid CQL list file")
                file.close()
                raise e
        startCqlList = time.time()
        numCql=threadExecution(neo4jConnection, cqlList)
        totalTime=str(timedelta(seconds=round(time.time()-startCqlList)))
        logger_1.info("Total Elapsed Time for: "+str(numCql)+" CQL's processed: " +str(processCount)+" CQL's Error Count " +str(errorCount)+" : "+ totalTime)
        neo4jConnection.close

    except Exception:
        logger_1.exception('CQL Execution Failed')
        neo4jConnection.close
        sys.exit(1);

    finally:
        neo4jConnection.close
