import gzip
import ujson
import os
from os import listdir
from os.path import isfile, join, normpath
import sys
import time
import csv



def getFilesInDirectory(inDir):
    inDir = normpath(inDir)
    onlyFilesTemp = [normpath(os.path.join(inDir, f)) for f in listdir(inDir) if (isfile(join(inDir, f)))]

    onlyFiles = []
    #do not return log files
    for line in onlyFilesTemp:
        try:
            if (line[-3:] != 'txt'):
                onlyFiles.append(line)
        except:
            pass

    return onlyFiles

def getProcessedFiles(inFile):
    inFile = normpath(inFile)
    processedFiles = []
    try:
        with open(inFile) as fp:
            #processedFiles = fp.readlines()
            processedFiles = fp.read().splitlines()
    except:
        processedFiles = []
    return processedFiles

def writeLogFile(logFilePath,processedFileName):
    logFilePath = normpath(logFilePath)
    processedFileName = normpath(processedFileName)
    if os.path.isfile(logFilePath) == False:
        with open(logFilePath,'w+') as fp: #create the log file
            pass

    with open(logFilePath,'a+') as fp:
        fp.write(processedFileName + '\n')



def writeFailedLog(logFilePath,failedFileName):
    logFilePath = normpath(logFilePath)
    failedFileName = normpath(failedFileName)
    if os.path.isfile(logFilePath) == False:
        with open(logFilePath,'w+') as fp: #create the log file
            pass

    with open(logFilePath,'a+') as fp:
        fp.write(failedFileName + '\n')

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

def createDictionaries(fileName,globalNameValue, globalNameReverse, nodeConnectivity, nodeMetrics):
    linesCount = 0
    localNameValue = set()
    with gzip.open(fileName) as fp:
        #The file is only 1 line
        tStart = time.time()
        line = fp.readline()

    data = ujson.loads(line)

    #get a set of keys
    parentKeys = data.keys()

    for parentURL in parentKeys:
        localNameValue.add(parentURL)
        for childURL in data[parentURL]:
            localNameValue.add(childURL)

    #get set of items in local but not in global
    #grabbed from: https://stackoverflow.com/questions/3462143/get-difference-between-two-lists
    s = set(globalNameValue)
    extraElements = [x for x in localNameValue if x not in s]

    #get the current length of the global dict; before adding elements
    currentGlobalLength = len(globalNameValue)

    #add items to globalNameValue list
    globalNameValue+=extraElements

    #now we have a name/value table
    #generate reverse lookup table
    for item in extraElements:
        globalNameReverse[item] = currentGlobalLength
        currentGlobalLength += 1  #increase number and go

    #given a site index # we get the name from globalNameValue[index #] = name
    #given a site name we get the index number from globalNameReverse[name] = index #

    #make global node connectivity table
    for item in parentKeys:
        #indexNo = globalNameReverse[item]  #get the index number - may not use this
        globalNodeDict = nodeConnectivity.get(item,{})
        localChildDict = data[item]

        for child in localChildDict.keys():
            #newValue             = existing value (or 0)       + incremental value
            globalNodeDict[child] = globalNodeDict.get(child,0) + localChildDict[child]

        #update the child count dict in the global node dict
        nodeConnectivity[item] = globalNodeDict






    print len(globalNameValue)
    print len(globalNameReverse.keys())
    print len(nodeConnectivity.keys())
    return (globalNameValue,globalNameReverse,nodeConnectivity)

def checkPointTables(globalNameValue,globalNameReverse,nodeConnectivity):

    #globalNameValue is a list; however, it is encoded as a json
    #because it contains unicode
    gnv = {'gnv':globalNameValue}
    try:
        with gzip.open('./globalNameValue.jl.gz', 'w+') as fp:
            fp.write(ujson.dumps(gnv))
    except:
        print "unable to write globalNameValues"

    try:
        with gzip.open('./globalNameReverse.jl.gz', 'w+') as fp:
            fp.write(ujson.dumps(globalNameReverse))
    except:
        print "unable to write globalNameReverse"

    try:
        with gzip.open('./nodeConnectivity.jl.gz', 'w+') as fp:
            fp.write(ujson.dumps(nodeConnectivity))
    except:
        print "unable to write nodeConnectivity"


def runPipeLine():
    nameValueDict = {}
    nameValueMeta = {}


    inDir = '../memexDomainRank/data/type2/counted/'
    inDir = normpath(inDir)

    #get list of processed files
    logSuccess = normpath(inDir + normpath('/processedFiles.txt'))
    logFailed = normpath(inDir + normpath('/failedFiles.txt'))
    rawFiles = getFilesInDirectory(inDir)

    processedFiles = getProcessedFiles(logSuccess)
    processedFiles += getProcessedFiles(logFailed)

    filesCount = len(rawFiles)
    globalNameValue = []
    globalNameReverse = {}
    nodeConnectivity = {}
    nodeMetrics = {}


    for file in rawFiles:
        if file not in processedFiles:
            # do the processing here
            start = time.clock()
            print "processing " + file
            try:
                (globalNameValue,globalNameReverse,nodeConnectivity) = createDictionaries(file,globalNameValue, globalNameReverse, nodeConnectivity, nodeMetrics)
                eTime = time.clock() - start
                print str(eTime) + ' sec'

                # logging
                print 'logging ', file
                #writeLogFile(logSuccess, file)
                time.sleep(0.1)
            except:
                print 'failed; ', file
                #writeFailedLog(logFailed, file)
    checkPointTables(globalNameValue,globalNameReverse,nodeConnectivity)

if __name__=='__main__':
    runPipeLine()