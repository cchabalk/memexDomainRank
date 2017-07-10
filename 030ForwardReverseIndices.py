import gzip
import ujson
import os
from os import listdir
from os.path import isfile, join, normpath
import sys
import time

















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

def createDictionaries(fileName):
    linksDict = {}
    linesCount = 0
    nameValueDict = {} #this must persist through function calls
    nameValueMeta={'currentKey':0}
    with gzip.open(fileName) as fp:
        #The file is only 1 line
        tStart = time.time()
        line = fp.readline()

    data = ujson.loads(line)

    kTemp = data.keys()
    for pURL in kTemp:

        linesCount += 1

        if pURL in nameValueDict.keys():
            pass
            #good
            #maybe do something with forward reverse
        else:
            nameValueDict[pURL]=nameValueMeta['currentKey']
            nameValueMeta['currentKey'] +=1

        for childURL in data[pURL]:
            if childURL in nameValueDict.keys():
                pass
                # good
                # maybe do something with forward reverse
            else:
                nameValueDict[childURL] = nameValueMeta['currentKey']
                nameValueMeta['currentKey'] += 1













                #        if (len(pURL) > 1):
    #            targetDict = linksDict.get(pURL, {})
    #            for link in data['childLinks']:
    #                targetDict[link] = targetDict.get(link, 0) + 1
    #            linksDict[pURL] = targetDict
    #            if linesCount % 100000 == 0:
    #                print linesCount,
    #                pass
    #tStop = time.time()
    #eTime = tStop - tStart
    #linesPerSec = linesCount / eTime
    #print "lines parsed: ", linesCount, "files to go:", filesCount, "took ", eTime, "; ", linesPerSec, "l/s"
    #writeDictToCsv(fileName, linksDict)  ##needs work here

    #linksDict.clear()
    #gc.collect()  #this does not apperar to work; it was included to attempt to free memory
    #print 'done gc'







if __name__=='__main__':

    nameValueDict = {}
    nameValueMeta = {}


    inDir = './data/type2/counted/'
    inDir = normpath(inDir)

    #get list of processed files
    logSuccess = normpath(inDir + normpath('/processedFiles.txt'))
    logFailed = normpath(inDir + normpath('/failedFiles.txt'))
    rawFiles = getFilesInDirectory(inDir)

    processedFiles = getProcessedFiles(logSuccess)
    processedFiles += getProcessedFiles(logFailed)

    filesCount = len(rawFiles)

    for file in rawFiles:
        if file not in processedFiles:
            # do the processing here
            start = time.clock()
            print "processing " + file
            try:
                createDictionaries(file)
                eTime = time.clock() - start
                print str(eTime) + ' sec'

                # logging
                print 'logging ', file
                #writeLogFile(logSuccess, file)
                time.sleep(0.1)
            except:
                print 'failed; ', file
                #writeFailedLog(logFailed, file)





