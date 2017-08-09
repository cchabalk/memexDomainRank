import os
from os import listdir
from os.path import isfile, join, normpath
import sys
import gzip
import ujson
import urllib
import time
import gc
from multiprocessing import Pool
import signal

from fileSupportFunctions import *

####################################################################
########## Parameters to to set when running stand alone ###########
####################################################################

baseDirStandalone = "../mediumDataTest/"
CONFIG_FILE_PATH = "./config"

####################################################################

def getGzFilesInDirectory(inDir):
    inDir = normpath(inDir)
    onlyFiles = [join(inDir, f) for f in listdir(inDir) if (isfile(join(inDir, f)) and ((f[-1] == 'z')))]
    return onlyFiles

def getCsvFilesInDirectory(inDir):
    inDir = normpath(inDir)
    onlyFiles = [join(inDir, f) for f in listdir(inDir) if (isfile(join(inDir, f)) and ((f[-1] == 'v')))]
    return onlyFiles

def getNonProcessedFiles(gzDir, csvDir):
    gzFiles = getGzFilesInDirectory(gzDir)
    csvFiles = getCsvFilesInDirectory(csvDir)

    #filter files
    gzFilesFiltered = []

    #convert to csv filename and the drop extension

    for file in gzFiles:
        csvFile = convertGZNametoCSVName(file)

        #N = len(file)
        #ix = N - file[::-1].find('.') - 1  # find last '.'
        #file = file[0:ix]

        # Find last again
        #N = len(file)
        #if '.' in file:
        #    ix = N - file[::-1].find('.') - 1  # find last '.'
        #    file = file[0:ix]
        gzFilesFiltered.append(csvFile[:-4])

    csvFilesFiltered = []
    for file in csvFiles:
        #N = len(file)
        #ix = N - file[::-1].find('.') - 1  # find last '.'
        #file = file[0:ix]
        #csvFilesFiltered.append(file)
        csvFilesFiltered.append(file[:-4])

    #do the compairison
    filesToBeProcessed = []
    for k1,f1 in enumerate(gzFilesFiltered):
        addToList = True
        for f2 in csvFilesFiltered:
            if f1 == f2:
                addToList = False
        if addToList == True:
            filesToBeProcessed.append(gzFiles[k1])

    return filesToBeProcessed

def getNonProcessedFilesVerified(gzDir, csvDir,parsingType):
    nStart = 0
    nSuccess = 0
    nFail = 0
    gzFiles = getGzFilesInDirectory(gzDir)
    csvFiles = getCsvFilesInDirectory(csvDir)


    filesToBeProcessed = []  #list of files to be processed

    for file in gzFiles[nStart:]:
        #open a gz file, get the final parent url
        nGZ = 1 #default value
        try:
            with gzip.open(file, "rb") as f:
                gzUniqueSet = []
                nGZ = 0
                data = ' ' #initialize data to be read later

                for line in f:
                    data = line  # Read all the lines serially
                    t1 = ujson.loads(data)['parentURL']
                    t2 = reparse(t1,parsingType)
                    gzUniqueSet.append(t2)
            gzUniqueSet = set(gzUniqueSet)
            nGZ = len(gzUniqueSet)
        except:
            pass


        #open the corrisponding csv file, get the final parent url
        nCSV = 0
        csvFile = convertGZNametoCSVName(file)
        csvParentURL = ' ' #will be populated later
        try:
            with open(csvFile, "rb") as f:
                csvUniqueSet = []
                nCSV = 0
                for line in f:
                    csvParentURL = line.split(',')[0]  #get the first csv value
                    csvUniqueSet.append(csvParentURL)
                csvUniqueSet = set(csvUniqueSet)  #convert to set
                nCSV = len(csvUniqueSet)

        except:
            print csvFile, 'not found'

        #if they are the same, the csv file is complete, and the fileName should be removed from the queue
        percentDifference = 100
        percentDifference = float((nGZ-nCSV)/float(nGZ)*100.)
        print 'nGZ:', nGZ, 'nCSV:', nCSV, 'pDif:', percentDifference

        if percentDifference<=6.:
            nSuccess += 1
            #don't do anything

            print file, ' processed successfully '
            print 'nSuccess', nSuccess
        else:
            nFail += 1
            print file, ' requires reprocessing '
            print 'nFail', nFail
            filesToBeProcessed.append(file)

        with open('logFile2.csv','a+') as f:
            output = str(nSuccess+nFail+nStart) + ',' + file + ',nGZ,' + str(nGZ) + ',nCSV,' + str(nCSV) + ',pDif,' + str(percentDifference) + '\n'
            f.write(output)

    return filesToBeProcessed


def convertGZNametoCSVName(fileName):
    N = len(fileName)
    ix = N - fileName[::-1].find('.') - 1  # find last '.'
    csvFile = fileName[0:ix]

    # Find last again
    N = len(csvFile)
    if '.' in csvFile:
        ix = N - csvFile[::-1].find('.') - 1  # find last '.'
        csvFile = csvFile[0:ix]

    csvFile = csvFile + '.csv'
    csvFile = os.path.normpath(csvFile)
    return csvFile

def writeDictToCsv(fileName, linksDict):

    # save dict to file

    (p,f) = os.path.split(fileName) #get (path, file)
    p = os.path.join(p,os.path.normpath('counted'))  #concatenate counted to path
    countedFile = os.path.join(p,f) #make fileName to store file in "counted" directory

    #create directory with COUNTED files
    #this files contain parents/child links with COUNTS rather than
    #individual children
    ensure_dir(countedFile)

    try:
        with gzip.open(countedFile, 'w+') as fp:
            fp.write(ujson.dumps(linksDict))
    except:
        print "unable to write"


    print "wrote " + countedFile
    return

def doTheWork(input):

    linksDict = {}
    fileName = input[0]
    # fileName = input
    with gzip.open(fileName) as fp:

        linesCount = 0
        tStart = time.time()
        # for x in range(0,5):
        # import pdb; pdb.set_trace()
        # ll = fp.readlines()

        for line in fp:
            linesCount += 1
            pURL = []

            try:
                data = ujson.loads(line)
                pURL = data['parentURL']
            except:
                print 'failed'
            if (len(pURL) > 1):
                targetDict = linksDict.get(pURL, {})
                for link in data['childLinks']:
                    targetDict[link] = targetDict.get(link, 0) + 1
                linksDict[pURL] = targetDict
                if linesCount % 100000 == 0:
                    print linesCount,
                    pass
    tStop = time.time()
    eTime = tStop - tStart
    linesPerSec = linesCount / eTime
    # print "lines parsed: ", linesCount, "files to go:", filesCount, "took ", eTime, "; ", linesPerSec, "l/s"
    writeDictToCsv(fileName, linksDict)  ##needs work here

    linksDict.clear()
    gc.collect()  #this does not apperar to work; it was included to attempt to free memory
    print 'done gc'

##my additions
##my additions
##my additions
##my additions
##my additions

def runPipeLine(baseDir, typeToParse, config):
    # note:  There is a memory issue, not a "memory leak"
    #If a large dictionary is created ~4GB+, memory is consumed by the object
    #However, when the object goes out of scope, or if it is = None
    #The memory is not returned
    #If a subsequent dictionary is created, addtitional memory is allocated
    #This continues until swap is consumed, etc.
    #One remedy for this is to operate in a single threaded process
    #The code below does not actually do anything in parallel, but it uses the multiprocessing
    #pool to ensure the processing thread is eliminated and memory is returned

    # Additionally, config is not used in the current version, but there for future use if parameters are added

    # All work is stored in the type2 subfolder of the main data folder
    # typeFolder = "type2"
    baseDirAbs = cleanPath(baseDir + '/' + typeToParse + '/')
    print baseDirAbs
    
    # Get the list of files to process
    rawFiles = getFilesInDirectory(baseDirAbs)

    # Get list of files that have already been processed 
    logSuccess, logFailed = getLogFilePaths(baseDirAbs, "Count")
    processedFiles = getProcessedFiles(logSuccess)
    processedFiles += getProcessedFiles(logFailed)

    filesCount = len(rawFiles)

    for file in rawFiles:
        if file not in processedFiles:
            # do the processing here
            start = time.clock()
            print "processing " + file

            # input = (file,)
            # print input
            # doTheWork(input)
            try:
                pool = Pool(processes=1)
                input = (file,)
                # input = file
                pool.map(doTheWork, (input,))
                pool.close()
                pool.join()
                gc.collect()
                eTime = time.clock() - start
                print str(eTime) + ' sec'
                # doTheWork((input,))
                # logging
                print 'logging ', file
                writeLogFile(logSuccess, file)
                time.sleep(0.1)
            except:
                print 'failed; ', file
                writeFailedLog(logFailed, file)

#######################################################################################
########## main function for standalone use, not used when full pipeline run ##########
#######################################################################################

if __name__=='__main__':
    # load in the configuration file, a default is loaded if config file not found
    config = loadconfig(CONFIG_FILE_PATH)

    # extract the set of link types to work on
    typesToParseLinks = config["types to parse"]

    # Run the pipeline for each link type
    for typeToParse in typesToParseLinks:
        runPipeLine(baseDirStandalone, typeToParse, config)
