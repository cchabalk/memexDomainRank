import time
import ujson
import numpy as np
from bs4 import BeautifulSoup
from multiprocessing import Process, Value, Lock, Pool, cpu_count

import smart_open
import gzip

import urllib
import lxml.html
import itertools

import os
from os import listdir
from os.path import isfile, join, normpath
import sys

from fileSupportFunctions import cleanPath, getLogFilePaths, getFilesInDirectory, getProcessedFiles, writeLogFile, writeFailedLog, ensure_dir

debug = False

def parseLinksLXML(inString):
    linkList = []
    try:
        dom = lxml.html.fromstring(inString.encode('ascii', 'ignore'))
        for link in dom.xpath('//a/@href'):  # select the url in href for all a tags(links)
            linkList.append(link)
        #if len(linkList) == 12330:
        #    print 12330
    except:
        pass
    return linkList

def parseLinksBS4(inString):
    linkList = []
    try:
        soup = BeautifulSoup(inString.encode('ascii', 'ignore'),"lxml")
        links = soup.find_all('a')

        for tag in links:
            link = tag.get('href', None)
            if link is not None:
                linkList.append(link)
    except:
        linkList = []
    return linkList

def parseListLinks(inJSON):
    outList = [] #initialize just in case
    outJSON = {}
    jData = ujson.loads(inJSON)
    outJSON['parentURL'] = jData['cleaned_url']
    outJSON['links'] = []

    try:
        # d2 = parseLinksBS4(jData['raw_content'])
        outList = parseLinksLXML(jData['raw_content'])
        outJSON['links'] = outList
    except:
        #failed += 1
        pass
    jsonString = ujson.dumps(outJSON)
    #return jsonString
    return outJSON

def cleanLinksFromFile(inFile):
    inFile = normpath(inFile)
    # create a pool of N workers
    #N = cpu_count()-1

    #i in range(0,N,1)
    #N = 1 #188 pages/sec
    #N = 2 # 237 pages/sec
    #N = 3 # #273
    #N = 4 #304
    #N = 5 #310
    #N = 6 #320
    #N = 7 #330
    #N = 8 #323

    #i in range(0,2*N,1)
    #N=1
    #N=2
    #N=3 #330
    #N=4 #350
    #N=5 #333
    N=6  #355
    #N=7 #356
    #N=8 #340




    if debug == True:
        N=1
    start = time.clock()
    pool = Pool(N)
    heartBeat = 5000*N  #heartbeat
    exportChunk = 100000  # lines
    if debug==True:
        exportChunk = 5  # lines
    finalOutput1 = []
    finalOutput2 = []
    finalOutput3 = []
    keepGoing = True
    currentLine = 0
    writeFileFirstTime(inFile)
    start = time.time()
    with smart_open.smart_open(inFile) as fp:
        while keepGoing == True:
            data = []
            for i in range(0, 2*N, 1):
                try:  # assume there is more data
                    data.append(fp.next())  # build up a chunk of data
                    currentLine += 1
                except:
                    print 'reached end of file'
                    keepGoing = False

            output = pool.map(cleanParentsAndChildren, data)
            #output = map(parseParentsAndChildren, data)


            #TO DO: IF OUTPUT IS LARGE - WRITE IT TO A FILE - CREATE APPEND
            for line in output:
                finalOutput1.append(line[0])
                finalOutput2.append(line[1])
                finalOutput3.append(line[2])

            if (len(finalOutput1)>=exportChunk):
                writeAllFiles(inFile,finalOutput1,finalOutput2,finalOutput3)
                finalOutput1 = []
                finalOutput2 = []
                finalOutput3 = []

            if currentLine % heartBeat == 0:
                eTime = time.time()-start
                start = time.time()
                print 'currentLine: ', currentLine, 'est. l/sec: ', heartBeat/float(eTime)
                sys.stdout.flush()

#            if (currentLine>206350):
#                print currentLine
    #EXPORT THE DATA AND ZERO OUT THE LISTS
    writeAllFiles(inFile, finalOutput1, finalOutput2, finalOutput3)
    finalOutput1 = []
    finalOutput2 = []
    finalOutput3 = []


    pool.close()
    pool.join()


    return

def cleanParentsAndChildren(inputString):
	"""
	This function takes in input from XXXXX type of input, which has already grabbed
	parent and children links. It then cleans them and outputs them in the expected formats
	for the rest of the processing pipeline.
	"""
	
    jsonEntry = ujson.loads(inputString)
    inputString = []
    # These links don't need to be parsed from XML, will be in list format for the JSON
    try:
        #print jsonEntry.keys()
        parentURL = jsonEntry['url'].rstrip(' ').lstrip(' ')
    except:
        parentURL = 'http://www.error.com'
        print 'exception'
    try:
    	extractedLinks = jsonEntry['children']
    except:
    	extractedLinks = []

    ##### The extracted links should be in the correct format now, and the rest of the normal process can continue #####

    parentURL = cleanParent(parentURL)
    extractedLinks = cleanChildren(parentURL, extractedLinks)


    #parse links 3 different ways
    (parent1,parent2,parent3) = reparse([parentURL])  #it expects a list
    (children1,children2,children3)=reparse(extractedLinks)

    #put into json
    pc1 = {'parentURL':parent1[0],'childLinks':children1}
    pc2 = {'parentURL':parent2[0],'childLinks':children2}
    pc3 = {'parentURL':parent3[0],'childLinks':children3}

    pc1 = ujson.dumps(pc1)
    pc2 = ujson.dumps(pc2)
    pc3 = ujson.dumps(pc3)

    return (pc1,pc2,pc3)  #return tuple of strings

def multipleUnquote(s):
    k = 0
    while ('%' in s) and (k <= 4):
        s = urllib.unquote(s)
        k += 1
    return s

def cleanParent(parentURL):
    try:
        parentURL = multipleUnquote(parentURL)
        parentURL = parentURL.lstrip()
        parentURL = parentURL.split(' ')[0]
    except:
        parentURL = 'http://www.error.com'
    return parentURL


def cleanChildren(parentURL,extractedLinks):
    cleanedLinks = []
    k = 0
    N = len(extractedLinks)
    for link in extractedLinks:
        try:
            link = multipleUnquote(link)
            if link[0] == '/':  #pure internal link
                link = link.split(' ')[0] #remove trailing spaces
                fullLink = parentURL + link
                cleanedLinks.append(fullLink)
                #print fullLink
            elif link[0:4] == 'http': #pure external link
                link = link.split(' ')[0] #remove trailing spaces
                cleanedLinks.append(link)
            else: #look like javascript and anchor tags; disregard for now
                pass
        except Exception as e:
            #print e
            pass
        k += 1
        #if k%10 == 0: #status update
        #    print str(k) + '/' + str(N),
    return cleanedLinks

def reparse(inputList):

    #TO DO - GET WEIGHTS
    #REMEMEBER TO PARSE PARENT URL

    type1List = []
    type2List = []
    type3List = []



    for inputString in inputList:
        #TYPE 1
        type1Temp = inputString.split('?')[0]  #POST parameters do not denote unique node
        type1Temp = type1Temp.split(';')[0]  #; shows up as some sort of delimiter in URL's
        type1List.append(type1Temp)

        #TYPE 2
        type2Temp = inputString.replace('http://','').replace('https://','').replace('http:/','').replace('https:/','')
        type2Temp = type2Temp.split('/')[0]    #keep the 1st
        type2List.append(type2Temp)

        #TYPE 3
        type3Temp = inputString.replace('http://','').replace('https://','').replace('http:/','').replace('https:/','')
        type3Temp = type3Temp.split('/')[0]    #keep the 1st
        if '.co.' in type3Temp: #this includes .co.uk & .co.in & others
           type3Temp = '.'.join(type3Temp.split('.')[-3:])  #keep the last three
        else:
           type3Temp = '.'.join(type3Temp.split('.')[-2:])  #keep the last two
        type3List.append(type3Temp)

    return (type1List,type2List,type3List)

def writeAllFiles(inFile,chunk1,chunk2,chunk3):
    #append to gzip
    caseName = os.path.basename(inFile)
    baseDirName = os.path.split(inFile)[0]
    fileName = normpath(baseDirName + '/type1/' + caseName)
    with gzip.open(fileName,'a+') as fp:
        for item in chunk1:
            fp.write(item + '\n')

    fileName = normpath(baseDirName + '/type2/' + caseName)
    with gzip.open(fileName,'a+') as fp:
        for item in chunk2:
            fp.write(item + '\n')

    fileName = normpath(baseDirName + '/type3/' + caseName)
    with gzip.open(fileName,'a+') as fp:
        for item in chunk3:
            fp.write(item + '\n')
    return

def writeFileFirstTime(inFile):
    #create/erase
    caseName = os.path.basename(inFile)
    baseDirName = os.path.split(inFile)[0]
    
    folders = ['/type1/', '/type2/', '/type3/']

    for folder in folders:
        #check if directopry erxists; if not create it
        ensure_dir(baseDirName + folder)
        try:
            fileName = normpath(baseDirName + folder + caseName)
            with gzip.open(fileName,'w') as fp:
                pass
        except:
            pass

    return

def runPipeLine(baseDir):
	#######################################################
	###### Run all support functions to ingest files #####
	#####################################################
    # All work is stored in the type2 subfolder of the main data folder
    baseDirAbs = cleanPath(baseDir)
    
    # Get the list of files to process
    rawFiles = getFilesInDirectory(baseDirAbs)

    # Get list of files that have already been processed 
    logSuccess, logFailed = getLogFilePaths(baseDirAbs, "CleanLinks")
    processedFiles = getProcessedFiles(logSuccess)
    processedFiles += getProcessedFiles(logFailed)

    #######################################################
	###### Process all files already containing links ####
	#####################################################
    for file in rawFiles:
        if file not in processedFiles:
            #do the processing here
            start = time.clock()
            print "processing " + file
#            file = 'hg_try_5_3438c921200e_items_deduped.jl.gz'
            try:
                cleanLinksFromFile(normpath(os.path.join(baseDirAbs,file)))
                eTime = time.clock()-start
                print str(eTime) + ' sec'

                #logging
                print 'logging ', file
                writeLogFile(logSuccess, file)
                time.sleep(0.1)
            except:
                print 'failed; ', file
                writeFailedLog(logFailed, file)


# This only needs to be set when running the script individually
baseDirStandalone = "../memexGithub/data/"
if __name__ == '__main__':
    runPipeLine(baseDirStandalone)