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

from fileSupportFunctions import *
from urlSupportFunctions import multipleUnquote, cleanParent, cleanChildren

####################################################################
########## Parameters to to set when running stand alone ###########
####################################################################

baseDirStandalone = "../mediumDataTest/"
CONFIG_FILE_PATH = "./config"

####################################################################

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

def parseLinksFromFile(inFile):
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
    heartBeat = 500*N  #heartbeat
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

            output = pool.map(parseParentsAndChildren, data)
            output = filter(lambda x: type(x)==tuple, output) #gets rid of rows that don't have a URL parsed
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

def parseParentsAndChildren(inputString):

    jsonEntry = ujson.loads(inputString)
    inputString = []

    # if there is no URL field, none of this parsing will matter. this also eliminates the index problem created from the S3 download
    if 'url' not in jsonEntry.keys():
        return "index_row"

    try:
        #print jsonEntry.keys()
        parentURL = jsonEntry['url'].rstrip(' ').lstrip(' ')
    except:
        parentURL = 'http://www.error.com'
        print 'exception'
    try:
        dom = lxml.html.fromstring(jsonEntry['raw_content'])
        extractedLinks = dom.xpath('//a/@href')
    except:
        try:
            extractedLinks = parseLinksBS4(jsonEntry['raw_content'])
            #print 'tricky'
        except:
            extractedLinks = []
            pass
            #print jsonEntry['raw_content']
            #sys.exit()

    #if links were already extracted; they could be inserted here
    #and the process could be picked up : SOLVED with 040
    parentURL = cleanParent(parentURL)
    extractedLinks = cleanChildren(parentURL, extractedLinks)
    
    #parse links 3 different ways
    (parent1,parent2,parent3) = reparse([parentURL])  #it expects a list
    (children1,children2,children3)=reparse(extractedLinks)

    #put into json
    pc1 = {'parentURL':parent1[0],'childLinks':children1}
    pc2 = {'parentURL':parent2[0],'childLinks':children2}
    pc3 = {'parentURL':parent3[0],'childLinks':children3}

    pc1 = ujson.dumps(pc1, ensure_ascii=False)
    pc2 = ujson.dumps(pc2, ensure_ascii=False)
    pc3 = ujson.dumps(pc3, ensure_ascii=False)

    return (pc1,pc2,pc3)  #return tuple of strings






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
        type1Temp = type1Temp.replace('www.','') #treat www. and non-www. addresses the same
        #TO DO: fix above to ensure www. only appears at the beginning of an address
        type1List.append(type1Temp)

        #TYPE 2
        type2Temp = inputString.replace('http://','').replace('https://','').replace('http:/','').replace('https:/','')
        type2Temp = type2Temp.split('/')[0]    #keep the 1st
        type2Temp = type2Temp.split('?')[0]  #yes - sometimes a link is www.example.com?x=5104.7545.4961...
        type2Temp = type2Temp.split('#')[0] #remove anchor tags
        type2Temp = type2Temp.replace('www.','') #treat www. and non-www. addresses the same
        #TO DO: fix above to ensure www. only appears at the beginning of an address
        type2List.append(type2Temp)

        #TYPE 3
        type3Temp = inputString.replace('http://','').replace('https://','').replace('http:/','').replace('https:/','')
        type3Temp = type3Temp.split('/')[0]    #keep the 1st
        type3Temp = type3Temp.split('?')[0]  # yes - sometimes a link is www.example.com?x=5104.7545.4961...
        type3Temp = type3Temp.split('#')[0] #get rid of anchor tags...
        type3Temp = type3Temp.replace('www.','') #treat www. and non-www. addresses the same
        #TO DO: fix above to ensure www. only appears at the beginning of an address
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

def runPipeLine(baseDir, config):
    # Currently, config is not used, but included for future use

    # All work is stored in the type2 subfolder of the main data folder
    baseDirAbs = cleanPath(baseDir)
    
    # Get the list of files to process
    rawFiles = getFilesInDirectory(baseDirAbs)

    # Get list of files that have already been processed 
    logSuccess, logFailed = getLogFilePaths(baseDirAbs, "Parse")
    processedFiles = getProcessedFiles(logSuccess)
    processedFiles += getProcessedFiles(logFailed)

    for file in rawFiles:
        if file not in processedFiles:
            #do the processing here
            start = time.clock()
            print "processing " + file
#            file = 'hg_try_5_3438c921200e_items_deduped.jl.gz'
            try:
                parseLinksFromFile(normpath(os.path.join(baseDirAbs,file)))
                eTime = time.clock()-start
                print str(eTime) + ' sec'

                #logging
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

    runPipeLine(baseDirStandalone, config)
