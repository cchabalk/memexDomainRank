import gzip
import ujson
import os
from os import listdir
from os.path import isfile, join, normpath
import sys
import time
import csv
import networkx as nx
import numpy as numpy
import pandas as pd
# from fileSupportFunctions import cleanPath, getLogFilePaths, getFilesInDirectory, getProcessedFiles, writeLogFile, writeFailedLog, ensure_dir
from fileSupportFunctions import *
from urlSupportFunctions import createURLAttributes, createUrlAttributesFromFile, urlAttributesDictToSortedCSVs

####################################################################
########## Parameters to to set when running stand alone ###########
####################################################################

baseDirStandalone = "../mediumDataTest/"
CONFIG_FILE_PATH = "./config"

####################################################################

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

def checkPointTables(globalNameValue,globalNameReverse,nodeConnectivity,outputDirAbs):

    #globalNameValue is a list; however, it is encoded as a json
    #because it contains unicode
    gnv = {'gnv':globalNameValue}
    try:
        with gzip.open(outputDirAbs + '/globalNameValue.jl.gz', 'w+') as fp:
            fp.write(ujson.dumps(gnv))
    except:
        print "unable to write globalNameValues"

    try:
        with gzip.open(outputDirAbs + '/globalNameReverse.jl.gz', 'w+') as fp:
            fp.write(ujson.dumps(globalNameReverse))
    except:
        print "unable to write globalNameReverse"

    try:
        with gzip.open(outputDirAbs + '/nodeConnectivity.jl.gz', 'w+') as fp:
            fp.write(ujson.dumps(nodeConnectivity))
    except:
        print "unable to write nodeConnectivity"

def runPipeLine(baseDir, typeToParse, config):
    nameValueDict = {}
    nameValueMeta = {}        

    baseDirAbs = cleanPath(baseDir + '/' + str(typeToParse) + '/counted/')
    
    # Get the list of files to process
    rawFiles = getFilesInDirectory(baseDirAbs)

    # Get list of files that have already been processed 
    logSuccess, logFailed = getLogFilePaths(baseDirAbs, "ForwardReverse")
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
    checkPointTables(globalNameValue,globalNameReverse,nodeConnectivity,baseDirAbs)
 
    ################################################################################
    #### Create URL link graph attributes dictionary and associated DataFrame ######
    ################################################################################

    # This will take in the node node connectivity and create graph attributes for it, returning for each URL in dictionary form
    urlAttributeDictionary = createURLAttributes(nodeConnectivity)

    # Extract into DataFrame format to use Pandas utilities
    df = pd.DataFrame(urlAttributeDictionary).transpose()

    ###### Sort the DataFrame based on the config parameter ######
    try:
        attrToSortBy = str(config["link attributes sort parameter"])
        df = df.sort_values(attrToSortBy, ascending=False)
    except:
        df = df.sort_values('outdeg', ascending=False)

    ##### Create a view of the dataframe that is filtered #####
    dffil = df

    ###################################################################
    ##### Filter out the top N sites based on pagerank if desired #####
    ###################################################################

    filterByPageRank = config["filter link attributes by top pagerank"]

    if str(filterByPageRank).lower()=="true":
        numberToFilter = int(config["top N of pagerank to filter"])
        
        topNToFilter = df.sort_values('pagerank', ascending=False).index.values[:numberToFilter]
        
        dffil  = dffil[~dffil.index.isin(topNToFilter)]

    #####################################################################################
    ##### Filter out sites based on the custom URL list provided in the config file #####
    #####################################################################################

    filterCustomUrlList = config["filter custom URL list"]

    if str(filterCustomUrlList).lower():
        urlsToClean = config["URL filter list"]
        dffil = dffil[~dffil.index.isin(urlsToClean)]

    def getUrlsFromFile(filepath):
        with open(filepath) as f:
            allUrls = f.readlines()
        allUrls = [x.strip() for x in allUrls]
        return allUrls

    #########################################
    ###### Filter out URLS from a file ######
    #########################################

    # Find out if filtering from file is desired
    cleanUrlsFromFile = config["filter custom URL from files"]

    # If filtering from file is desired...
    if str(cleanUrlsFromFile).lower()=="true":
        # Load in and clean the file or directory with the file lists
        urlsToCleanFilePath = config["custom URL list file or directory"]
        urlsToCleanFilePath = cleanPath(urlsToCleanFilePath)
        
        urlsToFilter = []
        ##### If path is to single file ########################################
        if os.path.isfile(urlsToCleanFilePath):
            try:
                urlsToFilter += getUrlsFromFile(urlsToCleanFilePath)
                print "URLs to filter loaded from", urlsToCleanFilePath
            except:
                print "Error loading URLs from file", urlsToCleanFilePath
                
        ##### If path is to directory with multiple files ######################
        elif os.path.isdir(urlsToCleanFilePath):
            try:
                # Grab the list of files in the directory
                filesToParse = getFilesInDirectory(urlsToCleanFilePath)
                # Grab the list of URLs from each file in the directory
                for fileToParse in filesToParse:
                    urlsToFilter += getUrlsFromFile(fileToParse)
                print "URLs to filter loaded from all files in", urlsToCleanFilePath
            except:
                print "Error loading files from directory", urlsToCleanFilePath
                print "List of files attempted:", filesToParse
        ##### Otherwise, there is no valid file, let the user know #############
        else:
            print "Provided file path does not appear to be valid"
            print "Path provided:", urlsToCleanFilePath

    dffil = dffil[~dffil.index.isin(urlsToFilter)]

    #########################################################################
    #### Output non-filtered lists sorted by each attribute if desired ######
    #########################################################################

    # Find out if sorted lists are going to be output. Output if so
    outputSortedLists = config["output sorted lists by each attribute"]
    if str(outputSortedLists).lower()=="true":
        urlAttributesDictToSortedCSVs(df.transpose().to_dict(), baseDirAbs, file_tag='_non_filtered')

    #########################################################################
    #### Output filtered lists sorted by each attribute if desired ##########
    #########################################################################

    # Find out if sorted lists are going to be output. Output if so
    outputSortedLists = config["output filtered sorted lists by each attribute"]
    if str(outputSortedLists).lower()=="true":
        urlAttributesDictToSortedCSVs(dffil.transpose().to_dict(), baseDirAbs, file_tag='_filtered')

    #########################################################################
    ##### Output non-filtered and filtered URL attribute lists ##############
    #########################################################################

    df.to_csv(cleanPath(baseDirAbs+'/linkAttributes_non_filtered.csv'), encoding='utf8')
    dffil.to_csv(cleanPath(baseDirAbs+'/linkAttributes_filtered.csv'), encoding='utf8')

    # print "df final shape", df.shape
    # print "dffil final shape", dffil.shape

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

