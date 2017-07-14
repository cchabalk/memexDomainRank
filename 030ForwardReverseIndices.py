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
from urlFeatureExtraction import createURLAttributes, createUrlAttributesFromFile, urlAttributesDictToSortedCSVs

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

# def createURLAttributes(nodeConnectivityDict):
#     # Weighted directed graph to hold the website connections
#     G = nx.DiGraph()

#     # Append all unique edges. Currently assumes no duplicated edges
#     edges = []
#     for parentURL, childrenURLs in nodeConnectivityDict.items():
#         for childURL, childCount in childrenURLs.items():
#             edges.append((parentURL, childURL, childCount))
#     G.add_weighted_edges_from(edges)

#     # Create a blank dictionary to hold the attributes of each URL in the graph
#     URL_attr = {parentURL: {'outdeg': 0, 'indeg': 0, 'uniquein': 0, 'uniqueout': 0, 'pagerank': 0.0} \
#                 for parentURL in G.nodes()}

#     # Find unique number of outgoing edges, total edge weight of outgoing edges
#     uniqueout = G.out_degree()
#     outdeg = G.out_degree(weight='weight')

#     # Find unique number of incoming edges, total edge weight of incoming edges
#     uniquein = G.in_degree()
#     indeg = G.in_degree(weight='weight')

#     # Store these values in a dictionary we can use for ML or any other ranking alg
#     for URL in G.nodes():
#         URL_attr[URL]['uniqueout'] = uniqueout[URL]
#         URL_attr[URL]['uniquein'] = uniquein[URL]
#         URL_attr[URL]['outdeg'] = outdeg[URL]
#         URL_attr[URL]['indeg'] = indeg[URL]
#         URL_attr[URL]['uin_uout_ratio'] = 0.0 if uniqueout[URL]==0 else float(uniquein[URL])/uniqueout[URL]
#         URL_attr[URL]['in_out_ratio'] = 0.0 if outdeg[URL]==0 else float(indeg[URL])/outdeg[URL]

#     # Calculate and store the weighted and unweighted pagerank for each
#     for URL, prval in nx.pagerank(G, weight='weight').iteritems():
#         URL_attr[URL]['pagerank'] = prval
#     for URL, prval in nx.pagerank(G, weight=None).iteritems():
#         URL_attr[URL]['pagerank_noweight'] = prval

#     return URL_attr #the dictionary with all URL nodes and their calculated attributes

def runPipeLine(baseDir):
    nameValueDict = {}
    nameValueMeta = {}


    # inDir = '../memexDomainRank/data/type2/counted/'
    # inDir = normpath(inDir)

    # #get list of processed files
    # logSuccess = normpath(inDir + normpath('/processedFiles.txt'))
    # logFailed = normpath(inDir + normpath('/failedFiles.txt'))
    # rawFiles = getFilesInDirectory(inDir)

    # processedFiles = getProcessedFiles(logSuccess)
    # processedFiles += getProcessedFiles(logFailed)

    # All work is stored in the type2 subfolder of the main data folder
    baseDirAbs = cleanPath(baseDir + '/type2/counted/')
    
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

    #  
    urlAttributeDictionary = createURLAttributes(nodeConnectivity)

    pd.DataFrame(urlAttributeDictionary).transpose().to_csv(cleanPath(baseDirAbs+'/linkAttributes.csv'), encoding='utf8')
    urlAttributesDictToSortedCSVs(urlAttributeDictionary, baseDirAbs)

# This only needs to be set when running the script individually
baseDirStandalone = "../memexGithubLargeDataTest/data/"
if __name__=='__main__':
    runPipeLine(baseDirStandalone)