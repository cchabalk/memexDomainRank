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
import smart_open
# from fileSupportFunctions import cleanPath, getLogFilePaths, getFilesInDirectory, getProcessedFiles, writeLogFile, writeFailedLog, ensure_dir
from fileSupportFunctions import *

def createURLAttributes(nodeConnectivityDict):
    # Weighted directed graph to hold the website connections
    G = nx.DiGraph()

    # Append all unique edges. Currently assumes no duplicated edges
    edges = []
    for parentURL, childrenURLs in nodeConnectivityDict.items():
        for childURL, childCount in childrenURLs.items():
            edges.append((parentURL, childURL, childCount))
    G.add_weighted_edges_from(edges)

    # Create a blank dictionary to hold the attributes of each URL in the graph
    URL_attr = {parentURL: {'outdeg': 0, 'indeg': 0, 'uniquein': 0, 'uniqueout': 0, 'pagerank': 0.0} \
                for parentURL in G.nodes()}

    # Find unique number of outgoing edges, total edge weight of outgoing edges
    uniqueout = G.out_degree()
    outdeg = G.out_degree(weight='weight')

    # Find unique number of incoming edges, total edge weight of incoming edges
    uniquein = G.in_degree()
    indeg = G.in_degree(weight='weight')

    # Store these values in a dictionary we can use for ML or any other ranking alg
    for URL in G.nodes():
        URL_attr[URL]['uniqueout'] = uniqueout[URL]
        URL_attr[URL]['uniquein'] = uniquein[URL]
        URL_attr[URL]['outdeg'] = outdeg[URL]
        URL_attr[URL]['indeg'] = indeg[URL]
        URL_attr[URL]['uin_uout_ratio'] = 0.0 if uniqueout[URL]==0 else float(uniquein[URL])/uniqueout[URL]
        URL_attr[URL]['in_out_ratio'] = 0.0 if outdeg[URL]==0 else float(indeg[URL])/outdeg[URL]

    # Calculate and store the weighted and unweighted pagerank for each
    for URL, prval in nx.pagerank(G, weight='weight').iteritems():
        URL_attr[URL]['pagerank'] = prval
    for URL, prval in nx.pagerank(G, weight=None).iteritems():
        URL_attr[URL]['pagerank_noweight'] = prval

    return URL_attr #the dictionary with all URL nodes and their calculated attributes

def createUrlAttributesFromFile(nodeConnectivityFileAbs):
	# Load in the json file. File format is one line of json, many entries
	with smart_open.smart_open(nodeConnectivityFileAbs) as fp:
	    data = []
	    try:
	        data = [fp.next()]
	    except:
	        data = []

	# Convert this node connectivity string into real json
	nodeCon = ujson.loads(data[0])

	# Parse using the urlFeatureExtraction pipeline
	urlAttributeDictionary = createURLAttributes(nodeCon)

	return urlAttributeDictionary

def urlAttributesDictToSortedCSVs(urlAttributeDictionary, nodeConnectivityFileAbs):
	
	baseDirAbs = cleanPath(os.path.split(nodeConnectivityFileAbs)[0])

	dfUrl = pd.DataFrame(urlAttributeDictionary).transpose()

	featureCols = ['uniqueout', 'uniquein', 'outdeg', 'indeg', 'uin_uout_ratio', 'in_out_ratio', 'pagerank', 'pagerank_noweight']
	outDir = baseDirAbs + '/nodeConnectivityLists/'
	ensure_dir(outDir)

	for featureCol in featureCols:
	    # Output sorted by the various possible parameters
		dfUrl.sort_values(featureCol,ascending=False).to_csv(cleanPath(outDir + featureCol + '.csv'), encoding='utf8')

