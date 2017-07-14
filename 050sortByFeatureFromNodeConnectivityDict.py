"""
Here we can read in an existing nodeConnection dictionary file and output the relevant ranked lists
"""
from fileSupportFunctions import *
from urlFeatureExtraction import createURLAttributes, createUrlAttributesFromFile, urlAttributesDictToSortedCSVs

# this will only be used when running the function standalone
baseDir = '../memexGithubLargeDataTest/data/'
nodeConnectivityFileLocation = baseDir + '/type2/counted/nodeConnectivity.jl.gz'
if __name__ == '__main__':
	urlAttributeDict = createUrlAttributesFromFile(nodeConnectivityFileLocation)
	urlAttributesDictToSortedCSVs(urlAttributeDict, nodeConnectivityFileLocation)