"""
Here we can read in an existing nodeConnection dictionary file and output the relevant ranked lists
"""
from fileSupportFunctions import *
from urlFeatureExtraction import createURLAttributes, createUrlAttributesFromFile, urlAttributesDictToSortedCSVs

# this will only be used when running the function standalone
baseDir = '../memexGithubLargeDataTest/data/'


if __name__ == '__main__':

    # load in the configuration file, a default is loaded if config file not found
    config = loadconfig(CONFIG_FILE_PATH)

    # extract the set of link types to work on
    typesToParseLinks = config["types to parse"]

    # Run the pipeline for each link type
    for typeToParse in typesToParseLinks:
	    nodeConnectivityFileLocation = baseDir + '/' + typeToParse + '/counted/nodeConnectivity.jl.gz'

	    # perform the actual parsing
		urlAttributeDict = createUrlAttributesFromFile(nodeConnectivityFileLocation)
		urlAttributesDictToSortedCSVs(urlAttributeDict, nodeConnectivityFileLocation)