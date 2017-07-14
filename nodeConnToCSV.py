import gzip
import ujson
import numpy as numpy
import pandas as pd
from fileSupportFunctions import *

baseDir = '../memexGithubLargeDataTest/data/'
nodeConnectivityFileLocation = baseDir + '/type2/counted/nodeConnectivity.jl.gz'

if __name__ == '__main__':
	edges = []

	with gzip.open(nodeConnectivityFileLocation) as fp:
		nodeConnectivityDict = ujson.loads(fp.readline())
		for parentURL, childrenURLs in nodeConnectivityDict.items():
			for childURL, childCount in childrenURLs.items():
				edges.append((parentURL, childURL, childCount))

	df = pd.DataFrame(edges, columns=['src','dst','weight'])

	# Filter out nodes that dangle
	dstcount = df.groupby('dst').agg({'dst': 'count'})
	dstcount.columns = ['dstcount']
	dstcount = dstcount[dstcount['dstcount'] > 1]
	dstcount = dstcount.reset_index()
	outputDF = pd.merge(df, dstcount, on='dst')

	outputDir = cleanPath(os.path.dirname(nodeConnectivityFileLocation))

	outputDF.to_csv(outputDir + 'nodeConnectivityGraphLinks.csv', index=False, encoding='utf8')