parseLinks = __import__('010parseLinksParallel')
countLinks = __import__('020countLinks')
forrevIndices = __import__('030ForwardReverseIndices')

## The directory where the data resides, all subfolders will be created within ##
BASE_DIR = "../memexGithub/data/"

if __name__ == '__main__':
	## parse the links from the raw dump ##
	parseLinks.runPipeLine()
	countLinks.runPipeLine()
	forrevIndices.runPipeLine()