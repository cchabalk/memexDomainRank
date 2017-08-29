import fileSupportFunctions

parseLinks = __import__('010parseLinksParallel')
countLinks = __import__('020countLinks')
forrevIndices = __import__('030ForwardReverseIndices')

## The directory where the data resides, all subfolders will be created within ##
# BASE_DIR = "../memexGithub/data/"
#BASE_DIR = "./mediumDataTest/"
#BASE_DIR = "/home/user1/projects/memexCrawlDivergence/HGData/"
#BASE_DIR = "/home/user1/projects/memexCrawlDivergence/JPLData/"
BASE_DIR = "/home/user1/projects/memexCrawlDivergence/NYUData/"




CONFIG_FILE_PATH = "./config"

# load in the configuration file, use the default if unable to load
config = fileSupportFunctions.loadconfig(CONFIG_FILE_PATH)

typesToParseLinks = config["types to parse"]

if __name__ == '__main__':
	## parse the links from the raw dump ##
	parseLinks.runPipeLine(BASE_DIR, config)
	for typeToParse in typesToParseLinks:
		countLinks.runPipeLine(BASE_DIR, typeToParse,config)
		forrevIndices.runPipeLine(BASE_DIR, typeToParse, config)