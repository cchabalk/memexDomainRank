import os
from os import listdir
from os.path import isfile, join, normpath
import sys
import gzip
import ujson
import urllib
import time
import gc
from multiprocessing import Pool
import signal

def cleanPath(inPath):
    outPath = normpath(inPath)
    outPath = os.path.abspath(outPath)
    return outPath

def getLogFilePaths(inPath, textModifier):
    logSuccessFile = '/processedFiles' + textModifier + ".txt"
    logFailFile = 'failedFiles' + textModifier + ".txt"

    logSuccess = cleanPath(inPath + normpath(logSuccessFile))
    logFailed = cleanPath(inPath + normpath(logFailFile))

    return logSuccess, logFailed

def getFilesInDirectory(inDir):
    inDir = normpath(inDir)
    onlyFilesTemp = [normpath(os.path.join(inDir, f)) for f in listdir(inDir) if (isfile(join(inDir, f)))]

    onlyFiles = []
    #do not return log files
    for line in onlyFilesTemp:
        try:
            if (line[-3:] != 'txt'):
                onlyFiles.append(line)
        except:
            pass

    # If using on MacOS, .DS_Store is often created by the system and should be ignored
    for element in onlyFiles:
        if ".DS_Store" in element:
            onlyFiles.remove(element)

    return onlyFiles

def getProcessedFiles(inFile):
    inFile = normpath(inFile)
    processedFiles = []
    try:
        with open(inFile) as fp:
            #processedFiles = fp.readlines()
            processedFiles = fp.read().splitlines()
    except:
        processedFiles = []
    return processedFiles

def writeLogFile(logFilePath,processedFileName):
    logFilePath = normpath(logFilePath)
    processedFileName = normpath(processedFileName)
    if os.path.isfile(logFilePath) == False:
        with open(logFilePath,'w+') as fp: #create the log file
            pass

    with open(logFilePath,'a+') as fp:
        fp.write(processedFileName + '\n')

def writeFailedLog(logFilePath,failedFileName):
    logFilePath = normpath(logFilePath)
    failedFileName = normpath(failedFileName)
    if os.path.isfile(logFilePath) == False:
        with open(logFilePath,'w+') as fp: #create the log file
            pass

    with open(logFilePath,'a+') as fp:
        fp.write(failedFileName + '\n')

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)