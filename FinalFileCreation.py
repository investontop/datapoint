# 2023-03-23 Recreate the java code FinalFileCreation.java

# import
import configparser
from datetime import datetime
import os
import pandas as pd
import glob
import zipfile
import subprocess
import csv


# import - Own utils
from utility import util

# read configs
config = configparser.ConfigParser()        # instance
configFile = util.initiate_env_var('ConfigFile')
config.read(configFile)

print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+']' + " FinalFileCreation Started - Considering config: ["
      + configFile + "]")
print(" ")

# Variables assigning [common variables]
sourcePath = config['datapoints-Common']['sourcePath']
destinationpath = config['datapoints-Common']['destinationpath']
archivepath = config['datapoints-Common']['archivepath']

InPutFile = config['datapoints-FinalFileCreation']['InPutFile']                     # MergedDeliveryData.csv
SectorFile = config['datapoints-ConsolidatedFileCreation']['SectorOutFileName']     # SectorName.csv
OutPutFile = config['datapoints-FinalFileCreation']['OutPutFile']                   # FinalDeliveryData.csv

inputFilePath = os.path.join(destinationpath, InPutFile)
sectorFilePath = os.path.join(destinationpath, SectorFile)
outputFilePath = os.path.join(destinationpath, OutPutFile)

secbhavfile = config['datapoints-Common']['sec_bhavdata_file']
secbhavfilepath = os.path.join(sourcePath, secbhavfile)

util.checkAndDeleteFile(outputFilePath, 'Y', OutPutFile, '        ')

header = config.get('Header', 'OriginalHeader')

# Convert the comma separated string in to a list of strings.
# header = [x for x in header.split(',')]
header = header.split(',')

# util.createfinalDelData(inputFilePath, sectorFilePath, outputFilePath, header)

# Create OutPutFile from inputfile by removing duplicates on the col 3
# Dataframe for inputFile
df_input = pd.read_csv(inputFilePath, header=None)
df_input = df_input.iloc[:, 3]
df_input = df_input.drop_duplicates()
df_input.to_csv(outputFilePath, header=[header[0]], index=False)

i = 1
while i <= len(header)-1:
    if header[i] == 'Sector':
        util.updatefinalDel_1(sectorFilePath, outputFilePath, header[:i+1])      # To include the [Sector]
        i += 1
    elif header[i] == 'PrevClose':      # Since [PrevClose,TodayClose,PriceMov,PriceChg,1DayCandle] is taken/calculated from same file, we check only 1 field
        util.updatefinalDel_2(secbhavfilepath, outputFilePath, header[:i+5])     # To include [PrevClose,TodayClose,PriceMov,PriceChg,1DayCandle]
        i += 5

print("")
print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+']' + " FinalFileCreation Completed")
