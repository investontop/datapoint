# 2023-03-11 Recreate the java code ConsolidatedFileCreation.java

# import
import configparser
from datetime import datetime
import os
import pandas as pd
import glob
import zipfile


# import - Own utils
from utility import util

# read configs
config = configparser.ConfigParser()        # instance
configFile = 'config-test.ini'              # Configfile name - We can change this file name as per our requirement
config.read(configFile)

print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+']' + " ConsolidatedFileCreation Started - Considering config: ["
      + configFile + "]")

# Variables assigning [common variables]
sourcePath = config['datapoints-ConsolidatedFileCreation']['sourcePath']
destinationpath = config['datapoints-ConsolidatedFileCreation']['destinationpath']


# ################  [START] Works on Merging the MTO files
print()
print(" 	Delivery DataPoint file consolidation - Started")

# Variables assigning [Merging the MTO files]
outputFile = config['datapoints-ConsolidatedFileCreation']['outputFile']
MTOfileCount = config['datapoints-ConsolidatedFileCreation']['MTOfileCount']

# Download the MTO DAT files
latestFileDate, date_count = util.downloadMTOfiles(sourcePath, int(MTOfileCount), '        ')
# used the above variable [latestFileDate] down while calling a function to download foddMMMyyyybhav.csv

# Delete the older MTO files
util.deleteoldmto(sourcePath, int(date_count), '          ')

# change the filename of "security wise del position" from MTO_DDMMYYYY.DAT to MTO_YYYYMMDD.txt
for filename in os.listdir(sourcePath):
    if filename.endswith('DAT') and filename.startswith('MTO'):
        name, ext = os.path.splitext(filename)
        # print(name)
        newName = (name[:4] + name[-4:] + name[6:8] + name[4:6])
        # Delete the TXT file if already present
        util.checkAndDeleteFile(sourcePath+"\\" + newName + '.txt', 'N', newName + '.txt', '')
        os.rename(os.path.join(sourcePath, filename), os.path.join(sourcePath, newName + '.txt'))

# Check if the file is open by trying to open it in write mode
try:
    with open(destinationpath+"\\" + outputFile, mode='w') as f:
        pass
except PermissionError:
    print(f"Warning: The file [{outputFile}] is open and cannot be deleted.")
    exit()
else:
    # If the file is not open, delete it
    util.checkAndDeleteFile(destinationpath+"\\" + outputFile, 'Y', outputFile, '        ')

# Get all the txt files in the sourcePath directory
txt_files = glob.glob(sourcePath + '\\MTO*.txt')

# Initialize an empty list to hold the data from all the txt files
data = []

NoofFiles = 0

# Loop through each txt file and append its data to the dataframe
for txt_file in txt_files:
    NoofFiles = NoofFiles + 1
    # Extract the file name from the file path
    file_name = os.path.basename(txt_file)
    # Read the txt file into a dataframe
    temp_df = pd.read_csv(txt_file, sep=',', skiprows=4, header=None, names=['RecordType', 'Sno', 'Script', 'Type',
                                                                             'QtyTraded', 'DelQty', '%Del'])
    # Add a new column to the dataframe with the Date name
    temp_df['Date'] = file_name[4:12]
    # Filter the dataframe to include only records with record-type = 20 and Type = 'EQ'
    temp_df = temp_df[(temp_df['RecordType'] == 20) & (temp_df['Type'] == 'EQ')]
    # Append the data to the list
    data.append(temp_df)

# Concatenate the dataframes in the list into a single dataframe
df = pd.concat(data, ignore_index=True)

# Sort the merged data by the 'Script' field
df = df.sort_values(['Script', 'Date'])

# Move the file name column to the front of the dataframe
cols = df.columns.tolist()
cols = [cols[-1]] + cols[:-1]
df = df[cols]

# Write the merged data to a csv file without header
df.to_csv(sourcePath+"\\" + outputFile, index=False, header=False)

print(f"		Number of 'MTO' Files Merged [{NoofFiles}] and created the new file [{outputFile}]")
print(" 	Delivery DataPoint file consolidation - Completed")
# ################  [END] Merging the MTO files done.

# ################  [START] Future OI data consolidation
print("")
print(" 	Future OI data consolidation - Started")
# Variables assigning [Future OI data consolidation]
FutureOutFileName = config['datapoints-ConsolidatedFileCreation']['FutureOutFileName']

"""
# Find the latest file to consolidate. File Format [foDDMMMYYYYbhav.csv]
# Get a list of all files in the directory
files = os.listdir(sourcePath)
# Filter the list to only include csv files with the correct format
csv_files = [f for f in files if f.endswith('.csv') and f.startswith('fo') and len(f) == 19]
# created a function to return the latest file
latest_file = util.latest_fo_file(csv_files)
print(latest_file)
"""

# Delete if the file [FandO_Output.csv] is already present
util.checkAndDeleteFile(destinationpath+"\\" + FutureOutFileName, 'Y', FutureOutFileName, '        ')

# Download the latest zip file
zip_file = util.downloadfofiles(sourcePath, latestFileDate, '       ')

# Extract the zip file
# open the zip file in read mode
if zip_file.endswith("zip"):
    with zipfile.ZipFile(sourcePath + "\\" + zip_file, 'r') as zip_ref:
        # extract all the contents of the zip file to the specified directory
        zip_ref.extractall(sourcePath)

print(" 	Future OI data consolidation - Completed")


print("")
print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+']' + " ConsolidatedFileCreation Completed")