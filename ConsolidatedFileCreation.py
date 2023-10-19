# 2023-03-11 Recreate the java code ConsolidatedFileCreation.java
# 2023-03-24 Included File download portion
    # sec_bhavdata_DDMMYYYY.csv + bulk.csv + block.csv
# 2023-10-02 Included a Validation between [MTOfileCount] & [DelPerDays]

# import
import configparser
from datetime import datetime
import os
import pandas as pd
import glob
import sys
import zipfile
import subprocess


# import - Own utils
from utility import util

# read configs
config = configparser.ConfigParser()        # instance
configFile = util.initiate_env_var('ConfigFile')
config.read(configFile)

print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+']' + " ConsolidatedFileCreation Started - Considering config: ["
      + configFile + "]")
print('Note: Exit Code 0 means Success')

# Variables assigning [common variables]
sourcePath = config['datapoints-Common']['sourcePath']
destinationpath = config['datapoints-Common']['destinationpath']



# ################  [START] Works on Merging the MTO files
print()
print(" 	Delivery DataPoint file consolidation - Started")

# Variables assigning [Merging the MTO files]
outputFile = config['datapoints-ConsolidatedFileCreation']['outputFile']
MTOfileCount = config['datapoints-ConsolidatedFileCreation']['MTOfileCount'] #TODO: We can take the last value from DelPerDays. In this case we do not require the below Validation.

DelPerDays = config.get('datapoints-FinalFileCreation', 'DelPerDays')
DelPerDays = DelPerDays.split(',')


if int(DelPerDays[-1]) > int(MTOfileCount):
    print('         [Validation] Please set the config correct [MTOfileCount] must be greater than or equal to [DelPerDays]''s last value')
    print('')
    print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+']' + " ConsolidatedFileCreation Completed with Warning!!")
    exit()

# Download the MTO DAT files
latestFileDate, secfileDate, date_count = util.downloadMTOfiles(sourcePath, int(MTOfileCount), '        ')
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
df = df.sort_values(['Script', 'Date'], ascending=[True, False])

# Move the file name column to the front of the dataframe
cols = df.columns.tolist()
cols = [cols[-1]] + cols[:-1]
df = df[cols]

# Write the merged data to a csv file without header
df.to_csv(destinationpath+"\\" + outputFile, index=False, header=False)

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
zip_file = util.downloadfofiles(sourcePath, latestFileDate, '        ')

# Extract the zip file
# open the zip file in read mode
if zip_file.endswith("zip"):
    with zipfile.ZipFile(sourcePath + "\\" + zip_file, 'r') as zip_ref:
        # extract all the contents of the zip file to the specified directory
        extractFile = zip_ref.namelist()[0]
        zip_ref.extractall(sourcePath)
else:
    extractFile = zip_file

# Get all the fo files in the sourcePath directory
fo_files = glob.glob(sourcePath + '\\fo*bhav*')
# Remove the other old files
for fo_file in [f for f in fo_files if not f.endswith(extractFile)]:
    os.remove(fo_file)

# Consolidate the data in the fo file
util.consolidatefofile(sourcePath, destinationpath, extractFile, FutureOutFileName, '        ')

print(" 	Future OI data consolidation - Completed")
# ################  [END] Future OI data consolidation

# ################  [START] Sector data file creation

print("")
print(" 	Sector data file creation - Started")

"""
# Download Sector files - Commenting these functions as this is not working as expected. The py keeps on running for 
# long time. So expecting manual download for these files. Occasionally we can download these files.
# util.download_files(sourcePath, 'ind_nifty500list.csv', 'https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv')
# util.download_files(sourcePath, 'ind_niftymidcap100list.csv', 'https://www.niftyindices.com/IndexConstituent/ind_niftymidcap100list.csv')
# util.download_files(sourcePath, 'ind_niftysmallcap250list.csv', 'https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv')
# util.download_files(sourcePath, 'ind_niftylargemidcap250list.csv', 'https://www.niftyindices.com/IndexConstituent/ind_niftylargemidcap250list.csv')
# util.download_files(sourcePath, 'ind_niftymidsmallcap400list.csv', 'https://www.niftyindices.com/IndexConstituent/ind_niftymidsmallcap400list.csv')
"""

# Variable
SectorFile = config['datapoints-ConsolidatedFileCreation']['SectorOutFileName']
SectorFilePath = os.path.join(destinationpath, SectorFile)

# Combine the Sector data in a separate csv
# Initialize an empty dataframe to store the combined data
combined_df = pd.DataFrame()

for filename in os.listdir(sourcePath):
    if filename.startswith('ind_nifty') and filename.endswith('csv'):
        file_path = os.path.join(sourcePath, filename)  # Join the directory path and filename
        df = pd.read_csv(file_path, usecols=[2, 1])
        # Concatenate the data to the combined dataframe
        combined_df = pd.concat([combined_df, df], ignore_index=True)

# Move the column
cols = combined_df.columns.tolist()
cols = [cols[-1]] + cols[:-1]   # takes the last element of the list, and concatenates it with the rest of the list,
# which is obtained by slicing the last element
combined_df = combined_df[cols]

# Sort the dataframe
combined_df = combined_df.sort_values(by=['Symbol'], ascending=True)

# Drop duplicates based on all columns except the index
combined_df = combined_df.drop_duplicates(keep='first')

# Save the combined data to a new file named "SectorName.csv"
combined_df.to_csv(SectorFilePath, index=False)
print("         Sectorfile created [" + SectorFile + "]")

print(" 	Sector data file creation - Completed")
# ################  [END] Sector data file creation

# ################  [START] File download.
print("")
print(" 	Few other Files download - Started")

sec_bhavdata_link = config['datapoints-Common']['sec_bhavdata_link']
sec_bhavdata_link = sec_bhavdata_link.replace("DDMMYYYY", secfileDate)
sec_bhavdata_file = config['datapoints-Common']['sec_bhavdata_file']
util.download_files(sourcePath, sec_bhavdata_file, sec_bhavdata_link, '         ')

block_link = config['datapoints-Common']['block_link']
blockfile = config['datapoints-Common']['blockfile']
bulk_link = config['datapoints-Common']['bulk_link']
bulkfile = config['datapoints-Common']['bulkfile']
util.download_files(sourcePath, blockfile, block_link, '         ')
util.download_files(sourcePath, bulkfile, bulk_link, '         ')

print(" 	File download - Completed")
# ################  [END] File download.

print("")
print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+']' + " ConsolidatedFileCreation Completed")


# execute example.py
# subprocess.call(['python', 'example.py'])