# 2023-10-20 Idea of this script is to get the Delivery percentage details of any specific Script at any
# specific date in adhoc basis.

# import
import configparser
import sys
import os
import pandas as pd
import subprocess
from datetime import datetime

# import - Own utils
from utility import util
from utility import util_adhoc

# read configs
config = configparser.ConfigParser()        # instance
configFile = util.initiate_env_var('ConfigFile')
config.read(configFile)

print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+']' + " adHoc DelPercentage Started - Considering config: ["
      + configFile + "]")
print('Note: Exit Code 0 means Success')
print('')

intend = '          '

# Variables assigning [common variables]
adhocMTOpath = config['adhoc']['adhocMTOpath']
DelPerDays = config.get('datapoints-FinalFileCreation', 'DelPerDays')
DelPerDays = DelPerDays.split(',')
MTOfileCount = (int(DelPerDays[-1]))

if not os.path.exists(adhocMTOpath):
    os.makedirs(adhocMTOpath)

script = input(intend + 'Enter the script name: ')
script = script.upper()
date = input(intend + 'Enter the Date in [YYYYMMDD] format: ')
print('')
print(intend+'Need to DownLoad MTOfilecount: {}'.format(MTOfileCount))
print('')
# Validate the entered date format
if util.is_valid_date(date):
    date = datetime.strptime(date, '%Y%m%d')
else:
    print('')
    print(intend + "Warning!! \n" + intend + "Please enter valid Date in [YYYYMMDD] format")
    sys.exit()

# Delete all MTO files
util_adhoc.deletemto_adhoc(adhocMTOpath, intend, 'log-NO')

# Download the necessary MTO files
date_count = util_adhoc.downloadMTOfiles_adhoc(adhocMTOpath, int(MTOfileCount), date, intend, 'log-NO')

# Rename the MTO file as per our need. DAT to TXT and the Date format aswell
for filename in os.listdir(adhocMTOpath):
    if filename.endswith('DAT') and filename.startswith('MTO'):
        name, ext = os.path.splitext(filename)
        # print(name)
        newName = (name[:4] + name[-4:] + name[6:8] + name[4:6])
        # Delete the TXT file if already present
        util.checkAndDeleteFile(adhocMTOpath+"\\" + newName + '.txt', 'N', newName + '.txt', '')
        os.rename(os.path.join(adhocMTOpath, filename), os.path.join(adhocMTOpath, newName + '.txt'))

txtFiles = [filename for filename in os.listdir(adhocMTOpath) if filename.startswith('MTO') and filename.endswith('txt')]

# Initialize an empty list
data = []
for filename in txtFiles:
    # Read the txt file into a dataframe
    # By specifying names below, you are essentially providing labels for each column in the resulting DataFrame.
    temp_df = pd.read_csv(os.path.join(adhocMTOpath, filename), sep=',', skiprows=4, header=None, names=['RecordType', 'Sno', 'Script', 'Type',
                                                                             'QtyTraded', 'DelQty', '%Del'])

    # Add a new column to the dataframe with the Date name
    temp_df['Date'] = filename[4:12]
    # Filter the dataframe to include only records with record-type = 20 and Type = 'EQ'
    temp_df = temp_df[(temp_df['RecordType'] == 20) & (temp_df['Type'] == 'EQ') & (temp_df['Script'] == script)]
    # Append the data to the list
    data.append(temp_df)

# Concatenate the data list into a single dataframe
inputdf = pd.concat(data, ignore_index=True)
# Sort the merged data by the 'Script' and 'Date' field
inputdf = inputdf.sort_values(['Script', 'Date'], ascending=[True, False])

# Create an DataFrame with header and Script
opRecord = {'Script': [script]}
outputDF = pd.DataFrame(opRecord)

outputDF = util_adhoc.updatefinalDel_4_adhoc(inputdf, outputDF, DelPerDays, intend)
print(outputDF)

outputDF.to_csv(os.path.join(adhocMTOpath, script +'.txt'), sep='\t', index=False)

print("")
print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+']' + " adHoc DelPercentage Completed")