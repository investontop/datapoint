import os
from datetime import datetime, timedelta
import requests
import shutil
import sys
import re
import csv
import configparser
import urllib.request
import pandas as pd


def initiate_env_var(whattoreturn):
    if whattoreturn == 'ConfigFile':
        return 'config.ini'

# read configs
config = configparser.ConfigParser()        # instance
configFile = initiate_env_var('ConfigFile')
config.read(configFile)

def checkAndDeleteFile(filepath, displaymsg, filename, intend):
    if os.path.exists(filepath):
        os.remove(filepath)
        if displaymsg == 'Y':
            print(intend + "Deleted the existing file [" + filename + "]")


"""
def latest_fo_file(csv_files):
    # Convert the filenames to datetime objects
    dates = [datetime.strptime(f[2:-8], '%d%b%Y') for f in csv_files]

    # Find the index of the latest date
    latest_index = dates.index(max(dates))

    # Get the filename of the latest file
    return csv_files[latest_index]
"""


def downloadMTOfiles(sourcepath, noOfFiles, intend):
    datestring = datetime.now()
    datestring = datestring.strftime("%Y%m%d")

    count = 0
    counta = 0
    while count < noOfFiles:
        counta = counta + 1
        if os.path.exists(sourcepath+"\\" + 'MTO_' + datestring + '.txt'):
            if count == 0:
                latestFileDate = datetime.strptime(datestring, "%Y%m%d")
                latestFileDate = latestFileDate.strftime("%d%b%Y")
                secfileDate = datetime.strptime(datestring, "%Y%m%d")
                secfileDate = secfileDate.strftime("%d%m%Y")
            count = count + 1
        else:
            filedownload = 'MTO_' + datestring[6:8] + datestring[4:6] + datestring[:4] + '.DAT'
            url = 'https://www1.nseindia.com/archives/equities/mto/'+filedownload
            file_path = sourcepath + '\\' + filedownload
            response = requests.get(url)
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                print(intend + 'Data downloaded successfully. [' + filedownload + ']')
                if count == 0:
                    latestFileDate = filedownload[4:12]
                    secfileDate = filedownload[4:12]
                    latestFileDate = datetime.strptime(latestFileDate, "%d%m%Y")
                    latestFileDate = latestFileDate.strftime("%d%b%Y")
                count = count + 1
            else:
                print(intend + 'Data not available. [' + filedownload + ']')
        datestring = datetime.strptime(datestring, "%Y%m%d") - timedelta(days=1)
        datestring = datestring.strftime("%Y%m%d")

    return latestFileDate, secfileDate, counta


def deleteoldmto(sourcepath, cutoffdays, intend):
    # Define the date to compare against
    cutoff_date = datetime.now() - timedelta(days=cutoffdays)

    # Loop through all files in the directory
    for filename in os.listdir(sourcepath):
        # Use regular expressions to extract the date from the filename
        match = re.match(r"MTO_(\d{8}).txt", filename)
        if match:
            # Convert the date string to a datetime object
            file_date = datetime.strptime(match.group(1), "%Y%m%d")
            # Compare the file date to the cutoff date
            if file_date < cutoff_date:
                # Delete the file
                os.remove(os.path.join(sourcepath, filename))
                print(intend + f"Deleted file: {filename}")


def downloadfofiles(sourcepath, latestfiledate, intend):

    filetodownload = 'fo' + latestfiledate.upper() + 'bhav.csv'

    if os.path.exists(sourcepath+"\\" + filetodownload):
        print(intend + 'fo file already present: [' + filetodownload + ']')
        return filetodownload
    else:
        url = 'https://archives.nseindia.com/content/historical/DERIVATIVES/' + latestfiledate.upper()[5:9] + '/' \
              + latestfiledate.upper()[2:5] + '/' + filetodownload + '.zip'
        file_path = sourcepath + '\\' + filetodownload + '.zip'
        try:
            # Send an HTTP request to the URL and get the response
            response = requests.get(url, stream=True, timeout=10)

            # Raise an exception if the response status code is not OK (200)
            response.raise_for_status()

            # Open the file in binary write mode and write the contents of the response to it
            with open(file_path, 'wb') as f:
                shutil.copyfileobj(response.raw, f)

            # Close the response to free up system resources
            response.close()
            print(intend + 'Data downloaded successfully. [' + filetodownload + '.zip]')
            return filetodownload + '.zip'

        except requests.exceptions.RequestException as e:
            # Handle any exceptions that occur during the request
            print(intend + 'Error downloading file: [' + filetodownload + '.zip] File not available in site')
            print(intend + 'TRY again LATER')
            print(" 	Future OI data consolidation - Completed")
            sys.exit(1)  # exit the process with a non-zero exit code


def consolidatefofile(sourcepath, destinationpath, inputfile, outputfile,  intend):
    inputfileandpath = sourcepath + '\\' + inputfile
    outputfileandpath = destinationpath + '\\' + outputfile

    # Open the input and output CSV files
    with open(inputfileandpath, 'r') as csv_input, open(outputfileandpath, 'w', newline='') as csv_output:

        # Create CSV reader and writer objects
        csv_reader = csv.reader(csv_input)
        csv_writer = csv.writer(csv_output)

        csv_writer.writerow(['SYMBOL', 'CONTRACTS', 'VAL_INLAKH', 'OPEN_INT', 'CHG_IN_OI'])

        # pick the Header - so this will get skipped in For Loop
        header_row = next(csv_reader)

        # Initialize a dictionary to consolidate the date
        consolidated_data = {}

        # Process each row in the input CSV file
        for row in csv_reader:
            if row[0] == 'FUTSTK':
                symbol = row[1]
                contracts = int(row[10])
                val_inlakh = float(row[11])
                open_int = int(row[12])
                chg_in_oi = int(row[13])

                # If this is the first entry for this stock, initialize the totals to zero
                if symbol not in consolidated_data:
                    consolidated_data[symbol] = {'contracts': 0, 'val_inlakh': 0, 'open_int': 0, 'chg_in_oi': 0}

                # Add the cost and shares for this row to the totals for this stock
                consolidated_data[symbol]['contracts'] += contracts
                consolidated_data[symbol]['val_inlakh'] += val_inlakh
                consolidated_data[symbol]['open_int'] += open_int
                consolidated_data[symbol]['chg_in_oi'] += chg_in_oi
            elif row[0] == 'OPTIDX':
                break

        # Write the totals for each stock to the output CSV file
        for symbol, totals in consolidated_data.items():
            csv_writer.writerow([symbol, totals['contracts'], totals['val_inlakh'], totals['open_int'],
                                 totals['chg_in_oi']])

        print(intend + "Inputfile [" + inputfile + "] consolidated in to [" + outputfile + "]")


def download_files(filepath, filename, url, intend):
    file_path = filepath + '\\' + filename
    try:
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
        print(intend + 'Data downloaded successfully. [' + filename + ']')

    except requests.exceptions.RequestException as e:
        # Handle any exceptions that occur during the request
        print(intend + 'Error downloading file: [' + filename + '] File not available in site')
        print(e)
        print(intend + 'TRY again LATER')
        sys.exit(1)  # exit the process with a non-zero exit code

"""
def createfinalDelData(inputFilePath, sectorFilePath, outputFilePath, oputheader):
    df = pd.read_csv(inputFilePath, header=None)
    df_sector = pd.read_csv(sectorFilePath, header=None)

    # merge the two dataframes on Script column using left join
    merged_df = pd.merge(df, df_sector, how='left', left_on=3, right_on=0)
    if oputheader[1] == 'Sector':
        merged_df = merged_df.iloc[:, [3, 9]]
    else:
        merged_df = merged_df.iloc[:, 3]
    merged_df = merged_df.drop_duplicates()

    merged_df.to_csv(outputFilePath, header=False, index=False)
"""

def updatefinalDel_1(inputFilePath, outputFilePath, oputheader, intend):
    df = pd.read_csv(outputFilePath, header=0)          # header=0, then python will consider the first row as Header
    df_sector = pd.read_csv(inputFilePath, header=0)

    # merge the two dataframes on Script column using left join
    merged_df = pd.merge(df, df_sector, how='left', left_on='Script', right_on='Symbol')
    merged_df = merged_df.iloc[:, [0, 2]]

    # Changing the header as per our data
    merged_df.columns = oputheader

    merged_df.to_csv(outputFilePath, header=True, index=False)
    print(intend + '[' + os.path.basename(outputFilePath) + '] Column added [Sector]')


def updatefinalDel_2(inputFilePath, outputFilePath, oputheader, intend):
    df = pd.read_csv(outputFilePath, header=0)
    df_bhav = pd.read_csv(inputFilePath, header=0)

    inputcol = df_bhav.columns
    outputcol = df.columns

    df_bhav = df_bhav[df_bhav[inputcol[1]] == ' EQ']     # inputcol[1] = ' SERIES'

    # merge the two dataframes on Script column using left join
    # outputcol[0] = 'Script' & inputcol[0] = 'SYMBOL'
    merged_df = pd.merge(df, df_bhav, how='left', left_on=outputcol[0], right_on=inputcol[0])
    merged_df = merged_df.iloc[:, [0, 1, 5, 6, 7, 8, 10]]

    # create a new column based on the condition
    def PriceMoveLabel(row):
        if row[' CLOSE_PRICE'] > row[' PREV_CLOSE']:
            return 'UP'
        elif row[' CLOSE_PRICE'] < row[' PREV_CLOSE']:
            return 'DOWN'
        elif row[' CLOSE_PRICE'] == row[' PREV_CLOSE']:
            return 'NO CHANGE'
        else:
            return ''

    def PriceChgPercentage(row):
        if row[' CLOSE_PRICE'] is None:     # Issue: This IF condition is not executing
            return 'xx'
        else:
            x = 100 * (row[' CLOSE_PRICE'] - row[' PREV_CLOSE'])/row[' PREV_CLOSE']
            return "{:.2f}".format(x)

    def OneDayCandle(row):
        if row[' OPEN_PRICE'] < row[' CLOSE_PRICE'] and row[' OPEN_PRICE'] == row[' LOW_PRICE']:
            return 'O=L (Bull)'
        elif row[' OPEN_PRICE'] > row[' CLOSE_PRICE'] and row[' OPEN_PRICE'] == row[' HIGH_PRICE']:
            return 'O=H (Bear)'

    merged_df['PriceMove'] = merged_df.apply(PriceMoveLabel, axis=1)
    merged_df['PriceChg%'] = merged_df.apply(PriceChgPercentage, axis=1)
    # Filter rows where the specific field is not empty
    field_to_check = ' CLOSE_PRICE'
    merged_df = merged_df[merged_df[field_to_check].notnull()]
    merged_df['DayCandle'] = merged_df.apply(OneDayCandle, axis=1)
    # Define the columns you want to delete
    # columns_to_delete = [' OPEN_PRICE', ' HIGH_PRICE', ' LOW_PRICE', 'SYMBOL', ' SERIES', ' DATE1', ' LAST_PRICE', ' AVG_PRICE', ' TTL_TRD_QNTY', ' TURNOVER_LACS', ' NO_OF_TRADES',
     #                    ' DELIV_QTY', ' DELIV_PER']
    columns_to_delete = [' OPEN_PRICE', ' HIGH_PRICE', ' LOW_PRICE']
    # Drop the specified columns
    merged_df = merged_df.drop(columns=columns_to_delete)
    # Changing the header as per our data
    merged_df.columns = oputheader
    merged_df.to_csv(outputFilePath, header=True, index=False)
    print(intend + '[' + os.path.basename(outputFilePath) + '] Column added [PrevClose,TodayClose,PriceMov,PriceChg,1DayCandle]')

def updatefinalDel_3(BulkFilePath, BlockFilePath, outputFilePath, oputheader, intend):

    df = pd.read_csv(outputFilePath, header=0)
    df_bulk = pd.read_csv(BulkFilePath, header=0)
    df_block = pd.read_csv(BlockFilePath, header=0)

    # Check if each company in df1 exists in df2 and create a new column 'Bulk'
    # df['Bulk/Block'] = df['Script'].isin(df_bulk['Symbol']).apply(lambda x: 'Bulk' if x else '')
    # df.to_csv(outputFilePath, header=True, index=False)

    # Check if each company in df exists in df_bulk or df_block and create a new column 'Bulk/Block'
    df['Bulk/Block'] = df['Script'].apply(lambda x: '')
    df.loc[df['Script'].isin(df_bulk['Symbol']), 'Bulk/Block'] += 'Bulk'
    df.loc[df['Script'].isin(df_block['Symbol']), 'Bulk/Block'] += 'Block'

    df.to_csv(outputFilePath, header=True, index=False)

    print(intend + '[' + os.path.basename(outputFilePath) + '] Column added [Bulk/Block]')

def updatefinalDel_4(inputFilePath, outputFilePath, oputheader, DelPerDays, intend):

    def DeliveryPercentage(inputFilePath, outputFilePath, oputCol, noOfDays):
        df = pd.read_csv(outputFilePath, header=0)  # This file have header
        df_mergeDel = pd.read_csv(inputFilePath, header=None, names=['Date', 'RecType', 'SrNo', 'Script', 'Type', 'TradedQty', 'DelQty', 'DelPercentage'])  # This file does not have header - Specifying the Header here as file does not have the header
        df_mergeDel = df_mergeDel.groupby('Script').head(noOfDays) # Picks only the number of Lines as we require in the dataFrame

        df_mergeDel = df_mergeDel.groupby('Script').agg({'TradedQty': 'sum', 'DelQty': 'sum'}).reset_index()

        # Merged both dataframe on col 'Script'
        merged_df = df.merge(df_mergeDel, on='Script', how='left')

        # Calculate the 'DelPercentage' column
        merged_df[oputCol] = (merged_df['DelQty'] / merged_df['TradedQty']) * 100

        # Define the columns you want to delete
        columns_to_delete = ['TradedQty', 'DelQty']
        # Drop the specified columns
        merged_df = merged_df.drop(columns=columns_to_delete)
        # Changing the header as per our data
        # merged_df.columns = oputheader
        merged_df.to_csv(outputFilePath, header=True, index=False)

    def calc_DelTrend(row, index, DayDelList, DelPerDaysHeader):
        trend = None
        to_value = index+len(DayDelList)-1
        j = 1
        for i in range(index, to_value):
            # return(row[index+len(DayDelList)-1])
            """
            if (row[i] > row[i+1] and j == 1):
                #trend = '1D inc %Del'
                trend = row[i]
            elif (j==1):
                return('Need To Develop Logic')
            elif (j > 1 and j < to_value and row[i] > row[i+1] and trend is not None):
                trend = str(DelPerDaysHeader[j-1])+'D inc %Del'
            else:
                trend = None
            """
            if (j==1):
                if(row[i] >= row[i+1]):
                    trend = str(DelPerDaysHeader[j])+'D inc %Del'
                else:
                    trend = str(DelPerDaysHeader[j])+'D dec %Del'
            elif (j <= to_value):
                if(row[i] >= row[i+1] and trend is not None and "inc" in trend):
                    trend = str(DelPerDaysHeader[j])+'D inc %Del'
                elif(row[i] < row[i+1] and trend is not None and "dec" in trend):
                    trend = str(DelPerDaysHeader[j])+'D dec %Del'
                else:
                    return(trend)

            j += 1

        return trend

    for items in DelPerDays:
        DeliveryPercentage(inputFilePath, outputFilePath, items+'DaysDel%', int(items))

    # To calculate the Del%Trend based on the other fields value
    df = pd.read_csv(outputFilePath, header=0)  # This file have header


    # Find the columns with 'DaysDel%' in their names
    day_del_columns = [col for col in df.columns if 'DaysDel%' in col]
    # Find the index of the first column with 'DaysDel%' in its name
    day_del_column_index = next((i for i, col in enumerate(df.columns) if 'DaysDel%' in col), None)

    DelPerDaysHeader = config.get('datapoints-FinalFileCreation', 'DelPerDays')
    DelPerDaysHeader = DelPerDaysHeader.split(',')

    df['Del%Trend'] = df.apply(calc_DelTrend, args=(day_del_column_index, day_del_columns, DelPerDaysHeader), axis=1)

    # Update the DataFrame, Replacing '3D inc %Del', '3D dec %Del' as NULL
    df.loc[df['Del%Trend'].isin(['3D inc %Del', '3D dec %Del']), 'Del%Trend'] = None
    df.to_csv(outputFilePath, header=True, index=False)

    print(intend + '[' + os.path.basename(outputFilePath) + '] Column added [DelPerDays]')

def is_valid_date(date_string):
    try:
        datetime.strptime(date_string, '%Y%m%d')
        return True  # Date is valid
    except ValueError:
        return False  # Date is not valid