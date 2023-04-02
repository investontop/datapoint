import os
from datetime import datetime, timedelta
import requests
import shutil
import sys
import re
import csv
import urllib.request
import pandas as pd


def initiate_env_var(whattoreturn):
    if whattoreturn == 'ConfigFile':
        return 'config-test.ini'


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


def updatefinalDel_1(inputFilePath, outputFilePath, oputheader):
    df = pd.read_csv(outputFilePath, header=0)          # header=0, then python will consider the first row as Header
    df_sector = pd.read_csv(inputFilePath, header=0)

    # merge the two dataframes on Script column using left join
    merged_df = pd.merge(df, df_sector, how='left', left_on='Script', right_on='Symbol')
    merged_df = merged_df.iloc[:, [0, 2]]

    # Changing the header as per our data
    merged_df.columns = oputheader

    merged_df.to_csv(outputFilePath, header=True, index=False)


def updatefinalDel_2(inputFilePath, outputFilePath, oputheader):
    print(oputheader)
    df = pd.read_csv(outputFilePath, header=0)
    df_bhav = pd.read_csv(inputFilePath, header=0)

    inputcol = df_bhav.columns
    outputcol = df.columns

    df_bhav = df_bhav[df_bhav[inputcol[1]] == 'EQ']     # inputcol[1] = ' SERIES'

    # merge the two dataframes on Script column using left join
    # outputcol[0] = 'Sector' & inputcol[0] = 'SYMBOL'
    merged_df = pd.merge(df, df_bhav, how='left', left_on=outputcol[0], right_on=inputcol[0])
