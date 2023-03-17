import os
from datetime import datetime, timedelta
import requests
import shutil
import sys
import re


def checkAndDeleteFile(file, displaymsg, filename, intend):
    if os.path.exists(file):
        os.remove(file)
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
                    latestFileDate = datetime.strptime(latestFileDate, "%d%m%Y")
                    latestFileDate = latestFileDate.strftime("%d%b%Y")
                count = count + 1
            else:
                print(intend + 'Data not available. [' + filedownload + ']')
        datestring = datetime.strptime(datestring, "%Y%m%d") - timedelta(days=1)
        datestring = datestring.strftime("%Y%m%d")

    return latestFileDate, counta


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
