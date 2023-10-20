import os
from datetime import datetime, timedelta
import requests
import shutil
import sys
import re
import csv
import configparser
import pandas as pd

def downloadMTOfiles_adhoc(sourcepath, noOfFiles, datestring, intend, log):

    datestring = datestring.strftime("%Y%m%d")
    count = 0
    counta = 0
    while count < noOfFiles:
        counta = counta + 1
        if os.path.exists(sourcepath+"\\" + 'MTO_' + datestring + '.txt'):
            count = count + 1
        else:
            filedownload = 'MTO_' + datestring[6:8] + datestring[4:6] + datestring[:4] + '.DAT'
            url = 'https://www1.nseindia.com/archives/equities/mto/'+filedownload
            file_path = sourcepath + '\\' + filedownload
            response = requests.get(url)
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                if log == 'log-YES':
                    print(intend + 'Data downloaded successfully. [' + filedownload + ']')
                count = count + 1
            else:
                if log == 'log-YES':
                    print(intend + 'Data not available. [' + filedownload + ']')
        datestring = datetime.strptime(datestring, "%Y%m%d") - timedelta(days=1)
        datestring = datestring.strftime("%Y%m%d")

    return counta

def deletemto_adhoc(sourcepath, intend, log):

    # Use a list comprehension to filter files that start with 'MTO'
    files_to_delete = [filename for filename in os.listdir(sourcepath) if filename.startswith('MTO')]

    for filename in files_to_delete:
        os.remove(os.path.join(sourcepath, filename))
        if log == 'log-YES':
            print(intend + f"Deleted file: {filename}")

def updatefinalDel_4_adhoc(inputDF, outputDF, DelPerDays, intend):

    def DeliveryPercentage(inputDF, outputDF, oputCol, noOfDays):
        #df = pd.read_csv(outputFilePath, header=0)  # This file have header
        #df_mergeDel = pd.read_csv(inputFilePath, header=None, names=['Date', 'RecType', 'SrNo', 'Script', 'Type', 'TradedQty', 'DelQty', 'DelPercentage'])  # This file does not have header - Specifying the Header here as file does not have the header
        inputDF = inputDF.groupby('Script').head(noOfDays) # Picks only the number of Lines as we require in the dataFrame

        inputDF = inputDF.groupby('Script').agg({'QtyTraded': 'sum', 'DelQty': 'sum'}).reset_index()

        # Merged both dataframe on col 'Script'
        merged_df = outputDF.merge(inputDF, on='Script', how='left')

        # Calculate the 'DelPercentage' column
        merged_df[oputCol] = round(((merged_df['DelQty'] / merged_df['QtyTraded']) * 100), 2)

        # Define the columns you want to delete
        columns_to_delete = ['QtyTraded', 'DelQty']
        # Drop the specified columns
        merged_df = merged_df.drop(columns=columns_to_delete)
        # Changing the header as per our data
        # merged_df.to_csv(outputFilePath, header=True, index=False)

        return merged_df

    def calc_DelTrend(row, index, DayDelList, DelPerDaysHeader):
        trend = None
        to_value = index+len(DayDelList)-1
        j = 1
        for i in range(index, to_value):
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
        outputDF = DeliveryPercentage(inputDF, outputDF, items+'DaysDel%', int(items))

    # Find the columns with 'DaysDel%' in their names
    day_del_columns = [col for col in outputDF.columns if 'DaysDel%' in col]
    # Find the index of the first column with 'DaysDel%' in its name
    day_del_column_index = next((i for i, col in enumerate(outputDF.columns) if 'DaysDel%' in col), None)
    DelPerDaysHeader = DelPerDays

    outputDF['Del%Trend'] = outputDF.apply(calc_DelTrend, args=(day_del_column_index, day_del_columns, DelPerDaysHeader), axis=1)
    # Update the DataFrame, Replacing '3D inc %Del', '3D dec %Del' as NULL
    outputDF.loc[outputDF['Del%Trend'].isin(['3D inc %Del', '3D dec %Del']), 'Del%Trend'] = None

    return outputDF
