# Datapoint
Recreating the Java codes in Python

# Content:
- Detail Flow of [ConsolidatedFileCreation.py](README.md#detailed-flow-of-consolidatedfilecreationpy-)
- Short version of [ConsolidatedFileCreation.py](/README.md#short-flow-chart-)
- Flow of [FinalFileCreation.py](/README.md#flow-of-finalfilecreationpy-)

## Detailed Flow of ConsolidatedFileCreation.py: <br> 

```mermaid
flowchart TB
    A[ConsolidatedFileCreation.py] -->|imports, variable configs| B
    subgraph B[MTO file consolidation]
    B1[Download MOT DAT files from url. <br> The noOfFile count is in config.ini] --> B2[Delete old MTO txt files.<br>Older files which are <br>more than noOfFiles]
    B2 --> B3[Rename the DAT files in to txt<br>Also change the Date format<br> in the filename from <br> DDMMYYYY to YYYYMMDD]
    B3 --> B4[Consolidate all the MTO txt<br>files into csv file<br>MergedDeliveryData.csv]
    end
    B --> C
    subgraph C[Future OI data consolidation]
    C1[Delete the outputfile] --> C2[Download the latest Zip file from url<br>this latest file is identified by a <br> variable from MOT file consolidation<br>If this latest file is not available in <br>website, then exit the proess]
    C2 --> C4[Extract the zip file<br>And consolidate the data<br> in that file across the SYMBOL]
    end
    C --> D
    subgraph D[Sector Data file creation]
    D1[Combine all ind_nifty*csv <br> Note: All these files need <br>to be downloaded Manually Ocassionally.] --> D2[SectorName.csv]
    end
    D --> E
    subgraph E[Few other files download. <br> Auto Download: <br> 1. sec_bhavdata_full.csv <br> 2. block.csv <br> 3. bulk.csv]
    end
```

## Short Flow chart: <br>

```mermaid
flowchart TB
    A[Consolidated<br>FileCreation.py] -->|imports, variable configs| B
    subgraph B[MTO file consolidation]
    B1[Download <br>MTO_DDMMYYYY.DAT] --> B2[Delete old MTO txt files.<br>MTO_YYYYMMDD.txt]
    B2 --> B3[Rename 'MTO_DDMMYYYY.DAT'<br>'MTO_YYYYMMDD.txt']
    B3 --> B4[Consolidate all<br> 'MTO_YYYYMMDD.txt' to <br> 'MergedDeliveryData.csv']
    end
    B --> C
    subgraph C[Future OI data consolidation]
    C1[Delete old outputfile<br>FandO_Output.csv] --> C2[Download zip file<br> fo10MAR2023bhav.csv.zip.<br> Extract 'fo10MAR2023bhav.csv']
    C2 --> C4[Consolidate <br>fo10MAR2023bhav.csv to <br> FandO_Output.csv]
    end
    C --> D
    subgraph D[Sector Data file creation]
    D1[ind_nifty*csv] --> D2[SectorName.csv]
    end
    D --> E
    subgraph E[Few other files download. <br> Auto Download: <br> 1. sec_bhavdata_full.csv <br> 2. block.csv <br> 3. bulk.csv]
    end
```

## Flow of FinalFileCreation.py: <br>

```mermaid
flowchart TB
    A[FinalFileCreation.py] -->|imports, variable configs| B
    subgraph B[Creation of FinalDeliveryData.csv]
    B1[MergedDeliveryData.csv] --> B2[Copy the Script col and<br>remove the duplicates]
    B2 --> B3[Create new file <br>'FinalDeliveryData.csv']
    end
    B --> C
    subgraph C[Update FinalDeliveryData.csv]
    C2[Include Sector from <br> 'SectorName.csv']
    C2 --> C4["Include <br>(PrevClose,TodayClose, <br>PriceMov,PriceChg,1DayCandle)"]
    C4 --> C5[Include Bulk/Block]
    C5 --> C6[Included DelPerDays]
    C6 --> C7[YETTO: Included Futures<br>data ]
    end
    C --> END
```
