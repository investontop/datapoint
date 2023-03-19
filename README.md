# datapoint
Recreating the Java codes in Python

Testing few scenarios

```mermaid
flowchart TB
    A[Consolidated<br>FileCreation.py] -->|imports, variable configs| B
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
```
