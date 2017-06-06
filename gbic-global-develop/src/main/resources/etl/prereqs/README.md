# Pre-requirements script

These scripts launch a pre-requirements subworkflow per OB, previously to ETL process.

## Pre-requirements

**Name**: `prereqs.sh` (bash script)

**Description**: It has to be installed on HDFS client node and be executed as admin user.  
Takes as arguments the project version and optionally the date for that files and the client local inbox.
Generates those interfaces that are not received and puts them in HDFS.  
It has to have write access to local inbox directory, p.e: ```/sftp/{OB}/GPLATFORM/MSv5```
The path for the installation should be `$GBIC_HOME/etl/prereqs`  
* With ```GBIC_HOME=/opt/gbic/services/gplatform/global```  

**Usage**: (as admin user)
```
Usage: /usr/gbic/services/gplatform/global/etl/prereqs/prereqs.sh [OPTION] {vers-num} \
                                                                           {country} \
                                                                           {file-list} \
                                                                           [{month-id} [{local_path_prefix}]]

Launch pre-requirements of the ETL for the specified OB

    [-i,--ingest]                    Execute ingestion script for generated files.
                                     It won't be executed by default.

Recognized MONTH format:
    YYYYMM

Recognized OPERATOR format:
    Two character country code according to ISO 3166-1 alpha-2 standard
```

**Needed files**:  
For the script to work properly:
* A `generation` directory must exist in `$GBIC_HOME`.  
Inside that directory there will be the generation of files process needed by the OB. For more information about this process, please see [genfile.sh](../../generation/README.md)  


**Arguments passed to the script**:  
Call to `$GBIC_HOME/etl/prereqs.sh` script will take 3 mandatory arguments, and 2 more optionally:  
* `{`_`vers-num`_`}` Major version of Semantic Model document. Example: ```5``` for *MSv5*
* `{`_`country`_`}`  OB in two characters according to [ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2#Officially_assigned_code_elements) standard.
* `{`_`file-list`_`}` List of interfaces to be generated. Example: ```"DIM_M_GROUP_SVA DIM_M_BILLING_CYCLE"``` for *ES*
* `{`_`month-id`_`}` Month in format ```yyyyMM``` of data to be loaded. If not specified, the script will take the previous month. For example, if the script is executed on March, 13th 2015, then it'll take ```201502```.
* `{`_`local_path_prefix`_`}` Only can be specified when ```month-id``` is also present. Tells the script the root directory to find the local files. By default, it's set to ```/sftp``` (where the ```ESP```, ```BRA```, ```ARG```,... directories are).

**Return Codes**:  
* `0`: Script successfully ended
* Other code: see return codes in [genfile.sh](../../etl/generation/README.md)  

**Examples**:
```bash
# Launch the pre-requirements for Spain, of Jan-2015.
/usr/gbic/services/gplatform/global/etl/prereqs/prereqs.sh 5                                     \
                                                           ES                                    \
                                                           "DIM_M_GROUP_SVA DIM_M_BILLING_CYCLE" \
                                                           201501
```
### Pre-requirements for ESP

**Tasks executed**:  

1. Generation and ingestion of files needed by the OB. In this case:  
  
  * *DIM_M_BILLING_CYCLE*
  * *DIM_M_GROUP_SVA*  

### Pre-requirements for BRA

**Tasks executed**:  

1. Generation and ingestion of files needed by the OB. In this case:  
  
  * *DIM_M_BILLING_CYCLE*
  * *DIM_M_CAMPAIGN*  

### Pre-requirements for ARG

**Tasks executed**:  

1. Generation and ingestion of files needed by the OB. In this case:  
  
  * *DIM_M_CAMPAIGN*
  * *DIM_M_MOVEMENT*  

### Pre-requirements for CHL

**Tasks executed**:  

1. Generation and ingestion of files needed by the OB. In this case:  
  
  * *DIM_M_BILLING_CYCLE*
  * *DIM_M_CAMPAIGN*
  * *DIM_M_MOVEMENT*
  * *DIM_M_OPERATORS*  
  
### Pre-requirements for PER

**Tasks executed**:  

1. Generation and ingestion of files needed by the OB. In this case:  
  
  * *DIM_M_BILLING_CYCLE*
  * *DIM_M_MOVEMENT*  
