# Historic pre-requirements scripts

These scripts launch the historic generation of the pre-requirements needed per OB, previously to ETL process.  
They generate those interfaces that are not received needed by each country from a start date to an end date, and then executes the ingestion of these files in background.

## Historic pre-requirements loading

**Name**: `prereqs-loading.sh` (bash script)

**Description**: It has to be installed on HDFS client node and be executed as admin user.  
It takes five **mandatory** arguments:
* Project version (number)
* List of countries to be loaded, in 2 letters format (ISO 3166-1 alpha-2)
* Start date (format yyyyMM), from which interfaces will be generated and ingested
* End date (format yyyyMM), until which interfaces will be generated and ingested
* Local inbox, where files will be generated (outbox for genfile.sh and inbox for ingest-files.sh)
  
It has to have write access to local inbox directory, p.e: ```/sftp/{OB}/GPLATFORM/MSv5```
The path for the installation should be `$GBIC_HOME/setup/prereqs`  
* With ```GBIC_HOME=/opt/gbic/services/gplatform/global```  

**Usage**: (as admin user)
```
Usage: /usr/gbic/services/gplatform/global/setup/prereqs/prereqs-loading.sh {vers-num}     \
                                                                            {country-list} \
                                                                            {start-date}   \
                                                                            {end-date}     \
                                                                            {local_path_prefix}

Launch historic pre-requirements of the ETL for the specified OBs, from a start date to an end date.

Recognized country-list format:
    List of two character country code according to ISO 3166-1 alpha-2 standard

Recognized START_DATE and END_DATE format:
    YYYYMM
```

**Needed files**:  
For the script to work properly:
* A `generation` directory must exist in `$GBIC_HOME`.  
Inside that directory there will be the generation of files process needed by the OB. For more information about this process, please see [genfile.sh](../../generation/README.md)  
* The script of `prereqs-ingestion.sh` in this directory.  
This script will be executed *in background* (one per OB) to execute the ingestion of the generated files.  


**Arguments passed the script**:  
Call to `$GBIC_HOME/setup/prereqs-loading.sh` script will take 5 mandatory arguments:  
* `{`_`vers-num`_`}` Major version of Semantic Model document. Example: ```5``` for *MSv5*
* `{`_`country-list`_`}`  List of OBs in two characters according to [ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2#Officially_assigned_code_elements) standard.
* `{`_`start-date`_`}` Start month in format ```yyyyMM``` of data to be loaded. If not specified, the script will take ```201501```.
* `{`_`end-date`_`}` End month in format ```yyyyMM``` of data to be loaded. If not specified, the script will take current month. For example, if the script is executed on March, 13th 2016, then it'll take ```201603```.
* `{`_`local_path_prefix`_`}` Tells the script the root directory to where generated files must be stored. By default, it's set to ```/sftp``` (where the ```ESP```, ```BRA```, ```ARG```,... directories are).

**Return Codes**:  
* `0`: Script successfully ended
* Other code: see return codes in [prereqs.sh](../../etl/prereqs/README.md)  
As the prereqs-ingestion.sh script is executed in background with nohup, it is not possible to get its return code. For information about the execution of this script, see the log file generated by this process (in `{local_path_prefix}/{country}/TEMP` ).

**Examples**:
```bash
# Launch the historic pre-requirements loading for Spain and Brazil, from Jan-2016 to Dec-2016.
/usr/gbic/services/gplatform/global/setup/prereqs/prereqs-loading.sh 5
                                                                     "ES BR" \
                                                                     201601 \
                                                                     201612 \
                                                                     /sftp
```

## Historic pre-requirements ingestion

**Name**: `prereqs-ingestion.sh` (bash script)

**Description**: It has to be installed on HDFS client node and be executed as admin user.  
It takes six **mandatory** arguments:
* Project version (number)
* Country to be loaded, in 2 letters format (ISO 3166-1 alpha-2)
* List of interfaces to be ingested
* Start date (format yyyyMM), from which interfaces will be generated and ingested
* End date (format yyyyMM), until which interfaces will be generated and ingested
* Local inbox, where files will be generated (outbox for genfile.sh and inbox for ingest-files.sh)  
It is launched once per OB *in background* by the prereqs-loading.sh script to ingest in hdfs the interfaces previously generated.
It has to have read access from local inbox directory, p.e: ```/sftp/{OB}/GPLATFORM/MSv5```
The path for the installation should be `$GBIC_HOME/setup/prereqs`  
* With ```GBIC_HOME=/opt/gbic/services/gplatform/global```  

**Usage**: (as admin user)
```
Usage: /usr/gbic/services/gplatform/global/setup/prereqs/prereqs-ingestion.sh {vers-num}   \
                                                                              {country}    \
                                                                              {file-list}  \
                                                                              {start-date} \
                                                                              {end-date}   \
                                                                              {local_path_prefix}

Launch the ingestion of historic previously generated files, from a start date to an end date.


Recognized COUNTRY format:
    Two character country code according to ISO 3166-1 alpha-2 standard

Recognized START_DATE and END_DATE format:
    YYYYMM
```

**Needed files**:  
For the script to work properly:
* An `ingestion` directory must exist in `$GBIC_HOME`.  
Inside that directory there will be the ingest process of files to hdfs. For more information about this process, please see [ingest-files.sh](../../ingestion/README.md)  


**Arguments passed the script**:  
Call to `$GBIC_HOME/setup/prereqs-ingestion.sh` script will take 3 mandatory arguments, and 2 more optionally:  
* `{`_`vers-num`_`}` Major version of Semantic Model document. Example: ```5``` for *MSv5*
* `{`_`country`_`}`  OB in two characters according to [ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2#Officially_assigned_code_elements) standard.
* `{`_`file-list`_`}` List of interfaces to be ingested. Example: ```"DIM_M_GROUP_SVA DIM_M_BILLING_CYCLE"``` for *ES*
* `{`_`start-date`_`}` Start month in format ```yyyyMM``` of data to be loaded. If not specified, the script will take ```201501```.
* `{`_`end-date`_`}` End month in format ```yyyyMM``` of data to be loaded. If not specified, the script will take current month. For example, if the script is executed on March, 13th 2016, then it'll take ```201603```.
* `{`_`local_path_prefix`_`}` By default, it's set to ```/sftp``` (where the ```ESP```, ```BRA```, ```ARG```,... directories are).

**Return Codes**:  
* `0`: Script successfully ended
* `1`: Fail in the execution of `ingest-files.sh` script.
* `2`: `ingest-files.sh` script return code not equals 0.
As the prereqs-ingestion.sh script executes the `ingest-files.sh` and this script does not return any code, there is no return code for the ingestion process. For information about the execution of this script, see the log file generated by this process.

**Examples**:
```bash
# Launch the historic pre-requirements ingestion for Spain, for its generated interfaces (DIM_M_BILLING_CYCLE and DIM_M_GROUP_SVA), from Jan-2016 to Dec-2016.
/usr/gbic/services/gplatform/global/setup/prereqs/prereqs-ingestion.sh 5                                     \
                                                                       ES                                    \
                                                                       "DIM_M_BILLING_CYCLE DIM_M_GROUP_SVA" \
                                                                       201601                                \
                                                                       201612                                \
                                                                       /sftp
```
