# Workflow scripts

These scripts execute workflow (end to end): ingest files to HDFS, launch coordinator to load files in hive, and copy these raw files to export directory.

## Data quality results checker

**Name**: `check-data-quality.sh`  

**Description**: Checks results of data quality model tests after executing oozie workflow.   
It has to have read access to mysql gbic_data_quality database.  
Takes as arguments the country of OBs, a dataset and optionally the date for that file.  

**Usage**: (as ```admin```):  
```
Usage: /opt/gbic/services/gplatform/global/workflow/check-data-quality.sh [OPTION] COUNTRY DATASET [MONTH]

Check the specified interface

    [--help]                         Display help and exit.
                                     It won't be executed by default.

Recognized COUNTRY format:
    Two character country code according to ISO 3166-1 alpha-2 standard

Recognized DATASET format:
    Dataset name

Recognized MONTH format:
    YYYYMM

When MONTH argument is not provided, previous month's will be generated.  
```

**Examples**:
```bash
# Check CUSTOMER file from ESP of Jan-2016.
/opt/gbic/services/gplatform/global/workflow/check-data-quality.sh ES CUSTOMER 201601
```

## Export raw files process

**Name**: `export-files.sh`  

**Description**: Copy raw files from gplatform to export directory.   
Takes as arguments the version number, the country of OB, the directory to export, and optionally the date for files to export.  
It has to have write access to local inbox directory, p.e: ```/sftp/LTV/GBICtoLTV/PE/GPLATFORM/MSv5/201606```   

**Usage**: (as ```admin```)  
```
Usage: /opt/gbic/services/gplatform/global/workflow/export-files.sh [OPTION] VERSION COUNTRY EXPORT-PATH [[MONTH] [LOCAL_PATH_PREFIX]]

Copy the specified interface

    [--help]                         Display help and exit.
                                     It won't be executed by default.

Recognized VERSION format:
    Version number

Recognized COUNTRY format:
    Two character country code according to ISO 3166-1 alpha-2 standard

Recognized EXPORT-PATH format:
    Export directory name

Recognized MONTH format:
    YYYYMM

Recognized LOCAL_PATH_PREFIX format:
    Local_path_prefix name

When MONTH argument is not provided, previous month's will be generated.  
When LOCAL_PATH_PREFIX argument is not provided, '/sftp' will be used for pro.  
```

**Examples**:
```bash
# Copy all files from ESP of Jan-2016.
/opt/gbic/services/gplatform/global/workflow/export-files.sh 5 ES /sftp/LTV/GBICtoLTV 201601
```

## Gplatform workflow execution process

**Name**: `gplatform.sh`  

**Description**: Executes the whole ingestion workflow for a given OB and month, followed by the call of the ETL oozie workflow.   
Takes as arguments the version number, the country of OB, dataset list, and optionally the date and the root directory to find the local files.  
It has to have write access to local inbox directory, p.e: ```/sftp/ESP/GPLATFORM/*```   

**Usage**: (as ```admin```)  
```
Usage: /opt/gbic/services/gplatform/global/workflow/gplatfom.sh [OPTION] VERSION COUNTRY DATASET_LIST [[MONTH] [LOCAL_PATH_PREFIX]]

Copy the specified interface

    [-Q, --ignore-dq-files]          Data quality files will be ignored.
                                     It won't be enabled by default (so ingest and load scripts for DQ will run).
                                     
    [-q, --skip-dq]                  Execute workflow skipping data quality processes.
                                     It won't be enabled by default (so DQ will run).
                                     
    [-l, --skip-local]               Execute workflow skipping local screens execution.
                                     It won't be enabled by default (so local screens will run).
                                     
    [-g, --skip-global]              Execute workflow skipping global screens execution.
                                     It won't be enabled by default (so global screens will run).
                                     
    [-p, --auto-promote]             Data will be automatically advanced to the gold zone at the end of the workflow.
                                     It won't be enabled by default (so data will remain in staging zone).
                                     
    [-f, --from] FROM_STEP           Execute workflow from this step (included).
                                     If not specified, pre-ingest will be used.
                                     
    [-t, --to] TO_STEP               Execute workflow to this step (included).
                                     If not specified, monitor-oozie-workflow will be used
                                     
    [--help]                         Display help and exit.
                                     It won't be executed by default.

Recognized FROM_STEP and TO_STEP values:
    This pipeline is a Directed Acyclic Graph wich is executed in predefined steps.
    FROM_STEP and TO_STEP should be one of the following:
    * 'pre': Pre-Ingestion of files: It empties the spool directory to correct inbox
    * 'ing': Ingestion of files and load quality files:
             It ingests files and quality files to HDFS directory and load quality files to mysql.
             If --ignore-dq-files is specified, only data files will be ingested.
             There are several ingestion scripts, executed according to OB/file specific riules:
             - ingest-files: puts data files into HDFS inbox
             - ingest-TRAFFIC-files: puts TRAFFIC data files into HDFS inbox (using HIVE)
             - ingest-quality-files: puts QUALITY files info HDFS inbox
             - load-quality-files: loads QUALITY screens into MySQL DQ data model
    * 'owf': Runs and monitorizes Oozie workflow

Recognized VERSION format:
    Version number

Recognized COUNTRY format:
    Two character country code according to ISO 3166-1 alpha-2 standard

Recognized DATASET_LIST format:
    Space separated list of uppercase datasets names

Recognized MONTH format:
    YYYYMM

Recognized LOCAL_PATH_PREFIX format:
    Name of local path prefix

When MONTH argument is not provided, previous month's will be generated.  
When LOCAL_PATH_PREFIX argument is not provided, '/sftp' will be used for pro.  
```

**Examples**:
```bash
# Copy all files from ESP of Jan-2016.
/opt/gbic/services/gplatform/global/workflow/gplatform.sh 5 ES "CUSTOMER M_LINES DIM_M_TARIFF_PLAN" 201601
```

## Oozie workflow run process

**Name**: `run-oozie-workflow.sh`  

**Description**: Creates a copy of the .properties template filling the values with the information given and run oozie workflow.
If oozie workflow has been launched successfuly and jobid_file option was given, writes in these file the id of the oozie job launched.   
It has to have write access to local properties directory, p.e: ```${GPLATFORM_HOME}/etl/oozie/config/```   
Takes as arguments the version number the country of OBs, list of datasets and optionally the date for that files.   

**Usage**: (as ```admin```)  
```
Usage: /opt/gbic/services/gplatform/global/workflow/run-oozie-workflow.sh [OPTION] VERSION COUNTRY DATASET_LIST [MONTH]

Fills a copy of the properties template with the information given as arguments and run oozie workflow

    [-q, --skip-dq]                  Execute workflow skipping data quality processes.
                                     It won't be enabled by default (so DQ will run).
                                     
    [-l, --skip-local]               Execute workflow skipping local screens execution.
                                     It won't be enabled by default (so local screens will run).
                                     
    [-g, --skip-global]              Execute workflow skipping global screens execution.
                                     It won't be enabled by default (so global screens will run).
                                     
    [-p, --auto-promote]             Data will be automatically advanced to the gold zone at the end of the workflow.
                                     It won't be enabled by default (so data will remain in staging zone).
                                     
    [-f FILE, --jobid-file=FILE]     Absolute path of the file in which job id must be written.
                                     If not present, no file will be written.
                                     
    [--help]                         Display help and exit.
                                     It won't be executed by default.

Recognized VERSION format:
    Version number

Recognized COUNTRY format:
    Two character country code according to ISO 3166-1 alpha-2 standard

Recognized DATASET_LIST format:
    Space separated list of uppercase datasets names

Recognized MONTH format:
    YYYYMM

When MONTH argument is not provided, previous month's will be generated.  
```

**Examples**:
```bash
# Execute workflow from ESP of Jan-2016.
/opt/gbic/services/gplatform/global/workflow/run-oozie-workflow.sh -f jobid.txt 5 ES "CUSTOMER M_LINES DIM_M_TARIFF_PLAN" 201601
```

## Data promotion process

**Name**: `run-promotion.sh`  

**Description**: Launch promotion process to move data from staging to gold zone.   
It has to have write access to Hive gbic_global database.   
Takes as arguments the version number, the country of OBs, a dataset and optionally the date for that file.   

**Usage**: (as ```admin```)  
```
Usage: /opt/gbic/services/gplatform/global/workflow/run-promotion.sh [OPTION] VERSION COUNTRY DATASET [MONTH]

Copy the specified interface

    [--help]                         Display help and exit.
                                     It won't be executed by default.

Recognized VERSION format:
    Version number

Recognized COUNTRY format:
    Two character country code according to ISO 3166-1 alpha-2 standard

Recognized DATASET format:
    Dataset name

Recognized MONTH format:
    YYYYMM

When MONTH argument is not provided, previous month's will be generated.  
```

**Examples**:
```bash
# Promote CUSTOMER file from ESP of Jan-2016.
/opt/gbic/services/gplatform/global/workflow/run-promotion.sh 5 ES CUSTOMER 201601
```

## Oozie coordinator execution monitor

**Name**: `monitor-oozie-workflow.sh`  

**Description**: Monitor the execution of an Oozie coordinator, waiting for it to finished and returning the status of the termination.   
It has to be executed as admin user.   
Takes as arguments the id of the Oozie job to be monitorized.   

**Usage**: (as ```admin```)  
```
Usage: /opt/gbic/services/gplatform/global/workflow/monitor-oozie-workflow.sh [OPTION] OOZIE_ID

Copy the specified interface

    [-r SECONDS, --refresh=SECONDS]  Seconds for the loop to sleep between oozie job status info executions.
                                     If not present, it will sleep 60 seconds by default.
                                     
    [-f FILE, --jobstatus-file=FILE] Absolute path of the file in which job end status must be written.
                                     If not present, no file will be written.
                                     
    [--help]                         Display help and exit.
                                     It won't be executed by default.

Recognized JOBID format:
    xxxxxxx-xxxxxxxxxxxxxxx-oozie-oozi-W  
```

**Examples**:
```bash
# Promote CUSTOMER file from ESP of Jan-2016.
/opt/gbic/services/gplatform/global/workflow/monitor-oozie-workflow.sh -f jobstatus.txt 0002178-161024092732106-oozie-oozi-C
```
