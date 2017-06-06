# Ingest scripts

These scripts ingest the files from the OBs to HDFS

* [`ingest-files.sh`](#ingest-by-ob-file-and-date-script): Ingest by OB, File and Date script
* [`ingest-quality.sh`](#ingest-quality-files-by-ob-and-date): Ingest QUALITY files by OB and Date
* [`ingest-tacs.sh`](#ingest-devices-catalog-by-month): Ingest Devices Catalog by month  
* [`ingest-TRAFFIC-files.sh`](#ingest-traffic-traffic-voice-traffic-sms-and-traffic-data-by-ob-and-date-script): Ingest TRAFFIC (TRAFFIC_VOICE, TRAFFIC_SMS and TRAFFIC_DATA) by OB and Date script
* [`load-quality.sh`](#load-quality-files-by-ob-and-date-to-mysql): Load QUALITY files by OB and Date to mysql
* [`pre-ingest`](#inbox-organization-script): Inbox Organization script

## Ingest by OB, File and Date script

**Name**: `ingest-files.sh`

**Description**: It has to be installed on HDFS client node and be executed as `admin` user.  
Takes as arguments the list of OBs and Files to be ingested and optionally the date for that files, and puts those files in HDFS.  
It has to have write access to local inbox directory, p.e: ```/sftp/{OB}/GPLATFORM/MSv5```

**Usage**: (as ```admin```)
```bash
/opt/gbic/services/gplatform/global/ingestion/ingest-files.sh {vers-num} \
                                                              {country-list} \
                                                              {file-list} \
                                                              [{month-id} [{local_path_prefix}]]
```
Takes 3 mandatory arguments, and two more optionally:  
* `{`_`vers-num`_`}` Major version of Semantic Model document. Example: ```5``` for *MSv5*
* `{`_`country-list`_`}` List of space-separated OBs in two characters according to [ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2#Officially_assigned_code_elements) standard.
* `{`_`file-list`_`}` List of space-separated interface names.
* `{`_`month-id`_`}` Month in format ```yyyyMM``` of data to be loaded. If not specified, the script will take the previous month. For example, if the script is executed on March, 13th 2015, then it'll take ```201502```.
* `{`_`local_path_prefix`_`}` Only can be specified when ```month-id``` is also present. Tells the script the root directory to find the local files. By default, it's set to ```/sftp``` (where the ```ESP```, ```BRA```, ```ARG```,... directories are).

**Examples**:
```bash
# Put CUSTOMER, M_LINES and DIM_M_TARIFF_PLAN files from ESP, BRA and ARG, of Jan-2015.
/opt/gbic/services/gplatform/global/ingestion/ingest-files.sh 5 \
                                                              ES \
                                                              "CUSTOMER M_LINES DIM_M_TARIFF_PLAN" \
                                                              201501
```

## Ingest QUALITY files by OB and Date

**Name**: `ingest-quality.sh`

**Description**: Ingest QUALITY files by OB and Date.  
It has to be installed on HDFS client node and be executed as `admin` user.  
Takes as arguments the version, the OB and Files to be ingested and optionally the date for that files, and puts those files in HDFS.  
It has to have write access to local inbox directory, p.e: ```/sftp/{OB}/GPLATFORM/MSv5```

**Usage**:  
```
Usage: /opt/gbic/services/gplatform/global/ingestion/ingest-quality.sh [OPTION] VERSION COUNTRY DATASET_LIST [[MONTH] [LOCAL_PATH_PREFIX]]

Takes files from local inboxes per OB and distributes them by name into HDFS

    [-h,--help]     display this help and exit

Recognized VERSION format:
    Version number

Recognized COUNTRY format:
    Two character country code according to ISO 3166-1 alpha-2 standard

Recognized DATASET_LIST format:
    Space separated list of uppercase datasets names

Recognized MONTH format:
    YYYYMM

When MONTH argument is not provided, previous month's will be generated.  
When LOCAL_PATH_PREFIX argument is not provided, '/sftp' will be used.  
```

**Examples**:
```bash
# Ingest CUSTOMER, M_LINES and DIM_M_TARIFF_PLAN quality files from ESP of Jan-2016.
/opt/gbic/services/gplatform/global/ingestion/ingest-quality.sh MSv5 ES "CUSTOMER M_LINES DIM_M_TARIFF_PLAN" 201601
```

## Ingest Devices Catalog by month

**Name**: `ingest-tacs.sh`

**Description**: It has to be installed on HDFS client node and be executed as `admin` user.  
Takes as arguments optionally the date for that files and the path, and puts those files in HDFS.  
It has to have write access to local inbox directory, p.e: ```/sftp/LTV/LTVtoGBIC```

**Usage**:  
```
Usage: /opt/gbic/services/gplatform/global/ingestion/ingest-tacs.sh [OPTION] [[MONTH] [LOCAL_PATH_PREFIX]]

Takes devices catalog from local inbox and puts it into HDFS

    [-h,--help]     display this help and exit

Recognized MONTH format:
    YYYYMM

When MONTH argument is not provided, previous month's will be generated.  
When LOCAL_PATH_PREFIX argument is not provided, '/sftp' will be used.  
```

**Examples**:
```bash
# Load devices catalog for Jan-2016 to HDFS.
/opt/gbic/services/gplatform/global/ingestion/ingest-tacs.sh 201601
```

## Ingest TRAFFIC (TRAFFIC_VOICE, TRAFFIC_SMS and TRAFFIC_DATA) by OB and Date script

**Name**: `ingest-TRAFFIC-files.sh`  

**Description**:  Takes files from local inbox `/sftp/{OB}/GPLATFORM/MSv{VERSION}/{INTERFACE}/`, upload to HDFS and generate merge TRAFFIC_VOICE file  

**Usage**:  
```
Usage: ingest-TRAFFIC-files.sh [OPTION] MS_VERSION COUNTRY INTERFACE [MONTH] [LOCAL_PATH_PREFIX]

Generates the specified interface based on some HQL logic

    [-i,--ingest]                    Execute ingestion script for specified file.
                                     It won't be executed by default.

Recognized MONTH format:
    YYYYMM

Recognized OPERATOR format:
    Two character country code according to ISO 3166-1 alpha-2 standard

Recognized INTERFACE format:
    Name of interface: TRAFFIC_VOICE, TRAFFIC_SMS or TRAFFIC_DATA

When MONTH argument is not provided, previous month's will be generated.  
When LOCAL_PATH_PREFIX argument is not provided, '/sftp' will be used.  
This script generates files in a raw directory, instead of using the standard one.  
```

**Needed files**:  
For the script to work properly:
* A HQL script with merging logic must exist in:
  - `$GBIC_HOME/ingestion/hive/generate-TRAFFIC_VOICE-file.hql`
  - `$GBIC_HOME/ingestion/hive/generate-TRAFFIC_SMS-file.hql`
  - `$GBIC_HOME/ingestion/hive/generate-TRAFFIC_DATA-file.hql`  

* `ingest-file` script (mentioned above) is called from this one.  

## Load QUALITY files by OB and Date to mysql

**Name**: `load-quality.sh`

**Description**: Load QUALITY files by OB and Date to mysql.  
It has to be installed on HDFS client node and be executed as `admin` user.  
It has to have read access to mysql gbic_data_quality database.  
Takes as arguments the version, the OB and Files to be ingested and optionally the date for that files, and puts those files in HDFS.  
It has to have write access to local inbox directory, p.e: ```/sftp/{OB}/GPLATFORM/MSv5```

**Usage**:  
```
Usage: /opt/gbic/services/gplatform/global/ingestion/load-quality.sh [OPTION] VERSION COUNTRY DATASET_LIST [[MONTH] [LOCAL_PATH_PREFIX]]

Load data quality files to mysql

    [-h,--help]     display this help and exit

Recognized VERSION format:
    Version number

Recognized COUNTRY format:
    Two character country code according to ISO 3166-1 alpha-2 standard

Recognized DATASET_LIST format:
    Space separated list of uppercase datasets names

Recognized MONTH format:
    YYYYMM

When MONTH argument is not provided, previous month's will be generated.  
When LOCAL_PATH_PREFIX argument is not provided, '/sftp' will be used.  
```

**Examples**:
```bash
# Load CUSTOMER, M_LINES and DIM_M_TARIFF_PLAN quality files from ESP of Jan-2016 to mysql.
/opt/gbic/services/gplatform/global/ingestion/load-quality.sh MSv5 ES "CUSTOMER M_LINES DIM_M_TARIFF_PLAN" 201601
```

## Inbox Organization script

**Name**: `pre-ingest.sh`

**Description**: Takes files from a Spool directory and distributes them by date.  
```
From /{OB}/GPLATFORM/INBOX
To   /{OB}/GPLATFORM/{VERSION}/{MONTH}
```
It has to be installed on HDFS client node and be executed as `admin` user.  
It has to have write access to local inbox directory, p.e: `/sftp/{OB}/GPLATFORM/*`

**Usage**:  
```
Usage: /opt/gbic/services/gplatform/global/ingestion/pre-ingest.sh [OPTION] VERSION LOCAL_PATH_PREFIX [ COUNTRY ]

Load data quality files to mysql

    [-h,--help]     display this help and exit

Recognized COUNTRY format:
    Two character country code according to ISO 3166-1 alpha-2 standard
```

**Examples**:
```bash
# Empty spool directory from ESP files
/opt/gbic/services/gplatform/global/ingestion/pre-ingest.sh MSv5 /sftp/ESP/GPLATFORM ES
```
