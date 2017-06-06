# Quality Assurance scripts

These scripts are intended to ensure the quality of the interfaces of the semantic model.

## QA Reports generator

**Name**: `generate-qa-reports` (bash script)

**Description**: Defines the workflow for a generic interface validation, and generates QA reports for one specific interface.  
It has to be installed on HDFS client.  
The path for the installation should be `$GBIC_HOME/qa`  
* With ```GBIC_HOME=/opt/gbic/services/gplatform/global``` (Alcalá)
* Or ```GBIC_HOME=/opt/gbic/services/global``` (Boecillo)  

**Usage**:  
```
Usage: generate-qa-reports [OPTIONS] MS_VERSION OPERATOR INTERFACE [MONTH]

Generates QA reports for the specified file

    [-i,--ingest]                    Execute ingestion script for specified file.
                                     It won't be executed by default.
    [-r,--reload-tables]             Reload QA temporary tables.
                                     It won't be executed by default, unless -i is present.
    [-s,--script-ids] SCRIPT-ID-LIST Space separated list of script-ids to be executed.
                                     If not specified, all scripts will be launched.

Recognized MONTH format:
    YYYYMM

Recognized OPERATOR format:
    Two character country code according to ISO 3166-1 alpha-2 standard
```

**Needed files**:  
For the script to work properly:
* A `hive` directory must exist in `$GBIC_HOME/qa`.  
Inside that directory there will be some _HQL_ (using standard nomenclature), responsible for the report generation itself.  
* Generically, there will be a file called `load_qa_XXXXXX.hql` (with `XXXXXX` being the name of the interface, using uppercase letters)
responsible for ensuring that the database, tables and partitions exist containing the data of the interface to be checked.  
* Aditionally, there will be a directory called `qa_rpt_XXXXXX` and within it, a collection of _HQL_ scripts.
* These scripts will be named `qa_rpt_XXXXXX_nn.hql` with _nn_ being a sequence two digit number, starting on `01`, and they will contain the query needed to generate a single QA report.  

So, we will have:
```
- $GBIC_HOME/
  |- common/
  |  |- gbic-gplatform-common.sh
  |
  |- qa/
  |  |- qa_rpt_CUSTOMER/
  |  |  |- qa_rpt_CUSTOMER_01.hql
  |  |  |- qa_rpt_CUSTOMER_02.hql
  |  |  |- qa_rpt_CUSTOMER_03.hql
  |  |  |- ...
  |  |- qa_rpt_DAILY_TRAFFIC/
  |  |  |- qa_rpt_DAILY_TRAFFIC_01.hql
  |  |  |- qa_rpt_DAILY_TRAFFIC_02.hql
  |  |  |- qa_rpt_DAILY_TRAFFIC_03.hql
  |  |  |- ...
  |  |- ...
  |  |- load_qa_CUSTOMER.hql
  |  |- load_qa_DAILY_TRAFFIC.hql
  |  |- load_qa_F_LINES.hql
  |  |- load_qa_INVOICE.hql
  |  |- load_qa_M_LINES.hql
  |  |- load_qa_MOVEMENTS.hql
  |  |- load_qa_SERVICES_LINE.hql
  |  |- load_qa_TRAFFIC_DATA.hql
  |  |- load_qa_TRAFFIC_SMS.hql
  |  |- load_qa_TRAFFIC_VOICE.hql
  |  |- ...
  |
  |- generate-qa-reports
```

**Arguments passed to hive scripts**:  
Call to `$GBIC_HOME/qa/hive/load_qa_XXXXXX.hql` script will be passing 4 arguments:
* `op`: with the `gbic_op_id` (used for partitioning)
* `op3m`: with the abbreviated name of the country using 3 lowercase letters (used to compose paths in HDFS for external tables)
* `month`: with the month of data, using format `YYYY-mm-01` (used for partitioning)
* `version`: number of major version of semantic model of the interface (used to compose paths)  

Call to `$GBIC_HOME/qa/hive/qa_rpt_XXXXXX/qa_rpt_XXXXXX_nn.hql` scripts will be passing only two arguments, both for accessing the right partition on report generation:
* `op`: with the `gbic_op_id`
* `month`: with the month of data, using format `YYYY-mm-01`  

**Return Codes**:  
* `0`: Script successfully ended
* `1`: Script execution was cancelled due to wrong usage
* `2`: Script failed trying to ingest specified file into HDFS before generating QA reports
* `3`: Script failed trying to prepare hive objects over inbox data before generating QA reports
* `4`: No QA script was found for the specified interface  

## Load QA data script for a specific interface:

**Name**: `load_qa_XXXXXX.hql` (with `XXXXXX` being the name of the interface, using uppercase letters)

**Description**: Prepares data of the interface `XXXXXX` and requested `op`/`month` for the QA reports to be generated
* Ensure existence of database `gbic_global_qa`
* Ensure existence of EXTERNAL table `gbic_global_qa_XXXXXX_ext` partitioned by `gbic_op_id` and `month`, stored as TEXTFILE and with field delimiter: `'|'`
* Ensure existence of table `gbic_global_qa_XXXXXX_orc` partitioned by `gbic_op_id` and `month`, stored as ORC with ZLIB compression and with field delimiter: `','`
* Ensure existence of partition `( gbic_op_id='${hivevar:op}', month='${hivevar:month}' )` on external table, from LOCATION `'/user/gplatform/inbox/${hivevar:op3m}/MSv${hivevar:version}/XXXXXX/month=${hivevar:month}'`
* Ensure IT DOESN'T EXIST partition `( gbic_op_id='${hivevar:op}', month='${hivevar:month}' )` on ORC table
* Load data from external table partition to ORC table partition using `INSERT / SELECT` statement (`...WHERE gbic_op_id=... AND month=...`)  

**Usage**:
```bash
  hive --hivevar op=${OP_ID}                                 \
       --hivevar op3m=${OB_3m}                               \
       --hivevar month="${CONTENT_YEAR}-${CONTENT_MONTH}-01" \
       --hivevar version=${MS_VERSION}                       \
        -f $GBIC_HOME/qa/hive/load_qa_XXXXXX.hql
```

## QA scripts for the generation of reports for a specific `interface`/`op`/`month`

**Name**: `qa_rpt_XXXXXX_nn.hql`  
With `XXXXXX` being the name of the interface, using uppercase letters;  
And _nn_ being a sequence two digit number, starting on `01`  

**Description**: Contain the query needed to generate a single QA report  

**Usage**:
```bash
      hive --hivevar op=${OP_ID}                                 \
           --hivevar month="${CONTENT_YEAR}-${CONTENT_MONTH}-01" \
            -f $GBIC_HOME/qa/hive/qa_rpt_XXXXXX/qa_rpt_XXXXXX_nn.hql
```
----

## QA Report Scripts by interface name: (**To Bo Completed**)  

**CUSTOMER**: (**To Do**)
* `01`: { _describe query here_ }
* `02`: { _describe query here_ }
* ...  

**DAILY_TRAFFIC**: (**To Do**)
* `01`: { _describe query here_ }
* `02`: { _describe query here_ }
* ...  

**F_LINES**: (**To Do**)
* `01`: { _describe query here_ }
* `02`: { _describe query here_ }
* ...  

**INVOICE**: (**To Do**)
* `01`: { _describe query here_ }
* `02`: { _describe query here_ }
* ...  

**M_LINES**: (**To Do**)
* `01`: { _describe query here_ }
* `02`: { _describe query here_ }
* ...  

**MOVEMENTS**: (**To Do**)
* `01`: { _describe query here_ }
* `02`: { _describe query here_ }
* ...  

**SERVICES_LINE**: (**To Do**)
* `01`: { _describe query here_ }
* `02`: { _describe query here_ }
* ...  

**TRAFFIC_DATA**: (**To Do**)
* `01`: { _describe query here_ }
* `02`: { _describe query here_ }
* ...  

**TRAFFIC_SMS**: (**To Do**)
* `01`: { _describe query here_ }
* `02`: { _describe query here_ }
* ...  

**TRAFFIC_VOICE**: (**To Do**)
* `01`: { _describe query here_ }
* `02`: { _describe query here_ }
* ...  
