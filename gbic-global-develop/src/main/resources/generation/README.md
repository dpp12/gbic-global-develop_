# Interface generation scripts

These scripts are intended to generate in Global Platform all interfaces that are not being sent from the OBs.  

## File generator

**Name**: `genfile.sh`

**Description**: Creates and, optionally, puts in HDFS, an interface (for a specific OB and period).  
This script will mainly generate descriptor table files based on templates.  
It has to be installed on HDFS client.  
The path for the installation should be `$GBIC_HOME/generation`  
* With ```GBIC_HOME=/opt/gbic/services/gplatform/global``` (Alcalá)
* Or ```GBIC_HOME=/opt/gbic/services/global``` (Boecillo)  

**Usage**:  
```
Usage: genfile.sh [OPTION] MS_VERSION COUNTRY INTERFACE [MONTH] [LOCAL_PATH_PREFIX]

Generates the specified interface based on an existing template

    [-i,--ingest]                    Execute ingestion script for specified file.
                                     It won't be executed by default.

Recognized MONTH format:
    YYYYMM

Recognized OPERATOR format:
    Two character country code according to ISO 3166-1 alpha-2 standard

When MONTH argument is not provided, previous month's will be generated.  
When LOCAL_PATH_PREFIX argument is not provided, '/sftp' will be used.  
This script generates files in a temporal directory, instead of using the standard one.  
```

**Needed files**:  
For the script to work properly:
* A `templates` directory must exist in `$GBIC_HOME/generation`.  
* A template called ```tpl_OP_INTERFACE.txt``` must exist in a templates folder for each interface that has to be generated. With:  
   - `OP`: Country code (ISO 3166-1 alpha-2)  
   - `INTERFACE`: Name of the interface to generate.  
* When the same template could be used for all countries, it would be possible to use `'XX'` as fake Country code meaning "any country".  
* This template will have the headers of the file to be generated, and the content it's supposed to have.  
* When the content could change for each country or month, the content of these templates can use variables with `${variable}` syntax.
* The content of templates must always be escaped as if it was inside an `echo -e " <content> "` statement.  
* A list of available variables is provided below.  

So, we will have:
```
- $GBIC_HOME/
  |- generation/
  |  |- tpl_BR_DIM_M_BILLING_CYCLE.txt
  |  |- tpl_CL_DIM_M_BILLING_CYCLE.txt
  |  |- tpl_ES_DIM_M_BILLING_CYCLE.txt
  |  |- tpl_PE_DIM_M_BILLING_CYCLE.txt   -- AR won't have template, since they do send the interface monthly.
  |  |
  |  |- tpl_XX_DIM_M_CAMPAIGN.txt        -- When files are empty (only headers), or content is exactly the same
  |  |- tpl_XX_DIM_M_OPERATORS.txt       --     for all OBs, a single template can be defined for all countries.
  |
  |- genfile.sh
```

**Examples**:
* **Generic empty template**. `tpl_XX_DIM_M_CAMPAIGN` (the same for all countries)  
```
COUNTRY_ID\|MONTH_ID\|CAMPAIGN_ID\|CAMPAIGN_DES
```
* **Country specific template**. `tpl_PE_DIM_M_BILLING_CYCLE`  (Perú's template for `DIM_M_BILLING_CYCLE` interface)
```
COUNTRY_ID\|BILLING_CYCLE_MONTH\|BILLING_CYCLE_ID\|BILLING_CYCLE_DES\|BILING_CYCLE_START_DT\|BILING_CYCLE_END_DT\|BILING_DUE_DT\|BILLING_RV_COMPUTES
5\|${CONTENT_DATE}\|0\|MES NATURAL \(PREPAGO\)\|${CONTENT_YEAR}-${CONTENT_MONTH}-01\|${CONTENT_YEAR}-${CONTENT_MONTH}-${CONTENT_LAST_DAY}\|${NEXT_MONTH_YEAR}-${NEXT_MONTH_MONTH}-20\|0
5\|${CONTENT_DATE}\|5\|CICLO DE FACTURACION 5\|${PREVIOUS_MONTH_YEAR}-${PREVIOUS_MONTH_MONTH}-06\|${CONTENT_YEAR}-${CONTENT_MONTH}-05\|${CONTENT_YEAR}-${CONTENT_MONTH}-25\|0
5\|${CONTENT_DATE}\|15\|CICLO DE FACTURACION 15\|${PREVIOUS_MONTH_YEAR}-${PREVIOUS_MONTH_MONTH}-16\|${CONTENT_YEAR}-${CONTENT_MONTH}-15\|${NEXT_MONTH_YEAR}-${NEXT_MONTH_MONTH}-09\|0
5\|${CONTENT_DATE}\|23\|CICLO DE FACTURACION 23\|${PREVIOUS_MONTH_YEAR}-${PREVIOUS_MONTH_MONTH}-24\|${CONTENT_YEAR}-${CONTENT_MONTH}-23\|${NEXT_MONTH_YEAR}-${NEXT_MONTH_MONTH}-16\|0

```  

**Available variables for templates**:
* `OB_2M`: Country code (ISO 3166-1 alpha-2). p.e.: `ES`, `BR`, `AR`, `CL`, `PE`  
* `OB_3m`: Country code in three lowercase characters. p.e. `esp`, `bra`, `arg`, `chl`, `per`  
* `OB_3M`: Country code in three uppercase characters. p.e. `ESP`, `BRA`, `ARG`, `CHL`, `PER`  
* `OP_ID`: GBIC Operator ID. p.e. `1`, `201`, `2`, `3`, `5`  
* `CONTENT_DATE`: Month ID for the file's content. Format: `YYYYmm`. p.e.: `201601`  
* `CONTENT_YEAR`: Year of the content date. Format `YYYY`. p.e.: `2016`  
* `CONTENT_MONTH`: Month of the content date. Format `mm`. p.e.: `01`  
* `CONTENT_LAST_DAY`: Last day of month of the content date. Format `dd`. p.e.: `31`  
* `PREVIOUS_MONTH`: Month ID for the previous month of the content date. Format `YYYYmm`. p.e.: `201512`  
* `PREVIOUS_MONTH_YEAR`: Year of the previous month. Format `YYYY`. p.e.: `2015`  
* `PREVIOUS_MONTH_MONTH`: Month of the previous month. Format `mm`. p.e.: `12`  
* `NEXT_MONTH`: Month ID for the next month to the content date. Format `YYYYmm`. p.e.: `201602`  
* `NEXT_MONTH_YEAR`: Year of the next month. Format `YYYY`. p.e.: `2016`  
* `NEXT_MONTH_MONTH`: Month of the next. Format `mm`. p.e.: `02`  
