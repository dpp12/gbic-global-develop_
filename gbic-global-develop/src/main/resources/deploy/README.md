# Deployment scripts

These scripts are for the build and deployment of GBIC GPLATFORM GLOBAL service

## Deployment script

**Name**: `deployment.sh`

**Description**: Deploys Global Platform Software to the specified environment based on predefined
config files.  
All project source files should be in the machine where ansible is installed, and this script
should be executed from it's original location within the project.  

**Usage**:  
```
Usage: deployment [OPTIONS] ENVIRONMENT

Deploys Global Platform Software to the specified environment based on predefined config files

    [-c,--custom-deployment] ENABLE  If the installation is done in a custom folder or in the definitive one.
                                     'enabled' by default.
                                     Set to 'disabled' to force productive installation:
                                       --custom-deployment=disabled
                                     When custom deployment is disabled, ansible will prompt user for password.
    [-u,--ssh-user] USER             Remote user, for the deployment and execution of scripts.
                                     If not received, will look for SSH_USER variable in the environment and,
                                     if not found, will use USER environment variable
                                     (the user executing ansible locally).
    [-s,--start-date] DATE           Oozie coordinator start date.
                                     If not specified, current day will be used.
                                     If desired first month is Jan-2016 use 2016-02-01.
    [-e,--end-date] DATE             Oozie coordinator end date.
                                     If not specified, 2099-12-31 will be used.
                                     If desired last month is Jan-2016 use 2016-03-01.
    [-o,--oozie-action] ACTION       Oozie action to execute.
                                     Possible values are:
                                       - none
                                       - submit   [by default]
                                       - run
    [-n,--no-mvn]                    Flag to skip maven generation of artifacts.
                                     If not present two actions will be executed prior to deployment:
                                       - mvn clean package -DskipTests
                                       - mvn dependency:copy-dependencies -Dsilent=true -DincludeScope=runtime
                                     Use flag to disable maven actions,
                                     when generation is managed outside of (and before) this script.
    [-a,--alarm-url] URL             URL of the Slack webhook used to send alarms when deployment fails.
                                     For example, in dev environment, push alert to #deploys channel using:
                                     https://hooks.slack.com/services/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

Recognized ENVIRONMENT values:
    dev-pre ...: EPG managed Boecillo's PRE environment  (Pre-Development and Lab environment)
    dev .......: EPG managed Boecillo's PROD environment (Development environment)
    qa ........: TGT managed Alcalá's QA Environment     (Quality Assurance environment)
    pro .......: TGT managed Alcalá's PROD Environment   (Real Production environment)

Recognized DATE format:
    yyyy-MM-DD
```

**Examples**:
```
# Execute a custom deployment on dev
# with current user, no MVN compilation, slack alerts and oozie submission
~/gbic-global/src/main/resources/deploy/deployment.sh                                           \
    --start-date='2016-02-01'                                                                   \
    --end-date='2016-03-01'                                                                     \
    --no-mvn                                                                                    \
    --alarm-url='https://hooks.slack.com/services/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' \
    dev

# Deploys on pro
# with javierb user, local MVN compilation and no submission to oozie
export MAVEN_HOME=/opt/maven
export SSH_USER=javierb
cd ~/ansible/toDeploy/gbic-global/src/main/resources/deploy
./deployment.sh --custom-deployment=disabled \
                --start-date='2015-02-01'    \
                --oozie-action=none          \
                pro
```

**Return Codes**:  
* `0`: Script successfully ended
* `1`: Script execution was cancelled due to wrong usage
* `2`: Script failed generating artifact
* `3`: Script failed getting dependencies
* `4`: Script failed deploying ansible-playbook  

## Stop service script

**Name**: `common/scripts/stopservice.sh`

**Description**: Kills all RUNNING and PREP coordinators, matching some name pattern in a specific oozie server.  

**Usage**:
```
Usage: stopservice.sh NAME_FILTER OOZIE_URL

Kills all RUNNING and PREP coordinators, matching NAME_FILTER, in specified Oozie server
```

**Examples**:
```
# Kill ETL coordinators for all countries in mim499 user's custom deployment in DEV environment
./stopservice.sh mim499_gbic-gplatform-global-etl-* http://prod-epg-hdpnn-02:11000/oozie
```
