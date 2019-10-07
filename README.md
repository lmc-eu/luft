# Luft

Luft is standard operators replacement for Airflow with declarative DAGs via Yaml file. It is basically client that helps you with everyday BI tasks.

Airflow comes with batteries loaded - couple of operators that makes your BI work less painful. But after years of using it we stumbled upon to some issues with standard operators.

* Operators are closely tied to Airflow. So for example if Data Scientist wants to ad-hoc download one table from MySQL database and
save it to BigQuery, he/she have to create new DAG, operator, jdbc credentials and run whole Airflow ecosystem on localhost. Which is usually overkill.
* Standard loading operators (eg. MySqlToGoogleCloudStorageOperator) doesn't work in Kubernetes.
* Standard loading operators are slow due to Python implementation and are not usable for big loads.
* It is really hard to debug and test operators.
* Airflow doesn't include standard principles for solving DWH problems.
* Schema of data is not usually versioned in standard loading operators.
* Airflow can be replaced with some alternatives in near future. E.g. Prefect, Dagster etc.

Luft is solving most of those problems.

## Basics

Luft is ment to be running inside Docker container (but of course it can run without it). It is just a simple Python library that is wrapper of multiple services.
_For example for paralell and fast bulk loading of data from any JDBC to BigQuery it uses Embulk, for executin BigQuery command it use standard Python BQ library, etc_.

### Task

Every work is done by task which is defined in **YAML file** (example is in `examples/tasks`).
_For example loading table Test from MySQL database into S3 is one task, loading data from GA into S3 is another task, historization script in BQ is another task etc._

Mandatory parameters of every task are:

* _name_: name of task. In case of tables it is usually table name.
* _source_system_: name of source system. Usually name of database in JDBC databases. Used for better organization especially on blob storage. E.g. jobs, prace, pzr. If not specified the source_system is taken from folder hierarchy. If you look into `example/tasks/world` then `world` will be source system if you do not specify it in your yaml file.
* _source_subsystem_: name of source subsystem. Usually name of schema in JDBC databases. Used for better organization especially on blob storage. E.g. public, b2b. If not specified the source_subsystem is taken from folder hierarchy. If you look into `example/tasks/world/public` then `public` will be source subsystem if you do not specify it in your yaml file.
* _task_type_: type of task. E.g. embulk-jdbc-load, mongo-load, etc. Luft will automatically decide which task will be used based on your cli command. So you do not have to manually specify it. But it can be useful when you want to enforce certain task regardless of cli command (e.g. you want to run BigQuery even if all other tasks in folder are responsible for loading data from MySQL to S3).

### Task List

Tasks are organized into Task Lists that is an array of work to be done for certain period of time.
_E.g. you want to download tables T1, T2 and T3 from MySQL database into S3 from 2018-01-01 to 2019-05-02 (and you have where condition on some date)._

## Task Types

Luft is currently supporting following task types:

### embulk-jdbc-load

Run Embulk and load data from JDBC db into S3 or GCS. Data are stored as CSV. Some other output data formats will be added later.

#### Command

```bash
luft jdbc load
```

#### Command parameters

* `y`, `--yml-path` (mandatory): folder or single yml file inside default tasks folder (see luft.cfg).
* `-s`, `--start-date`: Start date in format YYYY-MM-DD for executing task in loop. If not specified yesterday date is used.
* `-e`, `--end-date`: End date in format YYYY-MM-DD for executing task in loop. This day is not included. If not specified today date is used.
* `-sys`, `--source-system`: override source_system parameter. See description in _Task_ section. Has to be same as name in jdbc.cfg to get right credentials for JDBC database.
* `-sub`, `--source-subsystem`: override source_subsystem parameter. See description in _Task_ section.
* `-b`, `--blacklist`: Name of tables/objects to be ignored during processing. E.g. --yml-path gis and -b TEST. It will process all objects in gis folder except object TEST.
* `-w`, `--whitelist`: Name of tables/objects to be processed. E.g. --yml-path gis and -b TEST. It will process only object TEST.

#### Requirements

* Embulk in your docker image (see Dockerfile) or on your local.
* Appropiriate Embulk plugins:
  * Output - [embulk-output-gcs](https://github.com/embulk/embulk-output-gcs) or [embulk-output-s3](https://github.com/llibra/embulk-output-s3)
  * Input - any you need of [embulk-input-jdbc](https://github.com/embulk/embulk-input-jdbc)
* Luft installed :).
* `jdbc.cfg` file with right configuration.

### jdbc.cfg

This file contains basic jdbc configuration for all of your databases. Every database has to have `[DATABASE_NAME]` header. This has to be same as *source_system*. Supported parameters are:

* *type* - type of database accourding to [embulk-input-jdbc](https://github.com/embulk/embulk-input-jdbc)
* *uri* - uri of database
* *port* - database port
* *database* - database name
* *user* - username of user who you want to log into database
* *password* - you can specify your password even it is not recommeded way how to do that because you password can be stolen. It is good for DEV but not for PROD.
* *password_env* - name of enviromental variable used for storing password. If you use this variant you can then pass password in docker run command e.g. if password_env is set to  `MY_DB_PASS` then `docker run -e MY_DB_PASS=Password123 luft jdbc load -y <path_to_yml>` should work.

#### Yaml file parameters

Inside yaml file, following parameters are supported:

* *name* - Table name.
* *source_system* - usually name of database - used for organizational purposes and blob storage path. Has to be same as name in jdbc.cfg to get right credentials for JDBC database.
* *source_subsystem* - usually name of schema - used for organizational purposes and blob storage path.
* *task_type* - `embulk-jdbc-load` by default but can be overidden. When overriden it is going to be different kind of task :).
* *thread_name* - applicable only when used with Airflow. Thread name is automatically genereted based on number of threads. If you need this task to have totally different thread you can specify custom thread name.
Eg. I have tasks T1, T2, T3, T4 and T5 in my task list. and thread count set to 3. By default (if no task has _thread_name_ specified) it will look like this in Airflow:

```text
|T1| -> |T4|
|T2| -> |T5|
|T3|
```

When I specify any _thread_name_ in task T4:

```text
|T1| -> |T5|
|T2|
|T3|
|T4|
```

* *color* - applicable only when used with Airflow. Hex color of Task in Airflow. If not specified `#A3E9DA` is used.
* *path_prefix* - Path prefix (path) on blob storage. You can use following templated fields:
  * {env} - your environment (DEV/PROD...)
  * {source_system} - name of source system (whatever you like) - in case of jdbc it is usually friendly name of db
  * {source_subsystem} - name of source subsystem (whatever you like) - in case of jdbc it is schema name
  * {name} - name of table
  * {date_valid} - date of valid of export
  * {time_valid} - time valid of export
* *embulk_template* - path to your custom embulk template. Otherwise default from `luft.cfg` will be used.
* *fetch_rows* - number of rows to fetch one time. Default 10000.
* *source_table* - in case you need different name in blob storage. E.g. Table name is Test1 but you want to rename it to Test in your DWH and on your blob storage. In this case you will write Test to your _name_ parameter in yaml file and Test1 in _source_table_ parameter.
* *where_clause* - Where condition in your SQL command. You can use `{date_valid}` parameter inside this command to print actual date valid. E.g. `where_clause: date_of_change >= '{date_valid}'`. And if you execute `luft jdbc load -y <path_to_task> -s 2019-01-01 -e 2019-05-01` for evey date between `2019-01-01` and `2019-05-01` it will print `WHERE date_of_change >= '2019-01-01'`.
* *columns* - list of columns to download. Column parameters:
  * *name* - column name.
  * *type* - column type.
  * *mandatory* - wheter column is mandatory. Default false.
  * *pk* - wheter column is primary key. Default false.
  * *escape* - escape name of column with `. Some databases reqire it.
  * *value* - fixed column value. You should never delete any of your columns from yaml file. Instead you should set `value: 'Null'`.

### bq-load

Load data from BigQuery from Google Cloud Storage and historize them. Currently only CSV is supported

#### Command

```bash
luft bq load
```

#### Command parameters

* `y`, `--yml-path` (mandatory): folder or single yml file inside default tasks folder (see luft.cfg).
* `-s`, `--start-date`: Start date in format YYYY-MM-DD for executing task in loop. If not specified yesterday date is used.
* `-e`, `--end-date`: End date in format YYYY-MM-DD for executing task in loop. This day is not included. If not specified today date is used.
* `-sys`, `--source-system`: override source_system parameter. See description in _Task_ section. Has to be same as name in jdbc.cfg to get right credentials for JDBC database.
* `-sub`, `--source-subsystem`: override source_subsystem parameter. See description in _Task_ section.
* `-b`, `--blacklist`: Name of tables/objects to be ignored during processing. E.g. --yml-path gis and -b TEST. It will process all objects in gis folder except object TEST.
* `-w`, `--whitelist`: Name of tables/objects to be processed. E.g. --yml-path gis and -b TEST. It will process only object TEST.

#### Requirements

* Luft installed :) with BigQuery - `pip install luft[bq]`.
* Credentials file (usually `service_account.json`) mapped into docker and configured in `luft.cfg`.

#### Yaml file parameters

Inside yaml file, following parameters are supported:

* *name* - Any name you want. Used mainly for name in Airflow UI.
* *source_system* - only for organizational purposes. In exec has not some special role.
* *source_subsystem* -  only for organizational purposes. In exec has not some special role.
* *task_type* - `bq-load` by default but can be overidden. When overriden it is going to be different kind of task :).
* *thread_name* - applicable only when used with Airflow. Thread name is automatically genereted based on number of threads. If you need this task to have totally different thread you can specify custom thread name.
    Eg. I have tasks T1, T2, T3, T4 and T5 in my task list. and thread count set to 3. By default (if no task has _thread_name_ specified) it will look like this in Airflow:

    ```text
    |T1| -> |T4|
    |T2| -> |T5|
    |T3|
    ```

    When I specify any _thread_name_ in task T4:

    ```text
    |T1| -> |T5|
    |T2|
    |T3|
    |T4|
    ```

* *color* - applicable only when used with Airflow. Hex color of Task in Airflow. If not specified `#03A0F3` is used.
* *project_id* = BigQuery project id. Default from `luft.cfg`.
* *location* = BigQuery location. Default from `location.cfg`.
* *columns* - list of columns to download. Column parameters:
  * *name* - column name.
  * *type* - column type.
  * *mandatory* - wheter column is mandatory. Default false.
  * *pk* - wheter column is primary key. Default false.
  * *escape* - escape name of column with `. Some databases reqire it.
  * *value* - fixed column value. You should never delete any of your columns from yaml file. Instead you should set `value: 'Null'`.
* *dataset_id* - Google BigQuery dataset name. If not specified, source_system name is used. It will be created if does not exists.
* *path_prefix* - Path prefix (path) on blob storage. You can use following templated fields:
  * {env} - your environment (DEV/PROD...)
  * {source_system} - name of source system (whatever you like) - in case of jdbc it is usually friendly name of db
  * {source_subsystem} - name of source subsystem (whatever you like) - in case of jdbc it is schema name
  * {name} - name of table
  * {date_valid} - date of valid of export
  * {time_valid} - time valid of export
* *skip_leading_rows* - whether first row of CSV should be considered header and not loaded. Default True.
* *allow_quoted_newlines* - quoted data sections that contain newline characters in a CSV file are allowed. Defaults to True.
* *field_delimiter* - how the fields are delimited. Default '\t' (tab).
* *disable_check* - by default, the check for number of loader rows into stage schema is enabled. If no data are loaded the error will appear. 
If you need to disable this check, set this flag to True. Default False.


---------

### bq-exec

Run BigQuery sql command from file.

#### Command

```bash
luft bq exec
```

#### Command parameters

* `y`, `--yml-path` (mandatory): folder or single yml file inside default tasks folder (see luft.cfg).
* `-s`, `--start-date`: Start date in format YYYY-MM-DD for executing task in loop. If not specified yesterday date is used.
* `-e`, `--end-date`: End date in format YYYY-MM-DD for executing task in loop. This day is not included. If not specified today date is used.
* `-sys`, `--source-system`: override source_system parameter. See description in _Task_ section. Has to be same as name in jdbc.cfg to get right credentials for JDBC database.
* `-sub`, `--source-subsystem`: override source_subsystem parameter. See description in _Task_ section.
* `-b`, `--blacklist`: Name of tables/objects to be ignored during processing. E.g. --yml-path gis and -b TEST. It will process all objects in gis folder except object TEST.
* `-w`, `--whitelist`: Name of tables/objects to be processed. E.g. --yml-path gis and -b TEST. It will process only object TEST.

#### Requirements

* Luft installed :) with BigQuery - `pip install luft[bq]`.
* Credentials file (usually `service_account.json`) mapped into docker and configured in `luft.cfg`.

#### Yaml file parameters

Inside yaml file, following parameters are supported:

* *name* - Any name you want. Used mainly for name in Airflow UI.
* *source_system* - only for organizational purposes. In exec has not some special role.
* *source_subsystem* -  only for organizational purposes. In exec has not some special role.
* *task_type* - `bq-load` by default but can be overidden. When overriden it is going to be different kind of task :).
* *thread_name* - applicable only when used with Airflow. Thread name is automatically genereted based on number of threads. If you need this task to have totally different thread you can specify custom thread name.
    Eg. I have tasks T1, T2, T3, T4 and T5 in my task list. and thread count set to 3. By default (if no task has _thread_name_ specified) it will look like this in Airflow:

    ```text
    |T1| -> |T4|
    |T2| -> |T5|
    |T3|
    ```

    When I specify any _thread_name_ in task T4:

    ```text
    |T1| -> |T5|
    |T2|
    |T3|
    |T4|
    ```

* *color* - applicable only when used with Airflow. Hex color of Task in Airflow. If not specified `#73DBF5` is used.
* *sql_folder* - path of folder where your SQL are located.
* *sql_files* - list of SQL files to be executed.
* *project_id* = BigQuery project id. Default from `luft.cfg`.
* *location* = BigQuery location. Default from `location.cfg`.

#### Templating in SQL

Inside of SQL you can use shortcuts for some useful variables:

* *ENV*: Environment. E.g. PROD.
* *TASK_TYPE*: Task type. `bq-exec`.
* *NAME*: Name from yaml param.
* *SOURCE_SYSTEM*: Source system.
* *SOURCE_SUBSYSTEM*: Source subsystem.
* *DATE_VALID*: Date valid of current run.
* *TIME_VALID*: Time valid.
* *TASK_ID*: Id of task.
* *THREAD_NAME*: Thread name of task.
* *YAML_FILE*: Yaml file location.
* *BQ_PROJECT_ID*: BigQuery project id.
* *BQ_LOCATION*: BigQuery location.

Example:

```yml
-- Example of templating
SELECT '{{ BQ_LOCATION }}';
SELECT '{{ BQ_PROJECT_ID }}';
SELECT '{{ DATE_VALID }}';
SELECT '{{ SOURCE_SYSTEM }}';
SELECT '{{ ENV }}';
```

### qlik-cloud-upload

Export application from Qlik Sense Enterprise, upload it to Qlik Sense cloud and publish it into certain stream.

#### Command

```bash
luft qlik-cloud upload
```

#### Command parameters

* `y`, `--yml-path` (mandatory): folder or single yml file inside default tasks folder (see luft.cfg).
* `-s`, `--start-date`: Start date in format YYYY-MM-DD for executing task in loop. If not specified yesterday date is used.
* `-e`, `--end-date`: End date in format YYYY-MM-DD for executing task in loop. This day is not included. If not specified today date is used.
* `-sys`, `--source-system`: override source_system parameter. See description in _Task_ section. Has to be same as name in jdbc.cfg to get right credentials for JDBC database.
* `-sub`, `--source-subsystem`: override source_subsystem parameter. See description in _Task_ section.
* `-b`, `--blacklist`: Name of tables/objects to be ignored during processing. E.g. --yml-path gis and -b TEST. It will process all objects in gis folder except object TEST.
* `-w`, `--whitelist`: Name of tables/objects to be processed. E.g. --yml-path gis and -b TEST. It will process only object TEST.

#### Requirements

* Luft installed :) with Qlik Sense CLoud - `pip install luft[qlik-cloud]`.
* Installed `google-chrome` and `chromedriver` in your Docker image or localhost - see [Python Selenium Installation](https://selenium-python.readthedocs.io/installation.html).
* Credentials files (`client_key.pem`, `client.pem` and `root.pem`) mapped into docker and configured in `luft.cfg` in `[qlik_enterprise]` section.
* Set all other configs in `luft.cfg` in sections `[qlik_enterprise]` and `[qlik_cloud]`.

#### Yaml file parameters

Inside yaml file, following parameters are supported:

* *name* - Any name you want. Used mainly for name in Airflow UI.
* *group_id*: Qlik cloud Group ID.
* *source_system* - only for organizational purposes. In exec has not some special role.
* *source_subsystem* -  only for organizational purposes. In exec has not some special role.
* *task_type* - `bq-load` by default but can be overidden. When overriden it is going to be different kind of task :).
* *thread_name* - applicable only when used with Airflow. Thread name is automatically genereted based on number of threads. If you need this task to have totally different thread you can specify custom thread name.
    Eg. I have tasks T1, T2, T3, T4 and T5 in my task list. and thread count set to 3. By default (if no task has _thread_name_ specified) it will look like this in Airflow:

    ```text
    |T1| -> |T4|
    |T2| -> |T5|
    |T3|
    ```

    When I specify any _thread_name_ in task T4:

    ```text
    |T1| -> |T5|
    |T2|
    |T3|
    |T4|
    ```

* *color* - applicable only when used with Airflow. Hex color of Task in Airflow. If not specified `#009845` is used.
* *apps* - list of applications for loading from QSE into certain account on Qlik Sense Cloud. Has following sublists:
  * *name*: name to show in Airflow.
  * *filename*: name of file on file on filesystem.
  * *qse_id*: Qlik Sense Enterprise application id.
  * *qsc_stream*: Qlik Sense Cloud stream name.

### qlik-metric-load

Load data from Qlik metric, convert them to json and upload to blob storage.

#### Command

```bash
luft qlik-metric load
```

#### Command parameters

* `y`, `--yml-path` (mandatory): folder or single yml file inside default tasks folder (see luft.cfg).
* `-s`, `--start-date`: Start date in format YYYY-MM-DD for executing task in loop. If not specified yesterday date is used.
* `-e`, `--end-date`: End date in format YYYY-MM-DD for executing task in loop. This day is not included. If not specified today date is used.
* `-sys`, `--source-system`: override source_system parameter. See description in _Task_ section. Has to be same as name in jdbc.cfg to get right credentials for JDBC database.
* `-sub`, `--source-subsystem`: override source_subsystem parameter. See description in _Task_ section.
* `-b`, `--blacklist`: Name of tables/objects to be ignored during processing. E.g. --yml-path gis and -b TEST. It will process all objects in gis folder except object TEST.
* `-w`, `--whitelist`: Name of tables/objects to be processed. E.g. --yml-path gis and -b TEST. It will process only object TEST.

#### Requirements

* Luft installed :) with Qlik Sense CLoud - `pip install luft[qlik-metric]`.
* Credentials files (`client_key.pem`, `client.pem` and `root.pem`) mapped into docker and configured in `luft.cfg` in `[qlik_enterprise]` section.
* Set all other configs in `luft.cfg` in sections `[qlik_enterprise]`.

#### Yaml file parameters

Inside yaml file, following parameters are supported:

* *name* - Any name you want. Used mainly for name in Airflow UI.
* *source_system* - only for organizational purposes. In exec has not some special role.
* *source_subsystem* -  only for organizational purposes. In exec has not some special role.
* *task_type* - `bq-load` by default but can be overidden. When overriden it is going to be different kind of task :).
* *thread_name* - applicable only when used with Airflow. Thread name is automatically genereted based on number of threads. If you need this task to have totally different thread you can specify custom thread name.
    Eg. I have tasks T1, T2, T3, T4 and T5 in my task list. and thread count set to 3. By default (if no task has _thread_name_ specified) it will look like this in Airflow:

    ```text
    |T1| -> |T4|
    |T2| -> |T5|
    |T3|
    ```

    When I specify any _thread_name_ in task T4:

    ```text
    |T1| -> |T5|
    |T2|
    |T3|
    |T4|
    ```

* *color* - applicable only when used with Airflow. Hex color of Task in Airflow. If not specified `#009845` is used.
* *app_id* - Qlik application id.
* *dimensions* - list. List of field names.
* *measures* - list. List of Master Measure names.
* *selections* - List of selection dictionaries to filter data.

## Running example

### 1. Creating `luft.cfg`

First you need to create config file `luft.cfg` according to example in `example/config/luft.cfg` and place it into root folder. If you want to use BigQuery and Google Cloud Storage you of course need credentials for it - [GC authentication](https://cloud.google.com/docs/authentication/getting-started). In case of AWS S3 you need to get `AWS Access Key ID` and `AWS Secret Access Key`.

Credentials (GCS, AWS, BigQuery) can be specified by three ways:

#### 1) Directly in `luft.cfg` file

| WARNING: this possibility is recommended only for local development. Because if you publish image to public repository, everybody will know your secrets  |
| --- |

#### 2) In .env file. You can create .env file

```env
EMBULK_COMMAND=embulk
LUFT_CONFIG=example/config/luft.cfg
JDBC_CONFIG=example/config/jdbc.cfg
TASKS_FOLDER=example/tasks
BLOB_STORAGE=gcs
GCS_BUCKET=
GCS_AUTH_METHOD=json_key
GCS_JSON_KEYFILE=
BQ_PROJECT_ID=
BQ_CREDENTIALS_FILE=
BQ_LOCATION=US
AWS_BUCKET=
AWS_ENDPOINT=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```

And then run your Docker command with this enviroment file:

```bash
docker run -it -rm --env-file .env luft
```

#### 3) Directly specifying env in command

This variant is prefered.

```bash
docker run -it -rm -e BLOB_STORAGE=gcs luft
```

### 2. Creating `jcbc.cfg`

For example purposes just copy `jdbc.cfg` from `example/config/` into root folder or set `JDBC_CONFIG` in your `.env` file or by `-e` parameter.

### 3. Build Docker image

Just run:

```bash
docker build -t luft .
```

### 4. Run example postgres database

```bash
docker run -d -p 5432:5432 aa8y/postgres-dataset:world
```

### 5. Run Luft to download data

Store example data from postgres database in S3 or GCS.

```bash
docker run -rm luft jdbc load -y world
```

### Run BQ exec example

Optionally if you have configured BigQuery in your `luft.cfg` you can run:

```bash
docker run -rm luft bq exec -y bq
```
