# Test project for pyspark
## Goals : 
1. Use spark sql and dataframes API for data processing
2. Create docker image and run spark cluster (1 master 2 workers) on it
3. Create own data comparison framework
4. Test created all transformations for SQL and Dataframe api using pytest-spark  

## Requirements:
* Docker ( on Linux or with WSL support to run bash scripts )
  * 6 cores, 12 GB RAM
    - SPARK_WORKER_CORES : 2 * 3
    - SPARK_WORKER_MEMORY : 2G * 3
    - SPARK_DRIVER_MEMORY : 1G * 3
    - SPARK_EXECUTOR_MEMORY : 1G * 3

## How work with project environment:
1. For first time run use L_IN_BUILD_IMAGE = "y" it will build spark cluster image : ./bash/start-docker.sh y
2. To connect to docker container execute : ./bash/start-docker.sh n
3. To run tests ./bash/start-docker.sh n
4. To run failed tests ./bash/start-docker.sh f


## Project data
Tasks Description:
* Task_Description.txt

Inputs:
* data/tables/accounts/*.parquet
* data/tables/country_abbreviation/*.parquet
* data/tables/transactions/*.parquet

Outputs:
* data/df/task.../...
* data/sql/task.../...

Expected outputs:
* test/task1/expected_output/..
* test/task../expected_output/..

##Project realisation files
* src/pyspark_sql.py - general logic for tasks implementation using sql/df together with testing framework
* src/pyspark_dataframe.py - dataframes definition
* src/sql/.. - sql files with the same logic as for dataframes 

* test/test_app.py - all tests definition
* bash/start-docker.sh - file to start project
* bash/... other files are related to the spark env config 


