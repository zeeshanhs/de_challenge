# Data Engineering challenge

## Instructions:

### Choice of Database = PostgresSQL

1. Make sure dependencies are in-line as per the provided "dependencies.config" file.
2. The tables have to be created using the scripts provided in **"0 - DDLs + INCs"** folder before execution of code.
3. Pre-populated ETL config file is provided. It can be further extended. (for further comments on its format and handling, please refer to the comments within code) file: **"__ETL_column_valids.config"**. (Chunk_size for file read can be modified here)
4. Dataset is saved in the folder **"DATASET"**. Bigger version of data can be substituded with same name. (name hard-coded in code)
5. Complete code can be executed after completing all above steps. File: **"ETL_pipeline_complete.py"** to be executed.

### Results of successful execution:

As per configuration, system will load data in chunks of 1000 rows from data file and persist them to PostgresSQL database.
