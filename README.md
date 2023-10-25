
# Sparkify

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3 bucket, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project is using Python and SQL to build an ETL pipeline that extracts data from S3 bucket, stages them in Redshift, and transforms data into a set of dimensional tables




## Installation

```bash
  pip3 install configparser
```
```bash
  pip3 install psycopg2
```
```bash
  pip3 install tabulate 
```

    
## How to reproduce


1. Create IAM Role making sure to atttach `S3GetObject` and `AmazonS3ReadOnlyAccess` policies 

2. Create VPC security group allowing inbound and outbound connectivity to anywhere in the world - Source/Destination `0.0.0.0/0`

3. Create Redshift Cluster and configure database name, user, password and port

4. Fill `dwh.cfg` in using Gernal information of the Cluster created. Noted that `HOST` is `Endpoint` without :port/DB_name.

5. Run **`create_table.py`** to create/recreate fact and dimension tables for the star schema

```bash
python3 create_tables.py
```

6. Run **`etl.py`** to load data from JSON located in S3 bucket to staging tables and then to the final tables for analysis 

```bash
$ python3 etl.py
```

7. Run **`analysis.py`** for dalata validations and sample questions that users might have
```bash
$ python3 analysis.py
```

## Link to program

[Data Engineering with AWS Nanodegree Program](https://www.udacity.com/course/data-engineer-nanodegree--nd027)


