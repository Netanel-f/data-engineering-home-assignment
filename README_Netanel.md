# Data Engineering Assignment (PySpark) - Netanel

## Stack
My stack in cloudformation is consist of S3 Bucket, Glue DB, Glue Job ,4xGlue Crawlers and 4xGlue Triggers.

Basically my Glue job schedule to run everyday at midnight UTC, and my crawlers set to be triggered by Conditional trigger for the GlueJob state. Meaning Once the Glue job finishes successfully, the crawlers will start running.

## Some data assumptions I made:
- For a row with missing close price there's a row with close_price.
  Meaning there are no two consecutive rows with missing close price.
- Obj. 4: I assumed all dates exists in the dataset but might be with empty close price (as before).

## Storage
In addition to Spark being a big data engine which support tons of data :), 
I wrote result is Parquet to support future growth.
The bucket is set to be versioned, meaning we keeping copies of the non-current objects.
To reduce cost we should also set some Lifecycle Rules to permenantly delete old versions.



