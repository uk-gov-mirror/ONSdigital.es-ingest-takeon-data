# Ingest Take On Data - Results Pipeline

## Wrangler

### Ingest Take On Data Wrangler

The wrangler is responsible for preparing the data (ingesting it), invoking the method lambda and writing the returned value to a file in an S3 bucket. It will notify an SNS topic at the end of the process on success of the location of the file.

Steps performed:

    - Retrieves data from Take On S3 bucket
    - Invokes method lambda
    - Writes returned value to a .json file in the Take On S3 bucket (needs to be changed to results-s3-bucket)
    - Sends SNS notification

## Method

### Ingest Take On Data Method

**Name of Lambda** ingest_takeon_data_method

**Intro** The method is responsible for for filtering the ingested data by period, and then extracting the required values from the JSON and transforming it so that it fits in the Results pipeline.

**Inputs** The method requires the database 'dump' from Take On, which is in JSON format. This is passed into the method from the wrangler via the event object.

**Outputs** A .json file that is written to an S3 bucket.