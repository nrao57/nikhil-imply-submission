# Nikhil Rao - Imply Interview Submission

## Assignment

`https://github.com/implydata/candidate-exercises-public/tree/master/Customer%20Success/SA/DataEngineeringProject/Applicant`

## Overview 

I will be solving this with a single python script using Google BigQuery

## Steps

1. Create BQ dataset
2. Create a raw table to store data from files
3. Load the CSV and Avro data directly from the file
4. Load the json data from in memory
5. Create view to deduplicate the raw data
5. Export view to CSV in Google Cloud Storage


## How to Run the Python Script
1. Install the dependencies with `pip install -r requirements.txt`
2. Set your google project with `gcloud config set project PROJECT_ID`
3. Set the environment variable `PROJECT_ID` (with bash it is `export PROJECT_ID=your-project-id`)
4. run `python solution.py`



## Questions and Answers


## Possible Improvements

1. Partition Table
2. Use Spark and Cloud Blob Store to Load


