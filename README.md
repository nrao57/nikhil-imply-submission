# Nikhil Rao - Imply Interview Submission

## Assignment

`https://github.com/implydata/candidate-exercises-public/tree/master/Customer%20Success/SA/DataEngineeringProject/Applicant`

## Overview 

I will be solving this with a single python script and Google BigQuery. The script creates a dataset, table, and view and loads the data from the files. A raw table will be created first with the data directly loaded from the files. Then a view will be made with a query to deduplicate the data. All of the answers to the [questions listed in the assignments](https://github.com/implydata/candidate-exercises-public/tree/master/Customer%20Success/SA/DataEngineeringProject/Applicant) will be located at the end of this Markdown document. The final csv of the dataset will be added to this repository in the `output` directory.

## Steps

1. Create BQ dataset
2. Create a raw table to store data from files
3. Load the CSV and Avro data directly from the file
4. Load the json data from in memory
5. Create view to deduplicate the raw data
5. Export view to CSV


## How to Run the Python Script
1. Install the dependencies with `pip install -r requirements.txt`
2. Set the environment variable `PROJECT_ID` (with bash it is `export PROJECT_ID=your-project-id`)
4. run `python solution.py`

## Questions and Answers

**What is the count of all rows?**

2079

**What is the city with the largest population?**

Mumbai (Bombay)

**What is the total population of all cities in Brazil (CountryCode == BRA)?**

55,955,012

**What changes could be made to improve your program's performance.**

The script could be deployed on a compute engine instance in Google Cloud to improve the network speed and bandwidth. For the json data, the script could batch read and write the data instead of loading the entire file into memory, then writing to the database.

**How would you scale your solution to a much larger dataset (too large for a single machine to store)**

BigQuery is a datawarehouse desgined for petabytes of data. Therefore, the table and view should be able to scale easily. However, the script is currently loading data from a local machine, which is a bottleneck for storing and loading data (limited memory, storage, and network speeds). Therefore, I would store the City data in blob storage, then use an Apache Spark cluster to read the files and write to the data warehouse.
