from google.cloud import bigquery
import os
import logging
import json


logging.basicConfig(level=logging.INFO)


def create_dataset(client, dataset_id="nikhil_submission"):
    """
    This function creates a dataset for the City List Tables
    """
    logging.info("Creating BigQuery Dataset")
    # Define the dataset ID (replace 'your-project-id' and 'your_dataset_id' accordingly)
    dataset_ref = client.dataset(dataset_id)

    # Define the dataset
    dataset = bigquery.Dataset(dataset_ref)

    # Optional: Set properties for the dataset (e.g., location)
    dataset.location = "US"  # You can set the location to your preference

    # Create the dataset
    # Set exists_ok=True to not raise an error if the dataset already exists
    dataset = client.create_dataset(dataset, exists_ok=True)

    logging.info(f"Created dataset {client.project}.{dataset.dataset_id}")


def create_table(project_id, client, dataset_id="nikhil_submission", table_name="city_list_raw"):
    """
    This function creates the City List Raw Table
    """
    logging.info("Creating City List Raw Table")
    schema = [
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Population", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("CountryCode", "STRING", mode="NULLABLE"),
    ]
    # Replace 'your_table_id' with your table ID
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    logging.info(f"Successfully Created table {table_name}")


def create_view(project_id, client, dataset_id="nikhil_submission"):
    """
    This function creates a table with deduplicated data
    """
    logging.info("Creating View for Deduplicated data")
    # Define the view SQL query
    # Replace 'your_view_id' with your view ID
    view_id = f"{project_id}.{dataset_id}.solution"
    view_query = f"""
        SELECT
            name,
            CountryCode,
            Population
        FROM (
            SELECT
                name,
                CountryCode,
                Population,
                ROW_NUMBER() OVER (PARTITION BY name, CountryCode ORDER BY Population DESC) AS row_num
            FROM
                `{project_id}.{dataset_id}.city_list_raw`
        )
        WHERE
            row_num = 1
        ORDER BY name ASC
    """
    # Define the view
    view = bigquery.Table(view_id)
    view.view_query = view_query

    # Create the view
    view = client.create_table(view, exists_ok=True)
    logging.info(f"View {view_id} created successfully.")


def load_csv_data(client, filepath, project_id, dataset_id="nikhil_submission", table_name='city_list_raw'):
    """
    This function loads a csv file into a bigquery table
    """
    logging.info(f"importing the csv file {filepath}")
    # Load Avro data into BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    with open(filepath, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_id, job_config=job_config)
    load_job.result()
    logging.info("Finished importing the csv file")


def load_json_data(client, filepath, project_id, dataset_id="nikhil_submission", table_name='city_list_raw'):
    """
    This function loads a json file into a bigquery table
    Because the json file is not newline delimited, it must be loaded from memory
    """
    logging.info(f"importing the json file {filepath}")
    # Load Avro data into BigQuery
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    with open(filepath, "r") as source_file:
        data = json.load(source_file)
    errors = client.insert_rows_json(table_id, data)  # Make an API request.
    if errors == []:
        logging.info(f"New rows have been added to {table_id}.")
    else:
        logging.error(f"Encountered errors while inserting rows: {errors}")
    logging.info("Finished importing the json file")
    pass


def load_avro_data(client, filepath, project_id, dataset_id="nikhil_submission", table_name='city_list_raw'):
    """
    This function loads an avro file into a bigquery table
    """
    logging.info(f"importing the avro file {filepath}")
    # Load Avro data into BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.AVRO
    )
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    with open(filepath, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_id, job_config=job_config)
    load_job.result()
    logging.info("Finished importing the avro file")


if __name__ == "__main__":
    project_id = os.getenv('PROJECT_ID')
    client = bigquery.Client()
    create_dataset(client)
    create_table(project_id, client)
    load_avro_data(client, os.path.join('input', 'CityListB.avro'), project_id)
    load_csv_data(client, os.path.join('input', 'CityList.csv'), project_id)
    load_json_data(client, os.path.join('input', 'CityListA.json'), project_id)
    create_view(project_id, client)
