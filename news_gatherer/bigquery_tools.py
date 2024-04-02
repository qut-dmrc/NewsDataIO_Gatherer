'''
This file contains the Google BigQuery table schema for the newsdataio response
'''

import os

from google.cloud import bigquery
from google.cloud.bigquery.client import Client
from google.cloud.exceptions import NotFound

from .config import BigQuery
from .set_up_logging import *


class Schema:
    table_schema = [
        bigquery.SchemaField("article_id", "STRING", mode="REQUIRED", description="The unique id of the news article."),
        bigquery.SchemaField("title", "STRING", mode="REQUIRED", description="The title of the news article."),
        bigquery.SchemaField("link", "STRING", mode="NULLABLE", description="URL to the news article."),
        bigquery.SchemaField("link_clean", "STRING", mode="NULLABLE", description="Cleaned URL to the news article."),
        bigquery.SchemaField("video_url", "STRING", mode="NULLABLE", description="URL of embedded video content (if available)."),
        bigquery.SchemaField("description", "STRING", mode="NULLABLE", description="The blurb of the news article."),
        bigquery.SchemaField("content", "STRING", mode="NULLABLE", description="The full text of the article (if available)."),
        bigquery.SchemaField("pubDate", "TIMESTAMP", mode="NULLABLE", description="The date the article was published."),
        bigquery.SchemaField("image_url", "STRING", mode="NULLABLE", description="URL of embedded image (if available)."),
        bigquery.SchemaField("source_id", "STRING", mode="NULLABLE", description="The publisher of the news article"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE", description="The country in which the news article was published."),
        bigquery.SchemaField("language", "STRING", mode="NULLABLE", description="The language in which the news article was published.")
    ]

    categories_schema = [
        bigquery.SchemaField("article_id", "STRING", mode="NULLABLE", description="The unique id of the news article."),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE", description="The category of the news article (determined by source).")
    ]

    keywords_schema = [
        bigquery.SchemaField("article_id", "STRING", mode="NULLABLE", description="The unique id of the news article."),
        bigquery.SchemaField("keywords", "STRING", mode="NULLABLE", description="Key words associated with the news article.")

    ]

    creators_schema = [
        bigquery.SchemaField("article_id", "STRING", mode="NULLABLE", description="The unique id of the news article."),
        bigquery.SchemaField("creator", "STRING", mode="NULLABLE", description="Creator name associated with the news article.")

    ]


class PushTables:

    def push_to_gbq(self, suffixes, table_list):

        # Access Google BigQuery
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BigQuery.gbq_creds

        project_name = BigQuery.project_name
        dataset_name = BigQuery.dataset_name
        tablename = BigQuery.tablename
        table_schema = [Schema.table_schema, Schema.categories_schema, Schema.keywords_schema, Schema.creators_schema]

        bq = Client(project=project_name)

        dataset = f"{project_name}.{dataset_name}"
        schema = table_schema

        # Create dataset if does not exist
        try:
            bq.get_dataset(dataset)  # Make an API request.
            logging.info(f"Dataset {dataset} already exists")
        except NotFound:
            logging.info(f"Dataset {dataset} is not found")
            bq.create_dataset(dataset)
            logging.info(f"Created new dataset: {dataset}")

        if os.path.isfile('temp/temp_newsdataio.csv') == True:

            for i in range(len(table_list)):
                table_id = bigquery.Table(f'{dataset}.{tablename}{suffixes[i]}')
                try:
                    bq.get_table(table_id)
                    logging.info('Table exists')
                except:
                    table = bq.create_table(table_id)
                    logging.info(f'Created table {table.project}.{table.dataset_id}.{table.table_id}')

                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.CSV,
                    skip_leading_rows=1,
                    schema=schema[i],
                    max_bad_records=0
                )

                job_config.allow_quoted_newlines = True

                with open(f'temp/temp_newsdataio{suffixes[i]}.csv', 'rb') as fh:
                    job = bq.load_table_from_file(fh, f'{dataset}.{tablename}{suffixes[i]}', job_config=job_config)
                    job.result()  # Waits for the job to complete.

                table = bq.get_table(table_id)
                logging.info(
                    f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table.project}.{table.dataset_id}.{table.table_id}")
