import os
import yaml

wd = os.getcwd()

try:
    with open(f'{wd}/config/config.yml', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
except FileNotFoundError:
    print("\nCannot find config file!")
    exit()
except yaml.YAMLError:
    print("\nDetected an issue with your config.")


class Query:
    api_key = os.environ['newsdata_apikey']
    endpoint = config['endpoint']
    domains = config['domains']
    query = config['query']
    date_from = config['date_from']
    date_to = config['date_to']
    country = config['country']
    lang = config['language']


class BigQuery:
    gbq_creds = os.environ['gbq_servicekey']
    project_name = config['project_name']
    dataset_name = config['dataset_name']
    tablename = config['tablename']
