import math
import os
import logging
import time

import requests
import warnings
import pandas as pd
# from pandas.core.common import SettingWithCopyWarning


# warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

from .config import Query
from .bigquery_tools import PushTables
from .data_processor import ProcessTables
from .set_up_logging import set_up_logging


def get_api_response(url):

    # Make API call
    logging.info(f'\nAPI call:\n{url}')
    response = requests.get(url)
    logging.info(f'Response: {response.status_code}')

    return response


def get_response_data(response):
    # Get response dictionary and isolate results
    response_dict = response.json()
    data = response_dict['results']
    n_results = response_dict['totalResults']
    n_pages = math.ceil(n_results/50)

    # Check for a nextPage token for pagination
    try:
        nextPageToken = response_dict['nextPage']
    except:
        nextPageToken = None

    return data, nextPageToken, n_pages


def collect_news():

    print('\nHello!\n')

    print(f'You are about to collect news data from the following {len(Query.domains)} sources:\n')
    for domain in Query.domains:
        print(domain)
    print(f'\nYour timeframe is {Query.date_from} to {Query.date_to}.')
    print('\nIs this correct?')

    user_input = input('>>>')
    if user_input.lower() == 'y':

        logfile_filepath = f'{os.getcwd()}/logs'
        set_up_logging(logfile_filepath)
        logging.info('Starting News Gatherer')
        logging.info(f'Collecting news content from {len(Query.domains)} domains.')

        try:


            for domain in Query.domains:
                # Empty list to store pages
                pages = []
                logging.info('\n-------------------------------------------------------------------------------------------------------')
                logging.info(f'Gathering news articles from {domain} from {Query.date_from} to {Query.date_to}')

                # Make API call and get response
                url = (f'https://newsdata.io/api/1/{Query.endpoint}?'
                       f'apikey={Query.api_key}&'
                       f'q={Query.query}&'
                       f'domain={domain}&'
                       f'language={Query.lang}&'
                       f'country={Query.country}'
                       )

                if Query.endpoint == 'archive':
                    url = (f'{url}&'
                           f'from_date={Query.date_from}&'
                           f'to_date={Query.date_to}'
                           )

                # Check response code
                response = get_api_response(url=url)

                if response.status_code == 200:

                    data, nextPageToken, n_pages = get_response_data(response)

                    logging.info(f'Gathering page 1 of {n_pages}')

                    # Convert results to dataframe, append to pages list
                    df = pd.DataFrame(data)
                    if len(df) > 0:
                        pages.append(df)

                        # While nextPageToken exists, continue
                        count = 1
                        while nextPageToken is not None:
                            count = count + 1
                            # Construct URL with nextPageToken
                            url_paginate = (f'{url}&page={nextPageToken}')

                            # Make API call and get response
                            response_pag = get_api_response(url=url_paginate)
                            data, nextPageToken, n_pages = get_response_data(response_pag)

                            logging.info(f'Gathering page {count} of {n_pages}')

                            # Convert results to dataframe, append to pages list
                            df = pd.DataFrame(data)
                            pages.append(df)

                            time.sleep(1)

                        # Compile results
                        logging.info(f'\nCompiling {count} pages of results...')
                        result_df = pd.concat(pages)

                        logging.info(f'All news articles gathered from {domain} from {Query.date_from} to {Query.date_to}!')

                        # Initiate table processor, process tables
                        logging.info(f'Processing tables...')
                        processor = ProcessTables()
                        table_list, suffixes = processor.process_news_tables(result_df)
                        logging.info('Tables have been processed.')

                        # If any data have been collected, initiate pusher, push tables to GBQ
                        if len(table_list[0]) != 0:
                            logging.info(f'\nPushing results from {domain} to Google BigQuery database...')
                            pusher = PushTables()
                            pusher.push_to_gbq(suffixes, table_list)

                        else:
                            logging.info('No results to push to Google BigQuery!')

                    else:
                        logging.info(f'No results for {domain} during your specified timeframe!')

                elif response.status_code == 422:
                    logging.info(f'Domain {domain} does not exist in the NewsdataIO database. Moving to next keyword.')

                else:
                    logging.info(f'Bad response: {response.status_code}')

        except Exception as e:
            logging.info(e)

        logging.info('\nGather complete!')

    else:
        print('Exiting.')
        exit()




