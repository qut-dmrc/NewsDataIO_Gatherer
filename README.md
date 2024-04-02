# NewsdataIO News Gatherer

Uses the NewsData API to request news content and push results to a Google BigQuery database.

For more details, go to: https://newsdata.io/documentation


## How to use this tool
1. Enter your config details in `config.yml`. An example of a valid configuration is shown below:
```
# SearchParams:
domains: ['7news', 'skynewsau', 'sbs', ..., 'smh', 'thewest', 'theage', 'couriermail']

query: '*'
date_from: '2024-03-01'
date_to: '2024-03-03'

country: 'au'       # Examples: au=Australia, de=Germany
language: 'en'      # Examples: en=English/de=german

# Google BigQuery Params
project_name: 'your-gbqproject'
dataset_name: 'newsdataio_data'
tablename: 'newsdata_news_au'
```

2. Run `run_newsio_gather.py`

This will call `collector.py` to gather news articles from the NewsDataIO API and push the results to Google BigQuery.

## Data output





### TODO

