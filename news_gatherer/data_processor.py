'''
Contians functions relating to the cleaning of pulled data.
'''
import pandas as pd

class ProcessTables:
    def process_news_tables(self, result_df):
        # Extract 'category' from list
        result_df['category'] = result_df['category'].astype(str)
        result_df['category'] = [x.strip('[]').strip("'") for x in result_df['category']]


        # Extract 'country' from list
        result_df['country'] = result_df['country'].astype(str)
        result_df['country'] = [x.strip('[]').strip("'") for x in result_df['country']]

        # Create 'cleaned' link column
        result_df['link_clean'] = result_df['link'].str.rsplit('?', n=1).str.get(0)

        # Extract keywords - convert to list and explode
        keywords_col = result_df[['article_id', 'keywords']]
        # keywords_col['keywords'] = keywords_col['keywords'].astype(str).str.replace(" / ", ",").str.replace("'", "")
        # keywords_col['keywords1'] = [x.strip('[]').strip("'").split(',') for x in keywords_col['keywords']]
        keywords_col = keywords_col.explode('keywords').reset_index(drop=True)[['article_id', 'keywords']]
        # Make keywords lowercase, remove ""’" from keywords
        keywords_col['keywords'] = keywords_col['keywords'].str.lower().str.replace("’", "")

        # Extract category - convert to list and explode
        category_col = result_df[['article_id', 'category']]
        category_col['category'] = category_col['category'].astype(str).str.replace(" / ", ",").str.replace("'", "")

        category_col['category'] = [x.split(',') for x in category_col['category']]

        category_col = category_col.explode('category').reset_index(drop=True)[['article_id', 'category']]

        # Make keywords lowercase
        category_col['category'] = category_col['category'].str.lower().str.replace(" ", "").str.replace(",", "")


        # Extract creators - convert to list and explode
        creator_col = result_df[['article_id', 'creator']]
        creator_col['creator'] = creator_col['creator'].astype(str).str.replace(" and ", "','").str.replace("', ' ", ",")

        creator_col['creator'] = [x.strip('[]').strip("'").split(',') for x in creator_col['creator']]

        creator_cols = creator_col.explode('creator')

        creator_cols['creator'] = creator_cols['creator'].str.replace("'", "").str.replace('"', '')
        creator_cols = creator_cols.reset_index(drop=True)[['article_id', 'creator']]

        result_df = result_df.drop(['keywords', 'creator'], axis=1)
        result_df = result_df[['article_id', 'title', 'link', 'link_clean', 'video_url', 'description',
                               'content', 'pubDate', 'image_url', 'source_id', 'country',
                               'language']]

        table_list = [result_df, category_col, keywords_col, creator_cols]

        if len(table_list[0]) != 0:
            suffixes = ['', '_categories', '_keywords', '_creators']
            # Save results to temp csv
            for table, suff in zip(table_list, suffixes):
                table.to_csv(f'temp/temp_newsdataio{suff}.csv', encoding='utf-8', index=False)

        return table_list, suffixes

