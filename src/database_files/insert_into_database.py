'''
    Module that helps insert data from spark to database
    @Author: Emmanuel Awa
'''
from helper_modules import utility_functions as uf
from batch_process import *


if __name__ == '__main__':
    categories = ['merchants', 'dining-nightlife', 'activities-events', 'product']
    for category in categories:
        uf.print_out('Cleaning {} Table.'.format(category.capitalize()))
        df_category = create_dataframe('/tmp/exstreamly_cheap_files/{}.json'.format(category))
        df_category = remove_duplicate_deals(df_category)
        unique_vals = count_unique_rows(df_category)
        uf.print_out('Number of unique {} serving deals: {}'.format(category, unique_vals))