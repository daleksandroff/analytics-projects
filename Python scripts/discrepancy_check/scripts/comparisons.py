# This script contains functions for comparison

import pandas as pd

# Prepare data to be compared on fields-level
def prepare_fields_level_data(df_mongo_agg, df_redshift_agg, columns_map):
    # restructure data in dataframes
    df_mongo_agg = df_mongo_agg.melt(id_vars='day', var_name='mongo_field', value_name='mongo_sum')
    df_redshift_agg = df_redshift_agg.melt(id_vars='day', var_name='redshift_column', value_name='redshift_sum')
    
    # round numbers to 2
    df_mongo_agg['mongo_sum'] = df_mongo_agg['mongo_sum'].round(2)
    df_redshift_agg['redshift_sum'] = df_redshift_agg['redshift_sum'].round(2)
    
    # add to Mongo data new column with corresponding Redshift columns' names for further merge
    df_mongo_agg['redshift_column'] = df_mongo_agg['mongo_field'].map(columns_map)
    
    return df_mongo_agg, df_redshift_agg

# Compare Mongo data vs Redshift data
def compare_data(mongo_collection_name, rs_schema_table, df_mongo_agg, df_redshift_agg, comparison_type, columns_map):
    if comparison_type == 'fields_level':
        # restructure dataframes (columns to row values)
        df_mongo_agg, df_redshift_agg = prepare_fields_level_data(df_mongo_agg, df_redshift_agg, columns_map)
        merge_columns = ['day', 'redshift_column']
    else:
        merge_columns = ['day']
        
    # merge data from Mongo and Redshift
    df_comparison = pd.merge(df_mongo_agg, df_redshift_agg, how='left', left_on=merge_columns, right_on=merge_columns).fillna(0)
    
    # add additional info
    df_comparison['mongo_collection'] = mongo_collection_name
    df_comparison['redshift_table'] = rs_schema_table
    
    # compare Mongo vs Redshift numbers
    column_suffix = '_sum' if comparison_type == 'fields_level' else '_cnt'
     
    df_comparison['diff_abs'] = round(df_comparison['mongo' + column_suffix] - df_comparison['redshift' + column_suffix], 2)
    df_comparison['diff_pct'] = round(100 * df_comparison['diff_abs'] / df_comparison['mongo' + column_suffix], 2)

    
    print('Data from Mongo vs Redshift compared.')
    return df_comparison

