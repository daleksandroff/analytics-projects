# This script contains functions for processing data from Redshift and uploading final comparison results into Redshift table

import pandas as pd

# Extract aggregated data from Redshift
def extract_from_redshift(rs_conn, schema_table, ts_column, date_from_ts, date_to_ts, comparison_type, columns_map):
    print('Extracting data from Redshift table "{}"...'.format(schema_table))
    
    # define aggregation SQL query, based on type of comparison
    if comparison_type == 'docs_level':
        # simply count rows number
        agg_query_part = ', COUNT(*) AS redshift_cnt'
    else:
        agg_query_part = ''
        # add SUM() for each column
        for col in columns_map.values():
            agg_query_part += ', SUM({column}) AS {column}'.format(column=col)
            
    # prepare SQL query to extract data, group by day and aggregate data
    sql_query = """SELECT CAST({ts_column} AS DATE) AS day
                            {agg_part}
                     FROM {schema_table}
                    WHERE {ts_column} >= '{date_from_ts}'
                      AND {ts_column} < '{date_to_ts}'
                    GROUP BY 1;""".format(ts_column=ts_column
                                         ,agg_part=agg_query_part
                                         ,schema_table=schema_table
                                         ,date_from_ts=str(date_from_ts)
                                         ,date_to_ts=str(date_to_ts))

    # run sql query and export to dataframe
    df_redshift_agg = pd.read_sql(sql_query, rs_conn)
    
    print('Data from Redshift extracted and aggregated.')
    return df_redshift_agg


# Upload comparison's results into Redshift table
def upload_comparison_to_redshift(rs_conn, df_comparison_result, script_start_ts, comparison_type):
    # add timestamp when script started to run
    df_comparison_result['run_ts_utc'] = script_start_ts
    
    # set destination Redshift schema, table, and sorted columns list
    if comparison_type == 'docs_level':
        schema_destination = 'ext'
        table_destination = 'documents_num'
        columns_list_sorted = ['run_ts_utc'
                               ,'mongo_collection'
                               ,'redshift_table'
                               ,'day'
                               ,'mongo_cnt'
                               ,'redshift_cnt'
                               ,'diff_abs'
                               ,'diff_pct']

    elif comparison_type == 'fields_level':
        schema_destination = 'ext'
        table_destination = 'columns_sum'
        columns_list_sorted = ['run_ts_utc'
                               ,'mongo_collection'
                               ,'redshift_table'
                               ,'day'
                               ,'mongo_field'
                               ,'redshift_column'
                               ,'mongo_sum'
                               ,'redshift_sum'
                               ,'diff_abs'
                               ,'diff_pct']

    # re-sort columns
    df_comparison_result = df_comparison_result.reindex(columns=columns_list_sorted)
    
    # upload dataframe to Redshift table
    df_comparison_result.to_sql(table_destination, rs_conn, schema=schema_destination, index=False, if_exists='append', method='multi')
    
    print('Comparison results uploaded to Redshift table: {schema}.{table}'.format(schema=schema_destination, table=table_destination))
    return
