# This script contains functions for processing data from MongoDB

from bson import ObjectId
import pandas as pd

# Extract data from MongoDB
def extract_from_mongo(mongo_conn, mongo_collection_name, date_from_ts, date_to_ts, comparison_type, columns_map, field_to_filter_ts='_id'):
    print('Extracting data from Mongo collection "{}"...'.format(mongo_collection_name))
    mongo_collection = mongo_conn.get('db').get_collection(mongo_collection_name)
    
    # if '_id' field will be used for filtering data by timestamps then convert timestamps to ObjectId:
    if field_to_filter_ts == '_id':
        date_from_ts = ObjectId.from_datetime(date_from_ts)
        date_to_ts = ObjectId.from_datetime(date_to_ts)
    
    # set Filters for export
    filters = {field_to_filter_ts: {'$gte': date_from_ts, '$lt': date_to_ts}}

    # set Fields for export
    if comparison_type not in ['docs_level', 'fields_level']:
        raise SystemExit('(!) Please specify Comparison Type as the argument (docs_level or fields_level) which you want to run!')
    
    fields = {'created_at': 1}
    if comparison_type == 'fields_level':
        for field in columns_map.keys():
            fields[field] = 1

    # extract data from Mongo
    df_mongo = pd.DataFrame.from_records(mongo_collection.find(filters, fields))
    
    print('Data from Mongo extracted.')
    return df_mongo

def agg_mongo_data(df_mongo, comparison_type):
    # get date (day) from created_at field
    df_mongo['day'] = df_mongo['created_at'].dt.date
    df_mongo.drop(columns='created_at', inplace=True)

    if comparison_type == 'docs_level':
        # group data by days and count numbers of documents
        df_mongo_agg = df_mongo.groupby('day').count()
        df_mongo_agg.columns = ['mongo_cnt']
    else:
        # group data by days and sum values in fields
        df_mongo_agg = df_mongo.drop(columns='_id').groupby('day').sum().reset_index()
        
    print('Data from Mongo aggregated.')
    return df_mongo_agg
