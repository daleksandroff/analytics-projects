# This script is developed to run ETL Checks: compare documents number in Mongo's collection vs. rows number in Redshift table. The resul of comparison will be saved into Redshift table. So finally we will be able to have a BI dashboard with data discrepancies.

# To run it, use the following command: python discrepancy_check.py {mongo_collection_name} {comparison_type} {DATE_FROM_TS} {DATE_TO_TS}
# comparison_type possible values: 
#    - docs_level: simple count of Mongo documents number vs Redshift rows number
#    - fields_level: comparison by SUM() of values in particular columns
# e.g.: python discrepancy_check.py orders docs_level "2019-01-01" "2019-08-01"
# Mongo collection name should be also defined in settings.json with corresponding settings.

from datetime import datetime, time
import pytz
from bson import ObjectId
import pandas as pd
import json
import sys

from discrepancy_check.scripts import connect_to_databases as db_conn
from discrepancy_check.scripts.mongo_processing import extract_from_mongo, agg_mongo_data
from discrepancy_check.scripts.redshift_processing import extract_from_redshift, upload_comparison_to_redshift
from discrepancy_check.scripts.comparisons import compare_data

# Setup connections to MongoDB and Redshift
def connect_to_db():
    connections = db_conn.DbConnections()
    connections.connect_to_mongo()
    connections.connect_to_redshift(echo=False)
    return connections

# Get settings for comparisons (Mongo collection name, Redshift table, etc..)
def get_settings(filename):
    # load settings from JSON file with MongoDB collections names, Redshift tables...
    with open(filename) as f:
        settings = json.load(f)
    print('Settings for comparison loaded.')
    return settings

#### MAIN ####

# get current time (as a time when script started to run) to load later into Redshift comparison's result
script_start_ts = datetime.now(tz=pytz.utc).replace(tzinfo=None)

# set-up databases connections
connections = connect_to_db()

# get arguments from command line:
try:
    comparison_name = sys.argv[1]
    comparison_type = sys.argv[2]
    date_from_ts = datetime.strptime(sys.argv[3], "%Y-%m-%d")
    date_to_ts = datetime.strptime(sys.argv[4], "%Y-%m-%d")
except:
    raise SystemExit('(!) Please specify Mongo collection which you want to compare as an argument!')

print('Running ETL Check for "{comparison_name}" with option "{comparison_type}". Dates: since {date_from_ts} to {date_to_ts}'.format(comparison_name=comparison_name
                       ,comparison_type=comparison_type
                       ,date_from_ts=str(date_from_ts.date())
                       ,date_to_ts=str(date_to_ts.date())))

# get comparison settings for selected Mongo collection
filename = 'discrepancy_check/settings.json'
settings = get_settings(filename)[comparison_name]

# get corresponding comparison settings for this Mongo collection
mongo_collection_name = settings['mongo_collection']
schema_table = settings['redshift_table']
ts_column = settings['redshift_ts_column']
columns_map_mongo_to_redshift = settings['columns_map_mongo_to_redshift']

# get data from Mongo
df_mongo = extract_from_mongo(connections.mongo, mongo_collection_name, date_from_ts, date_to_ts,  comparison_type, columns_map_mongo_to_redshift)

# aggregate data from Mongo
df_mongo_agg = agg_mongo_data(df_mongo, comparison_type)

# get aggregated data from Redshift
df_redshift_agg = extract_from_redshift(connections.redshift.get('conn'), schema_table, ts_column, date_from_ts, date_to_ts, comparison_type, columns_map_mongo_to_redshift)

# compare Mongo vs Redshift numbers
comparison_result = compare_data(mongo_collection_name, schema_table, df_mongo_agg, df_redshift_agg, comparison_type, columns_map_mongo_to_redshift)

# upload comparison results into Redshift
upload_comparison_to_redshift(connections.redshift.get('conn'), comparison_result, script_start_ts, comparison_type)

# close all DB connections
connections.close_connections()

print('Total execution duration (min.): ', round((datetime.now(tz=pytz.utc).replace(tzinfo=None) - script_start_ts).seconds / 60, 2))

print('Done!')