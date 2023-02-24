## This script helps to identify if the car journey has been 
## passed through London Congestion Charge Zone (CCZ)

import pyodbc
import pandas as pd
from shapely.geometry import Point, LineString
from shapely.geometry.polygon import Polygon
import getpass
import numpy as np
import json

con = pyodbc.connect(DSN="Amazon Redshift")

q = """
        SELECT j.journey_id
             , j.chauffeur_id
             , j.pickup_ts_loc
             , j.pickup_dt_loc
             , j.dropoff_ts_loc
             , j.dropoff_dt_loc
             , j.pickup_position_lat
             , j.pickup_position_lon
             , j.dropoff_position_lat
             , j.dropoff_position_lon

        FROM analytics.journeys j

        WHERE j.completed_ts_loc >= '2019-01-01'
        AND j.city = 'London'   
        AND j.status = 'done'
    """

# get data and close connection
df = pd.read_sql(q, con)
con.close()


# Drop Cancelled/Failed journeys which can have n/a for dropoffs coordinates
# n/a values cause errors for intersection checking
# we have only 0.01% of these cases for query above
df.dropna(inplace=True)

# Congestion charge zone
ccz_file_name = 'London_Congestion_Charge_Zone.geojson'
with open(ccz_file_name) as f:
    ccz = json.load(f)

# geojson file contains polygons for 3 CCZ areas, let's process them
ccz_areas = []
for i in range(0, len(ccz['features'])):
    ccz_areas.append(ccz['features'][i]['geometry']['coordinates'][0])
ccz_polygons = []

for area in ccz_areas:
    ccz_polygons.append(Polygon(area))

# convert pickup/dropoff lat/lon to shapely Point types
df['pickup_geo_point'] = df.apply(lambda row: Point([row['pickup_position_lon'], row['pickup_position_lat']]), axis=1) #lon -> x, lat -> y
df['dropoff_geo_point'] = df.apply(lambda row: Point([row['dropoff_position_lon'], row['dropoff_position_lat']]), axis=1) #lon -> x, lat -> y

df['is_pickup_ccz'] = df['is_dropoff_ccz'] = 0

# check whether pickup/dropoff is in CCZ
for poly in ccz_polygons:
    df['is_pickup_ccz'] = df.apply(lambda x: 1 if poly.contains(x['pickup_geo_point']) else x['is_pickup_ccz'], axis=1)
    df['is_dropoff_ccz'] = df.apply(lambda x: 1 if poly.contains(x['dropoff_geo_point']) else x['is_dropoff_ccz'], axis=1)

# convert pickup/dropoff Points to shapely Lines
df['pickup_dropoff_path'] = df.apply(lambda row: LineString([row['pickup_geo_point'], row['dropoff_geo_point']]), axis=1)

# check whether pickup-dropoff path intersects CCZ
df['is_pass_through_ccz'] = 0
for poly in ccz_polygons:
    df['is_pass_through_ccz'] = df.apply(lambda x: 1 if x['pickup_dropoff_path'].intersects(poly) else x['is_pass_through_ccz'], axis=1)
    
# change 1 to 0 in is_pass_through_ccz for cases when pickup/dropoff in CCZ as well
df.loc[(df['is_pickup_ccz'] == 1) | (df['is_dropoff_ccz'] == 1), 'is_pass_through_ccz'] = 0

# add is_ccz column which = 1 in case when pickup OR dropoff OR pass_through CCZ
df['is_ccz'] = 0
df.loc[(df['is_pickup_ccz'] == 1) | (df['is_dropoff_ccz'] == 1) | (df['is_pass_through_ccz'] == 1), 'is_ccz'] = 1

# export to excel
user = getpass.getuser()
export_loc1 = '/users/' + user + '/desktop/ccz_poly_intersection_data.xlsx'

print('All journeys = ', len(df))
print('Pass through CCZ journeys num = ', len(df[(df['is_pass_through_ccz'] == 1)]))

cols_for_export = ['journey_id', 'chauffeur_id', 
                   'pickup_ts_loc', 'pickup_dt_loc', 'dropoff_ts_loc', 'dropoff_dt_loc',
                   'is_pickup_ccz', 'is_dropoff_ccz', 'is_pass_through_ccz', 'is_ccz']
df[cols_for_export].to_excel(export_loc1, index=False)

print('Done.')