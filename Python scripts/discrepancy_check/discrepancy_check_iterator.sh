# This script will run separated ETL Checks for Mongo collections.
#
# Please specify the following 4 arguments:
# 1) Mongo collection name (and make sure that there are settings for it in settings.json)
# 2) Comparison type (docs_level for simple count of Mongo documents number vs Redshift rows number
#                    or fields_level for comparison by SUM() of values in particular columns )
# 3) DATE_FROM_TS: data will be validated since this date
# 4) DATE_TO_TS: data will be validated until this date


# Set dates range for data to be validated
DATE_FROM_TS="2020-01-01"
DATE_TO_TS="$(date +"%Y-%m-%d")"

# Set Mongo collections which should be validated
COLLECTIONS_TO_VALIDATE="orders users"

# echo ${CURRENT_DATE}

for COLLECTION in $COLLECTIONS_TO_VALIDATE
do
	python -m discrepancy_check.discrepancy_check $COLLECTION docs_level 2017-01-01 2017-12-31
	python -m discrepancy_check.discrepancy_check $COLLECTION docs_level 2018-01-01 2018-12-31
	python -m discrepancy_check.discrepancy_check $COLLECTION docs_level 2019-01-01 2019-12-31
	python -m discrepancy_check.discrepancy_check $COLLECTION docs_level $DATE_FROM_TS $DATE_TO_TS
	python -m discrepancy_check.discrepancy_check $COLLECTION fields_level 2017-01-01 2017-12-31
	python -m discrepancy_check.discrepancy_check $COLLECTION fields_level 2018-01-01 2018-12-31
	python -m discrepancy_check.discrepancy_check $COLLECTION fields_level 2019-01-01 2019-12-31
    python -m discrepancy_check.discrepancy_check $COLLECTION fields_level $DATE_FROM_TS $DATE_TO_TS
done