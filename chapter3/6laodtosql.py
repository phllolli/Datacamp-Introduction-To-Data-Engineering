# strategies for pandas.to_sql(): 'fail', replace', 'append'
from pandas import to_sql
# Transformation on data
recommendations = transform_find_recommendations(rating_df)
# Load into PostgreSQL Database
recommendations,to_sql("recommendations", db_engine,
                       schema="store",if_exists="replace")