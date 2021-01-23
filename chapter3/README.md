# Chapter 3: ETL

# Extract

Extracting data from persistent storage, which is not suited for data processing, into memory.

- Persistent Storage
    - Amazon S3
    - SQL database
    - etc.

## Extract from text files

### Unstructured

- Plain text
- e.g. page

```
In his inaugural address, President Biden said this is a time for boldness, 
for there is so much to do. He’s right. And while the work is just beginning, 
it really does feel like a new day for America — not only because of 
the President’s words but because of his actions.
```

### Flat files

- Row = record
- Column = attribute
- e.g. csv

```
6/1,6/2,"['astro', 'pe', 'th', 'he', 'mathex', 'eng', 'chem60', 'social', 'math', 'phy60', 'bio60']",250,250,donet2,,
6/1,6/2,"['astro', 'pe', 'th', 'he', 'mathex', 'eng', 'chem60', 'social', 'math', 'phy60', 'bio60']",250,250,donet2,,
6/2,6/2,"['astro', 'pe', 'th', 'he', 'mathex', 'eng', 'chem60', 'social', 'math', 'phy60', 'bio60']",240,240,donet2,,
6/2,6/1,All,210,210,donet2,,
```

---

## JSON

(JavaScript Object Notation)

- Semi-structured
- Atomic
    - number
    - string
    - boolean
    - null
- Composite
    - array
    - object

Many web services use JSON to communicate data.

```json
{
    "glossary": {
        "title": "example glossary",
		"GlossDiv": {
            "title": "S",
			"GlossList": {
                "GlossEntry": {
                    "ID": "SGML",
					"SortAs": "SGML",
					"GlossTerm": "Standard Generalized Markup Language",
					"Acronym": "SGML",
					"Abbrev": "ISO 8879:1986",
					"GlossDef": {
                        "para": "A meta-markup language, used to create markup languages such as DocBook.",
						"GlossSeeAlso": ["GML", "XML"]
                    },
					"GlossSee": "markup"
                }
            }
        }
    }
}
```

## Data on the Web

### Requests

![Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/Untitled.png](Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/Untitled.png)

- **Example**
    1. Browse to Google
    2. Request to Google Server
    3. Google responds with web page

## Data on the Web through APIs

- Send data in JSON format
- API: application programming interface
- **Examples**
    - Twitter API

    [https://twitter.com/TwitterDev/status/850006245121695744](https://twitter.com/TwitterDev/status/850006245121695744)

    ```json
    {
      "created_at": "Thu Apr 06 15:24:15 +0000 2017",
      "id_str": "850006245121695744",
      "text": "1\/ Today we\u2019re sharing our vision for the future of the Twitter API platform!\nhttps:\/\/t.co\/XweGngmxlP",
      "user": {
        "id": 2244994945,
        "name": "Twitter Dev",
        "screen_name": "TwitterDev",
        "location": "Internet",
        "url": "https:\/\/dev.twitter.com\/",
        "description": "Your official source for Twitter Platform news, updates & events. Need technical help? Visit https:\/\/twittercommunity.com\/ \u2328\ufe0f #TapIntoTwitter"
      },
      "place": {   
      },
      "entities": {
        "hashtags": [      
        ],
        "urls": [
          {
            "url": "https:\/\/t.co\/XweGngmxlP",
            "unwound": {
              "url": "https:\/\/cards.twitter.com\/cards\/18ce53wgo4h\/3xo1c",
              "title": "Building the Future of the Twitter API Platform"
            }
          }
        ],
        "user_mentions": [     
        ]
      }
    }
    ```

Parse JSON → transform into a Python object

---

## Data in databases

### Applications databases

- OLTP
- Transactions
- Inserts or changes
- Row-oriented

### Analytical databases

- OLAP
- Column-oriented

## Extraction from databases

### Connection string/URI

```
postgresql//[user[:password]@][host][:port]
```

### Use in Python

```python
import sqlalchemy
connection_uri = "postgresql://repl:password@localhost:5432/pagila"
db_engine  = sqlalchemy.create_engine(connection_uri)

import pandas as pd
pd.read_sql("SELECT * FROM customer", db_engine)
```

---

# Transform

## Kind of transformations

[Example](https://www.notion.so/5c444ea9c9b84d86969a7428b6092da3)

- Selection of attribute (e.g. 'email')
- Translation of code values (e.g. 'New York' → 'NY')
- Data validation (e.g. date input in 'created_at')
- Splitting columns into multiple columns

    [Split Example](https://www.notion.so/616008b3ed3d424aa7269726430c673d)

- Joining from multiple sources

    [Customer](https://www.notion.so/d6727ad829eb49caaa3e7277f53ef3eb)

    [Rating](https://www.notion.so/ff97b1fce6a74f81977d53d0efb4d0aa)

    ```python
    ratings_per_customer = rating_df.groupBy("customer_id").mean("rating")

    customer_df.join(
    	rating_per_customer,
    	customer_df.customer_id==ratings_per_customer.customer_id
    )
    ```

If the load is small, Pandas is acceptable
else, use PySpark

### Transforming in PySpark

**Extract data into PySpark**

```python
import pyspark
spark = pyspark.sql.SparkSession.builder.getOrCreate()
spark.read.jdbc('jdbc:postgresql://localhost:5432/pagila','customer',
								properties = {'user':'repl','password':'password'})
```

---

# Load

## Analytics or Applications Databases

### Analytics

![Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/stats.png](Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/stats.png)

- Aggregate queries
- Online Analytical Processing (OLAP)

### Applications

![Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/computer.png](Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/computer.png)

- Lots of transactions
- Online Transactional Processing (OLTP)

## Column- and row-oriented

### Analytics

- Column-oriented

    ![Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/Untitled%201.png](Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/Untitled%201.png)

- Queries about subset of columns
- Parallelization

### Applications

- Row-oriented

    ![Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/Untitled%202.png](Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/Untitled%202.png)

    - Stored per record
    - Added per transaction
    - e.g. adding new customer is fast

## MPP Databases

Massively Parallel Processing Databases

![Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/Untitled%203.png](Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/Untitled%203.png)

- Amazon Redshift
- Azure SQL Data Warehouse
- Google BigQuery

### Example: Redshift

Load from file to columnar storage format

```python
# Pandas
df.to_parquet("./s3://path/to/bucket/customer.parquet")
# PySpark
df.write.parquet("./s3://path/to/bucket/customer.parquet")
```

```sql
COPY customer
FROM './s3://path/to/bucket/customer.parquet'
FORMAT as parquet
...
```

## Load to PostgreSQL

```python
# strategies for pandas.to_sql(): 'fail', replace', 'append'
from pandas import to_sql
# Transformation on data
recommendations = transform_find_recommendations(rating_df)
# Load into PostgreSQL Database
recommendations,to_sql("recommendations",db_engine,schema="store",if_exists="replace")
```

---

# Putting it all together

## The ETL function

```python
import pandas as pd

def extract_table_to_df(tablename, db_engine):
    return pd.read_sql("SELECT * FROM {}".format(tablename), db_engine)

def split_columns_transform(df, column, pat, suffixes):
    # Converts column into str and splits it on pat
    return

def load_df_into_dwh(df, tablename, schema, db_engine):
    return pd.to_sql(tablename, db_engine, schema=schema, if_exists="replace")

df_engines = { } # needs to be configured
def etl():
    #Extract
    film_df = extract_table_to_df("film", db_engines["store"])
    #Transform
    film_df = split_columns_transform(film_df, "rental_rate", ".", ["_dollar", "_cents"])
    #Load
    load_df_into_dwh(film_df, "film", "store", db_engines["dwh"])
```

## Airflow refresher

![Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/Untitled%204.png](Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/Untitled%204.png)

- Workflow scheduler
- Python
- DAGs (Directed Acyclic Graphs)

![Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/Untitled%205.png](Chapter%203%20ETL%201bb57b21e5164393b727c981d0d38c1c/Untitled%205.png)

- Tasks defined in operators (e.g. `BashOperator`)

```python
from airflow.models import DAG

dag = DAG(dag_id = 'sample',
          ...,
          schedule_interval = "0 0 * * *")
```