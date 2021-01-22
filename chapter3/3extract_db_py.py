import sqlalchemy
connection_uri = "postgresql://repl:password@localhost:5432/pagila"
db_engine  = sqlalchemy.create_engine(connection_uri)

import pandas as pd
pd.read_sql("SELECT * FROM customer", db_engine)
