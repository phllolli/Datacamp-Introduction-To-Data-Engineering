# Pandas
df.to_parquet("./s3://path/to/bucket/customer.parquet")
# PySpark
df.write.parquet("./s3://path/to/bucket/customer.parquet")

"""
COPY customer
FROM './s3://path/to/bucket/customer.parquet'
FORMAT as parquet
...
"""