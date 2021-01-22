import pyspark
spark = pyspark.sql.SparkSession.builder.getOrCreate()
spark.read.jdbc('jdbc:postgresql://localhost:5432/pagila','customer', 
                properties = {'user':'repl','password':'password'})
