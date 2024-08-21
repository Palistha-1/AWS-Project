def create_dataframe(spark, filepath6):
#filepath6 = "file:///home/takeo/awsproject/us_states.csv"
    df6 = spark.read.csv(filepath6, header=True, inferSchema=True)
    table_name = "us_states"
    df6.write.mode("overwrite").saveAsTable(table_name)