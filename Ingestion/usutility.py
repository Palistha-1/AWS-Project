def create_dataframe(spark, filepath4):
#filepath4 = "file:///home/takeo/awsproject/us.csv"
    df4 = spark.read.csv(filepath4, header=True, inferSchema=True)
    table_name = "us"
    df4.write.mode("overwrite").saveAsTable(table_name)