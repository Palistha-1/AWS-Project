def create_dataframe(spark, filepath7):

#filepath7 = "file:///home/takeo/awsproject/us_county.csv"
    df7 = spark.read.csv(filepath7, header=True, inferSchema=True)
    table_name = "us_county"
    df7.write.mode("overwrite").saveAsTable(table_name)