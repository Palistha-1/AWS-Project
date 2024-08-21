def create_dataframe(spark, filepath3):
#filepath3 = "file:///home/takeo/awsproject/us_daily.csv"
    df3 = spark.read.csv(filepath3, header=True, inferSchema=True)
    table_name = "us_daily"
    df3.write.mode("overwrite").saveAsTable(table_name)