def create_dataframe(spark, filepath5):
#filepath5 = "file:///home/takeo/awsproject/states_daily.csv"
    df5 = spark.read.csv(filepath5, header=True, inferSchema=True)
    table_name = "states_daily"
    df5.write.mode("overwrite").saveAsTable(table_name)