def create_dataframe(spark, filepath2):
#filepath2 = "file:///home/takeo/awsproject/states_abv.csv"
    df2 = spark.read.csv(filepath2, header=True, inferSchema=True)
    table_name = "states_abv"
    df2.write.mode("overwrite").saveAsTable(table_name)