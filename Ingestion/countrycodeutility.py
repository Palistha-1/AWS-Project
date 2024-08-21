def create_dataframe(spark, filepath):
#filepath = "file:///home/takeo/awsproject/CountryCodeQS.csv"
    df = spark.read.csv(filepath, header=True, inferSchema=True)
    df_countrycode = df.withColumnRenamed("Alpha-2 code", "Alpha_2_code").withColumnRenamed("Alpha-3 code", "Alpha_3_code").withColumnRenamed("Numeric code", "Numeric_code")
    table_name = "countrycode"
    df_countrycode.write.mode("overwrite").saveAsTable(table_name)
    df_countrycode.show()


