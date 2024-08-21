def create_dataframe(spark, filepath1):
#filepath1 = "file:///home/takeo/awsproject/County_Population.csv"
    df_countypopulation = spark.read.csv(filepath1, header=True, inferSchema=True)
    df1 = df_countypopulation.withColumnRenamed("Population Estimate 2018", "PopulationEstimate2018")
    table_name = "county_population"
    df1.write.mode("overwrite").saveAsTable(table_name)
    df1.show()
