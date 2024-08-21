
def create_dataframe(spark, filepath8):
#filepath8 = "file:///home/takeo/awsproject/usa-hospital-beds.geojson"
    df8 = spark.read.json(filepath8)
    table_name = "us_hospital_beds"
    df8.write.mode("overwrite").saveAsTable(table_name)
    df8.printSchema()