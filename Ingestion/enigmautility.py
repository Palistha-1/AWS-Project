from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType, IntegerType, StructField


def create_dataframe(spark, filepath):
    schema = StructType([
        StructField("fips", StringType(), True),
        StructField("admin2", StringType(), True),
        StructField("province_state", StringType(), True),
        StructField("country_region", StringType(), True),
        StructField("last_update", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("confirmed", IntegerType(), True),
        StructField("deaths", IntegerType(), True),
        StructField("recovered", IntegerType(), True),
        StructField("active", IntegerType(), True),
        StructField("combined_key", StringType(), True)
    ])
    #filepath9 = "file:///home/takeo/awsproject/Enigma-JHU.csv"
    df9 = spark.read.format("csv").options(header=True, schema=schema, delimiter=",").load(filepath9)
    table_name = "enigma_jhu"
    df9.write.mode("overwrite").saveAsTable(table_name)
    df9.show()

    
