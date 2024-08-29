def create_dim_date_redshift_table(spark, output_table):
    df16 = spark.sql("SELECT * FROM bootcamp.us_county")
    df16.createOrReplaceTempView("selected_date")
    dim_date = spark.sql("""
        SELECT fips, date_format(date, 'dd MMM yyyy') AS date_formatted,
        CASE WHEN dayofweek(date) IN (6, 7) THEN "True" ELSE "False" END AS is_weekend
        FROM selected_date
    """)
    dim_date.createOrReplaceTempView("dim_date_table")

    dim_date.write.format("jdbc").option("url", "jdbc:redshift://default-workgroup.730335321373.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "dev.test." + output_table).option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Admin1234").mode("overwrite").save()