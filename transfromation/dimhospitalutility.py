def create_dim_hospital_redshift_table(spark, output_table):
    df15 = spark.sql("SELECT * FROM bootcamp.us_hospital_beds")
    df15.createOrReplaceTempView("selected_us_hospitals")

    dim_hospital = spark.sql("SELECT fips, latitude As hos_lat, longtitude As hos_lang, hq_address, hospital_type, "
                             "hospital_name, hq_city, hq_state, state_name As state from selected_us_hospital")
    dim_hospital.write.format("jdbc").option("url", "jdbc:redshift://default-workgroup.730335321373.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "dev.test." + output_table).option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Admin1234").mode("overwrite").save()