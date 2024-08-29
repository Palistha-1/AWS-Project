
def create_region_dim_redshift_table(spark, output_table):

    df3 = spark.sql("SELECT fips, country_region AS region, latitude, longitude, province_state FROM bootcamp.enigma_jhu")
    df4 = spark.sql("SELECT state, abbreviation AS state_abb FROM bootcamp.states_abv")
    df5 = spark.sql("SELECT county, state FROM bootcamp.us_county")

    df3.createOrReplaceTempView("selected_enigma")
    df4.createOrReplaceTempView("selected_states_abv")
    df5.createOrReplaceTempView("selected_county_population")

    dimregion_df = spark.sql("""
        SELECT e.fips, e.region, e.latitude, e.longitude, e.province_state, 
               s.state_abb, c.county
        FROM selected_enigma e
        JOIN selected_states_abv s ON e.province_state = s.state
        JOIN selected_county_population c ON s.state = c.state
    """)
    dimregion_df.write.format("jdbc").option("url", "jdbc:redshift://default-workgroup.730335321373.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "dev.test." + output_table).option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Admin1234").mode("overwrite").save()

