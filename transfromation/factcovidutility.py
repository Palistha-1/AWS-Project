
def create_fact_covid_redshift_table(spark, output_table):

    df8 = spark.sql(
        "SELECT fips, country_region As region, confirmed, deaths, recovered, active, province_state from bootcamp.enigma_jhu")
    df9 = spark.sql("SELECT positive, negative, hospitalizedcurrently, hospitalized, states from bootcamp.us_daily")
    df10 = spark.sql("SELECT hospitalizeddischarged from bootcamp.states_daily")

    df8.createOrReplaceTempView("selected_enigma1")
    df9.createOrReplaceTempView("selected_us_daily")
    df10.createOrReplaceTempView("selected_states_daily")

    factcovid_df = spark.sql("""
        SELECT e.fips, e.region, e.confirmed, e.deaths, e.recovered, e.active, 
               e.province_state, u.positive, u.negative, u.hospitalizedcurrently, 
               u.hospitalized, u.states, st.hospitalizeddischarged
        FROM selected_enigma1 e
        JOIN selected_us_daily u ON e.province_state = u.states
        CROSS JOIN selected_states_daily st
    """)
    factcovid_df.write.format("jdbc").option("url", "jdbc:redshift://default-workgroup.730335321373.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "dev.test." + output_table).option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Admin1234").mode("overwrite").save()

