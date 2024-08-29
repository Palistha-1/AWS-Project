from pyspark.sql import SparkSession

import dimregionutility
import dimdateutility
import dimhospitalutility
import factcovidutility

if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder.master("local[1]").appName(
        "aws_covid19_data_analysis_project").enableHiveSupport().getOrCreate()

    dimregionutility.create_region_dim_redshift_table(spark, output_table="dim_region_table")

    dimdateutility.create_dim_date_redshift_table(spark, output_table="dim_date_table")

    dimhospitalutility.create_dim_hospital_redshift_table(spark, output_table="dim_hospital_table")

    factcovidutility.create_fact_covid_redshift_table(spark, output_table="fact_covid_table")



























# #dim region
# selected_enigma = df9."select ("flips", "country_region".alias("region"), "latitude", "longitude" from "enigma_jhu").groupBy("province_state")
#
# selected_states_abv = df2.select("state", "abbreviation".alias("state_abb"))
#
# selected_county_population = df1.select("county").groupBy("state")
#
# join_one = selected_enigma.join(selected_states_abv, selected_enigma.province_state == selected_states_abv.state)
# join_two = selected_states_abv.join(selected_county_population, selected_states_abv.state == selected_county_population.state)
#
# dim_region = join_one.join(join_two, join_one.province_state == join_two.state)
# dim_region.show()

#fact_covid
# selected_enigma1 = df9.select("flips", "country_region".alias("region"), "confirmed", "deaths", "recovered", "active").groupBy("province_state")
# 
# selected_us_daily = df4.select("positive", "negative", "hospitalizedcurrently", "hospitalized", "states").groupBy("states")
# 
# selected_states_daily = df5.select("hospitalizeddischarged")
# 
# join_three = selected_enigma1.join(selected_us_daily, selected_us_daily.states == selected_enigma1.province_state)
# join_four = selected_us_daily.join(selected_states_daily, selected_us_daily.states == selected_states_daily.state)
# 
# #dimhospaital
# dim_hospital = df8.select("fips", "latitude".alias("hos_lat"), "longitude".alias("hos_lang"), "hq_address", "hospital_type", "hospital_name", "hq_city", "hq_state", "state_name".alias("state"))
# 
# #dim_date
# df_date_dim = df_enigma_jhu_fips_date
# df_date_dim = df_date_dim.withColumn("month", month(col('date'))).withColumn("year", year(col('date')))
# df_date_dim = df_date_dim.withColumn("is_weekend",
#                                      when(dayofweek(col('date')).isin([1, 7]), "True").otherwise("False")).limit(10)
# 
# return df_date_di