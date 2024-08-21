from pyspark.sql import SparkSession

import usutility
import usstatesutility
import usdailyutility
import uscountyutility
import usahospitalbedutility
import statesdailyutility
import statesabvutility
import countypopulationutility
import countrycodeutility
import enigmautility

spark = SparkSession.builder \
    .appName("CSV to Hive") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE Bootcamp")

filepath = "file:///home/takeo/awsproject/CountryCodeQS.csv"
countrycodeutility.create_dataframe(spark, filepath)

filepath1 = "file:///home/takeo/awsproject/County_Population.csv"
countypopulationutility.create_dataframe(spark, filepath1)

filepath2 = "file:///home/takeo/awsproject/states_abv.csv"
statesabvutility.create_dataframe(spark, filepath2)

filepath3 = "file:///home/takeo/awsproject/us_daily.csv"
usdailyutility.create_dataframe(spark, filepath3)

filepath4 = "file:///home/takeo/awsproject/us.csv"
usutility.create_dataframe(spark, filepath4)

filepath6 = "file:///home/takeo/awsproject/us_states.csv"
usstatesutility.create_dataframe(spark, filepath6)

filepath7 = "file:///home/takeo/awsproject/us_county.csv"
uscountyutility.create_dataframe(spark, filepath7)

filepath5 = "file:///home/takeo/awsproject/states_daily.csv"
statesdailyutility.create_dataframe(spark, filepath5)

filepath8 = "file:///home/takeo/awsproject/usa-hospital-beds.geojson"
usahospitalbedutility.create_dataframe(spark, filepath8)

filepath9 = "file:///home/takeo/awsproject/Enigma-JHU.csv"
enigmautility.create_dataframe(spark, filepath9)

dimregiontramsformationutility.dim_region.show()
