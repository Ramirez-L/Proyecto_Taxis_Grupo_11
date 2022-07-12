from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, IntegerType

# Build SparkSession
spark = SparkSession.builder.appName('taxis').getOrCreate()

###############################################################
#######################   EXTRACT  ############################
###############################################################

weather = spark.read.option("header", "true").csv("hdfs://namenode:9000/data/Weather.csv")
weather.show()

count = weather.count()

weather.show()

print("Data points from files count: {}".format(count))
print((weather.count(), len(weather.columns)))


###############################################################
#######################   TRANSFORM  ##########################
###############################################################

### Filter any NULL symbols
df2 = weather.filter("symbol is not NULL")

df2.show()

df2_count = df2.count()
print("Data points remaining after removing nulls: {}".format(df2_count))

print("Removed {} nulls".format(weather - df2_count))

###############################################################
#########################   LOAD  #############################
###############################################################

print("DB Write complete")

print("Complete")