# Do all imports and installs here
import pandas as pd

from pyspark.sql import SparkSession
spark = SparkSession.builder.\
config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
.getOrCreate()

from pyspark.sql.functions import first
from pyspark.sql.functions import upper, col
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType
from pyspark.sql.functions import udf, date_trunc, date_format
import datetime as dt

# Read US Cities Demo dataset file
us_spark=spark.read.csv("./data/us-cities-demographics.csv", sep=';', header=True)

# Creating 'us_race_cnt' dataset
us_race_cnt=(us_spark.select("city","state code","Race","count")
    .groupby(us_spark.City, "state code")
    .pivot("Race")
    .agg(first("Count")))
    
uscols=["Number of Veterans","Race","Count"]
# Drop columns we don't need and drop duplicate rows
us=us_spark.drop(*uscols).dropDuplicates()

# Finally saving (committing) joined US dataset
us=us.join(us_race_cnt, ["city","state code"])

# Change `state code` column name to `state_code` and other similar problems to avoid parquet complications
us=us.select('City', col('State Code').alias('State_Code'), 'State', col('Median Age').alias('Median_age'),
     col('Male Population').alias('Male_Pop'), col('Female Population').alias('Fem_Pop'), 
        col('Total Population').alias('Ttl_Pop'), 'Foreign-born', 
          col('Average Household Size').alias('Avg_Household_Size'),
             col('American Indian and Alaska Native').alias('Native_Pop'), 
                 col('Asian').alias('Asian_Pop'), 
                    col('Black or African-American').alias('Black_Pop'), 
                      col('Hispanic or Latino').alias('Latino_Pop'), 
                        col('White').alias('White_Pop'))
                        
# Drop the `state` column
us=us.drop("state")

# Now write (and overwrite) transformed `US` dataset onto parquet file
us.write.mode('overwrite').parquet("./data/us_cities_demographics.parquet")

# i94visa dim
'''/* I94VISA - Visa codes collapsed into three categories:
   1 = Business
   2 = Pleasure
   3 = Student
*/'''
i94visa_data = [[1, 'Business'], [2, 'Pleasure'], [3, 'Student']]

# Convert to spark dataframe
i94visa=spark.createDataFrame(i94visa_data)

# Create parquet file
i94visa.write.mode('overwrite').parquet('./data/i94visa.parquet')

#i94res dim
# Read i94res text file
i94res_df = pd.read_csv('./data/i94res_cit.txt',sep='=',names=['id','country'])
# Remove whitespaces and single quotes
i94res_df['country']=i94res_df['country'].str.replace("'",'').str.strip()
# Convert pandas dataframe to list (objects which had single quotes removed automatically become string again with single quotes)
i94res_data=i94res_df.values.tolist()

# Now convert list to spark dataframe
# Create a schema for the dataframe
i94res_schema = StructType([
    StructField('id', StringType(), True),
    StructField('country', StringType(), True)
])
i94res=spark.createDataFrame(i94res_data, i94res_schema)

# Create parquet file
i94res.write.mode('overwrite').parquet('./data/i94res.parquet')

'''
/* I94MODE - There are missing values as well as not reported (9) */
	1 = 'Air'
	2 = 'Sea'
	3 = 'Land'
	9 = 'Not reported' ;
'''
# Create i94mode list
i94mode_data =[[1,'Air'],[2,'Sea'],[3,'Land'],[9,'Not reported']]

# Convert to spark dataframe
i94mode=spark.createDataFrame(i94mode_data)

# Create i94mode parquet file
i94mode.write.mode("overwrite").parquet('./data/i94mode.parquet')

# Create `i94port` dimension table so we can use it to join to `i94non_immigrant_port_entry`:
# Read i94port text file
i94port_df = pd.read_csv('./data/i94port.txt',sep='=',names=['id','port'])

# Remove whitespaces and single quotes
i94port_df['id']=i94port_df['id'].str.strip().str.replace("'",'')

# Create two columns from i94port string: port_city and port_addr
# also remove whitespaces and single quotes
i94port_df['port_city'], i94port_df['port_state']=i94port_df['port'].str.strip().str.replace("'",'').str.strip().str.split(',',1).str

# Remove more whitespace from port_addr
i94port_df['port_state']=i94port_df['port_state'].str.strip()

# Drop port column and keep the two new columns: port_city and port_addr
i94port_df.drop(columns =['port'], inplace = True)

# Convert pandas dataframe to list (objects which had single quotes removed automatically become string again with single quotes)
i94port_data=i94port_df.values.tolist()

# Now convert list to spark dataframe
# Create a schema for the dataframe
i94port_schema = StructType([
    StructField('id', StringType(), True),
    StructField('port_city', StringType(), True),
    StructField('port_state', StringType(), True)
])
i94port=spark.createDataFrame(i94port_data, i94port_schema)

# Create parquet file
i94port.write.mode('overwrite').parquet('./data/i94port.parquet')

# Read i94 non-immigration dataset
i94_spark=spark.read.parquet("sas_data")

# Converted numbers to longtype and integrtype
i94_spark=i94_spark.select(col("i94res").cast(IntegerType()),col("i94port"), 
                           col("arrdate").cast(IntegerType()),
                           col("i94mode").cast(IntegerType()),col("depdate").cast(IntegerType()),
                           col("i94bir").cast(IntegerType()),col("i94visa").cast(IntegerType()), 
                           col("count").cast(IntegerType()),
                              "gender",col("admnum").cast(LongType()))
                  
# We will drop duplicate rows for i94_spark
i94_spark=i94_spark.dropDuplicates()

# Add i94port city and state columns to i94 dataframe
i94_spark=i94_spark.join(i94port, i94_spark.i94port==i94port.id, how='left')

# Drop `id` column
i94_spark=i94_spark.drop("id")

# Join US with i94_spark to get fact table `i94non_immigrant_port_entry`
# NOTE: We use left join againt city records which may cause null values because
# we may not currently have demographic stats on all U.S. ports of entry
i94non_immigrant_port_entry=i94_spark.join(us, (upper(i94_spark.port_city)==upper(us.City)) & \
                                           (upper(i94_spark.port_state)==upper(us.State_Code)), how='left')
                                           
# Drop City and State_Code
i94non_immigrant_port_entry=i94non_immigrant_port_entry.drop("City","State_Code")

# Convert SAS arrival date to datetime format
get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
i94non_immigrant_port_entry = i94non_immigrant_port_entry.withColumn("arrival_date", get_date(i94non_immigrant_port_entry.arrdate))

i94date=i94non_immigrant_port_entry.select(col('arrdate').alias('arrival_sasdate'),
                                   col('arrival_date').alias('arrival_iso_date'),
                                   date_format('arrival_date','M').alias('arrival_month'),
                                   date_format('arrival_date','E').alias('arrival_dayofweek'), 
                                   date_format('arrival_date', 'y').alias('arrival_year'), 
                                   date_format('arrival_date', 'd').alias('arrival_day'),
                                  date_format('arrival_date','w').alias('arrival_weekofyear')).dropDuplicates()

# Save to parquet file
i94non_immigrant_port_entry.drop('arrival_date').write.mode("overwrite").parquet('./data/i94non_immigrant_port_entry.parquet')

# Add seasons to i94date dimension:
i94date.createOrReplaceTempView("i94date_table")
i94date_season=spark.sql('''select arrival_sasdate,
                         arrival_iso_date,
                         arrival_month,
                         arrival_dayofweek,
                         arrival_year,
                         arrival_day,
                         arrival_weekofyear,
                         CASE WHEN arrival_month IN (12, 1, 2) THEN 'winter' 
                                WHEN arrival_month IN (3, 4, 5) THEN 'spring' 
                                WHEN arrival_month IN (6, 7, 8) THEN 'summer' 
                                ELSE 'autumn' 
                         END AS date_season from i94date_table''')

# Save i94date dimension to parquet file partitioned by year and month:
i94date_season.write.mode("overwrite").partitionBy("arrival_year", "arrival_month").parquet('./data/i94date.parquet')