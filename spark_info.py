# Some code snippets to make life a bit easier in pySpark.
# Not all areas are covered, not all table references make sense
# Use at own risk

# When being run in standard cluster config
# spark connection is already built
# uncomment and run place this on the first line
# or after imports (before spark app config)
# sc.stop()

# spark imports
from pyspark.sql.functions import broadcast
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession

from pyspark import SparkConf, SparkContext
import pyspark
import pyspark.sql

from dateutil.parser import parse
from datetime import datetime, timedelta

from pyspark.sql.functions import broadcast, col

# define spark application
# with recommended settings for clusters
spark = (
    SparkSession
    .builder
    .appName('appname')

    # use dynamic resource allocation #
    # allows for adding or removing Spark executors dynamically
    # to match the workload. Unlike the traditional static allocation
    # where a Spark application reserves CPU and memory resources upfront
    # with dynamic allocation you get as much as needed and no more
    .config("spark.dynamicAllocation.enabled", "true")

    # needed for dynamicAllocation.enabled
    .config("spark.shuffle.service.enabled", "true")

    # remove an executor if it has cached data and has been idle for more than this time
    .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "1800")

    # remove an executor if it has been idle for more than this time
    .config("spark.dynamicAllocation.executorIdleTimeout", "60")

    # Upper bound for the number of executors
    .config("spark.dynamicAllocation.maxExecutors", maxExecutors)

    # Lower bound for the number of executors
    .config("spark.dynamicAllocation.minExecutors", "0")

    # if there have been pending tasks for longer than this time, request more executors
    .config("spark.dynamicAllocation.schedulerBacklogTimeout", "30")

    # if there have been pending tasks for longer than this time, request more executors
    .config("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", "10")

    # number of partitions to use when shuffling data for joins or aggregations
    .config('spark.sql.shuffle.partitions', 1024)

    # max size in bytes for a table that will be broadcast to all workers when doing a join
    # only supported for Hive Metastore tables where the command ANALYZE TABLE has been run
    .config("spark.sql.autoBroadcastJoinThreshold", 20 * 1024 * 1024)

    # serializing objects that will be sent over the network
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    .enableHiveSupport()
    .getOrCreate()

)

# set spark context
sc = spark.sparkContext

# clear cache
sqlContext.clearCache()

# Get data from tables in Hadoop

# through a hql query

query = """ select * from schema.table """

df = spark.sql(query)

# pyspark native syntax

df = spark.table('schema.table')


# upload a table from Hadoop and manipulate it in pySpark

df = (spark
      .table('schema.table')
      .select('field', 'field')  # can be also select('*')
      .where('1=1')
      ).repartition(1000).cache()

# Create a DataFrame Data Frame from scratch

columns = ['id', 'cars', 'trains']
vals = [(1, 2, 0)]  # write each row between brackets

df = spark.createDataFrame(vals, columns)

# show rows from DataFrame

df.show()

# Another way

df.take(n)

# Aggregations
df2 = (df
       .select('field', 'field', 'field')
       .groupBy('field', 'field')
       .agg(sf.count('field').alias('count'))  # having defined before import
       .sort('field')
       ).cache().show()

# aggregation of many fields - example

df4 = (temp_viewF
       .groupBy(['field'])
       .agg(
           sf.sum('field').alias('sum'), sf.count('field').alias('count')
       )
       )

# Another way using curly brackets:

df3 = temp_view.groupBy('field').agg({'field': 'sum', 'field': 'count'})

# Aggregation: Aggregate list of fields
# Aggregate all metrics except some of them

# list_impl = list of the metrics wanted to be summed
list_impl = ['field', 'field']

Table = (Table
         .groupBy('field', 'field')
         .agg(*(sf.sum(x) for x in list_impl))
         .sort('field')
         )

# Pivot tables

# example

pivot = (temp_view
         .groupBy(['field'])
         .pivot('field')
         .count()
         )

# another example (sum)

pivot = (table
         .groupBy('field', 'field')
         .pivot('field')
         .sum('field', 'field')  # no sf.sum or f.sum ...
         .orderBy(["field", "field"], ascending=[1, 1])
         )

# Show Unique / distinct values of a column/df

df.select('k').distinct()  # (this should be enough!)
df.select('k').distinct().rdd.map(lambda r: r[0]).collect()

# Know Types of columns from a df

df.dtypes
[('age', 'int'), ('name', 'string')]

# another way
df.printSchema()

# Filter values

df = df.filter(df.field == 'CRITERIA')

# Filter on many fields (and how to set where NOT)
# Where NOT: ~

df = df.filter((~sf.isnull("field")) & (df.yyyy_mm_dd >= 'DATE'))

# Filter Null values

# (by null values - SQL):
df.filter(~sf.isnull('id')).show()

# or…

Final_Join.filter(sf.col('id').isNotNull()).show()

# (keeps just the null values):
Final_Join.filter(sf.isnull("id")).show()

# spark native

(table
 .withColumn('example', sf.when(sf.col('col1').isNull(), 'this is null')
             .otherwise('this is not null')))

# you can also use isNotNull()

df.where(col("field").isNull())

df.where(col("field").isNotNull())

# When, otherwise (equivalent for when case or if else)

df.select(
    sf.when(sf.col('field') == sf.col('field'), 'criteria')
    .when(sf.col('field') != sf.col('field'), 'criteria')
    .otherwise('field')
    .alias('field')
).show()

# Coalesce (and alias)


df = (spark.table('schema.table')
      .join(table2, 'field', 'left')
      .join(table3, 'field', 'left')
      .select(
    'field',
    f.coalesce('field', f.lit('field')).alias('field'),
    f.coalesce('field', f.lit('field Tag')).alias('field'),
    'field',
    f.coalesce('field', f.lit('No field')).alias('field'),
    f.lit(0).alias('field')
)
)


# Joins

df.join(df2, df.name == df2.name, 'left').select(df.name, df2.height)

# if the field name is the same, the condition can be input like this:

df.join(df2, 'name', 'left').select(df.name, df2.height)

# Possible joins:
# inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi,  # and left_anti.

# Broadcast join

# When joining large tables with small ones. In this example, df2 is the small one.
# It can increase a lot efficiency


table = df.join(broadcast(df2), cond, 'inner')

# Create another Join function in order not to duplicate fields


def join(df1, df2, cond, how='left'):
    df = df1.join(df2, cond, how=how)
    repeated_columns = [c for c in df1.columns if c in df2.columns]
    for col in repeated_columns:
        df = df.drop(df2[col])
    return df


# Join In multiple fields:

cond = [df.name == df3.name, df.age == df3.age]
df.join(df3, cond, 'outer').select(df.name, df3.age)

# If the fields are called the same…

df.join(df4, ['name', 'age']).select(df.name, df.age)

# With the select part at the end, we avoid duplicated columns (drop duplicated columns otherwise)
# http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html

#Cross join / Cartessian product / Join all against all

df.crossJoin(df2.select("field1"))

# Append / Union  Tables (dfs)

df_concat = df_1.union(df_2)

# Save / Write file externally
# will be saved in HDFS

df.write.csv('mycsv.csv')


# Write a function, store it in HiveContext and use it

# Example:

from pyspark.sql.types import ArrayType, StringType, DateType
from pyspark.sql import SparkSession, HiveContext
def get_matches(src,dst,src_list, dst_list):
    """Get matching elements in src_list and dst_list except if matches src or dst"""
    matches=[]
    for src_conn in src_list:
        print src_conn
        if (src_conn in dst_list) & (src_conn<>dst):
            matches.append(src_conn)
    return matches

hc = HiveContext(sc)
hc.registerFunction('get_matches', get_matches, ArrayType(StringType()))

df_conn_list_mutual=df_conn_list\
    .selectExpr('*','get_matches(src,dst,connections_src,connections_dst) AS mutual_connections')

# Dates (String to Date, Date to String, etc)

# String to Date:

from dateutil.parser import parse
str_date = '2018-05-01'
a = parse(str_date)

# Date to string:

a.strftime('%Y-%m-%d')

# Get weekday
df.select(date_format('capturetime', 'E').alias('weekday'))

# Get Weeks (date)

import datetime
a = datetime.now()
a.isocalendar()[1]

# Go from week (yyyyww) to day (yyyy_mm_dd) format

def get_start_end_dates(yyyyww):
    year = yyyyww[:4]
    week = yyyyww[-2:]
    first_day_year = str(year) + '-' +  '01' + '-' + '01'
    d = parse(first_day_year)
    if(d.weekday()<= 3):
        d = d - timedelta(d.weekday())
    else:
        d = d + timedelta(7-d.weekday())
    dlt = timedelta(days = (int(week)-1)*7)
    return d + dlt,  d + dlt + timedelta(days=6)

# Go easily from yyyy_mm_dd to month (yyyy_mm)

# example using SQL syntax in the F.expr statement

final_table4 = (final_table3\
.withColumn('month_impl',F.\
expr("concat(year(date_signup),'-',LPAD(month(date_signup),2,0))")))


# Add and Substract dates

from datetime import datetime, timedelta
from dateutil.parser import parse

d = datetime.today() - timedelta(days=1305)

# Add or subtract months

def monthdelta(date, delta):
    m, y = (date.month+delta) % 12, date.year + ((date.month)+delta-1) // 12
    if not m: m = 12
    d = min(date.day, [31,
        29 if y%4==0 and not y%400==0 else 28,31,30,31,30,31,31,30,31,30,31][m-1])
    return date.replace(day=d,month=m, year=y)

previous_month = monthdelta(datetime.now(), -1)

# Get the difference between two months

def monthdelta(d1, d2):
    d1=parse(d1)
    d2=parse(d2)
    delta = 0
    while True:
        mdays = monthrange(d1.year, d1.month)[1]
        d1 += timedelta(days=mdays)
        if d1 <= d2:
            delta += 1
        else:
            break
    return delta

monthdelta_F = F.udf(monthdelta,T.IntegerType())

# Difference between two dates (absolute number)

d1 =  '2020-12-01'
d2 =  '2020-12-08'

abs((parse(d2) - parse(d1)).days)

# Difference between two weeks of the same year (absolute number)

# being the week field in this format: YYYYWW or YYYY_WW or YYYY-WW ...

def substract_weeks(w2,w1):
    w2 = int(w2[-2:])
    w1 = int(w1[-2:])
    result = abs(w2-w1)
    return result

substract_weeks = F.udf(substract_weeks,T.IntegerType())

final_table_2 = ( final_table_2.withColumn('signup_act_diff',substract_weeks(F.col('yyyy_ww_activation'),F.col('yyyy_ww_signups')))
)

# Export/save/write dataframe to Hadoop

Spark_df.repartition([200]).write.saveAsTable('schema_name.table_name', format = 'orc', mode = 'overwrite')


# Export/save/write dataframe to Hadoop and append it to existing table

Spark_df.repartition([200]).write.mode("append").insertInto("schema_name.table_name")


# Export/save/write dataframe to Hadoop with Partitions

Spark_df.write.partitionBy('col').saveAsTable('schema_name.table_name', format = 'orc', mode = 'overwrite')

# Exp./save/write dataframe to Hadoop with Partitions and append it to existing table

Spark_df.write.partitionBy('col').saveAsTable('schema_name.table_name', format = 'orc', mode = 'append')

# Delete partitions from existing table in Hadoop

last_month = datetime.now()

initial_month = str(last_month.year) + '-' + str(last_month.month).zfill(2) +  '-' + '01'

num_partitions_to_delete = abs((datetime.now() - parse(initial_month)).days)

day = initial_month

for i in range(num_partitions_to_delete):

    next_day = (parse(day) + timedelta(days=1)).strftime('%Y-%m-%d')
    print day, next_day

    spark.sql(f''' ALTER TABLE schema.table DROP IF EXISTS PARTITION( partitioncol = "{day}" )''')

    day = next_day

# Get statistics from a Hadoop table

# Number of rows
# Number of files
# Size in Bytes
# Number of partition if the table is partitioned

spark.sql(''' ANALYZE TABLE schema.tablename COMPUTE STATISTICS ''')

# Get tables from schema

#example
spark.catalog.listTables('reporting')

# Pandas

# Think VERY carefully before using pandas with spark.
# Using pandas will turn your job into a non-distributed
# job and your driver will be the only executor doing any work.
# Also it will require that all your data fit in this single executor.
# Over time pyspark native dataframes have gained features that were, before,
# only available in pandas. Check if what you want to do can now be done using
# pyspark dataframes.

# you also need to make sure that the DataFrame needs to be small enough
# because all the data will be loaded into the driver’s memory.

# Send spark df to pandas df

df.toPandas()
