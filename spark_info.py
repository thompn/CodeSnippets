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

    # use dynamic resource allocation
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

# write to new ORC table (in overwrite mode)
df.write.saveAsTable('TABLE NAME', format='orc', mode='overwrite')
