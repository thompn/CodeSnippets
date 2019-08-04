## Overview

# Code snippets and commonly used lines for connecting to spark in R.

## Imports + config

# Importing common packages and setting spark context.

library(sparklyr)
library(dplyr)
library(ggplot2)

sc <- spark_connect(master = "yarn-client", version="1.6.0", spark_home = '/path/to/spark')


## Cache Hive table into Spark

tbl_cache(sc, 'table')
my_tbl <- tbl(sc, 'table')
