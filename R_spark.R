---
title: "Connecting to Spark in R"
author: "Author: Nick Thompson"
date: "Last update: `r format(Sys.time(), '%d %B, %Y')`"
output:
  html_document:
    toc: true
    toc_float:
        collapsed: true
        smooth_scroll: true
    toc_depth: 3
    fig_caption: yes
    code_folding: show
    number_sections: true

fontsize: 14pt
---

<!---
- Compile from command-line
Rscript -e "rmarkdown::render('sample.Rmd', c('html_document'), clean=FALSE)"
-->

## Overview

Code snippets and commonly used lines for connecting to spark in R.

## Imports + config

Importing common packages and setting spark context.

```{r}
library(sparklyr)
library(dplyr)
library(ggplot2)

sc <- spark_connect(master = "yarn-client", version="1.6.0", spark_home = '/path/to/spark')
```

## Cache flights Hive table into Spark

```{r}
tbl_cache(sc, 'table')
my_tbl <- tbl(sc, 'table')
```
