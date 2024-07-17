# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog demo_olymbics

# COMMAND ----------

df = spark.read.csv('abfss://tokyo-olymbics@dbstorageb2h4nhthophfk.dfs.core.windows.net/Raw/Teams.csv')
athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://tokyo-olymbics@dbstorageb2h4nhthophfk.dfs.core.windows.net/Raw/Athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://tokyo-olymbics@dbstorageb2h4nhthophfk.dfs.core.windows.net/Raw/Coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://tokyo-olymbics@dbstorageb2h4nhthophfk.dfs.core.windows.net/Raw/EntriesGender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://tokyo-olymbics@dbstorageb2h4nhthophfk.dfs.core.windows.net/Raw/Medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://tokyo-olymbics@dbstorageb2h4nhthophfk.dfs.core.windows.net/Raw/Teams.csv")

# COMMAND ----------

athletes.write.mode('overwrite').format('delta').save('abfss://tokyo-olymbics@dbstorageb2h4nhthophfk.dfs.core.windows.net/Transformed/athletes')
coaches.write.mode('overwrite').format('delta').save('abfss://tokyo-olymbics@dbstorageb2h4nhthophfk.dfs.core.windows.net/Transformed/coaches')
entriesgender.write.mode('overwrite').format('delta').save('abfss://tokyo-olymbics@dbstorageb2h4nhthophfk.dfs.core.windows.net/Transformed/entriesgender')
medals = medals.withColumnRenamed("Rank by Total", "Rank_By_Total")
medals.write.mode('overwrite').format('delta').save('abfss://tokyo-olymbics@dbstorageb2h4nhthophfk.dfs.core.windows.net/Transformed/medals')
teams.write.mode('overwrite').format('delta').save('abfss://tokyo-olymbics@dbstorageb2h4nhthophfk.dfs.core.windows.net/Transformed/teams')
