# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/rohanmanasa26@gmail.com/consolidated_pipeline/setup/alis

# COMMAND ----------

dbutils.widgets.text("data_source", "customers", "data_source")
dbutils.widgets.text("catalog", "abhi", "catalog")
data_source = dbutils.widgets.get("data_source")
catalog = dbutils.widgets.get("catalog")

base=f"s3://fmcg-project-data-pipeline/{data_source}/*.csv"
print(base)


# COMMAND ----------

df=spark.read.format('csv').option('header','true').option('inferSchema','true').load(base)
df.printSchema()
df.write.mode('overwrite').format('delta').option("delta.enableChangeDataFeed", "true").saveAsTable(f"{catalog}.bronze.new_{data_source}")


# COMMAND ----------

df_bronze=spark.sql(f"select * from {catalog}.bronze.{data_source}")
df_bronze.limit(5).show()

# COMMAND ----------

df_silver=df_bronze.dropDuplicates(["customer_id"]).withColumn("customer_name",f.trim(f.col("customer_name"))).withColumn("customer_name",f.when(f.col("customer_name").isNull(),None).otherwise(f.initcap(f.col("customer_name"))))

df_silver.select("city").distinct().show()


# COMMAND ----------

city={
    "Bengalore":"Bengaluru",
    "Bengaluruu":"Bengaluru",
    "Hyderabadd":"Hyderabad",
    "Hyderbad":"Hyderabad",
    "NewDelhi":"New Delhi",
    "New Dheli":"New Delhi",
    "NewDelhee":"New Delhi"
}
allowed=["Bengaluru","Hyderabad","New Delhi"]

df_silver=df_silver.replace(city, subset=["city"]).withColumn(
    "city",
    f.when(f.col("city").isNull(), None)
     .when(f.col("city").isin(allowed), f.col("city"))
     .otherwise(None)
)


# COMMAND ----------

df_silver.select("city").distinct().show()

# COMMAND ----------

df_silver.limit(10).show()

# COMMAND ----------

df_silver.filter(f.col("city").isNull()).display()

# COMMAND ----------

null_cities=["SprintX Nutrition",
"ZenAthlete foods",
"PrimeFuel Nutrition",
"PrimeFuel Nutrition",
"Recovery Lane"]

df_null=df_silver.filter(f.col("customer_name").isin(null_cities))
df_null.display()

# COMMAND ----------

city_map={
    "789403":"Bengaluru",
    "789420":"New Delhi",
    "789521":"Hyderabad",
    "789522":"New Delhi",
    "789603":"Hyderabad"

}

df_fix=spark.createDataFrame([(k, v) for k, v in city_map.items()], ["customer_id", "fix_city"])
df_fix.show()




# COMMAND ----------

df_join=df_silver.join(df_fix, on="customer_id", how="left").withColumn("city",f.coalesce("city","fix_city")).drop("fix_city")
df_join.display()


# COMMAND ----------


df_join.write.mode("overwrite").format("delta").option("delta.enableChangeDataFeed", "true").saveAsTable(f"{catalog}.silver.new_{data_source}")

# COMMAND ----------

df_gold=spark.table("abhi.silver.customers")

# COMMAND ----------

df_gold=df_gold.withColumn("customer",f.concat_ws("-","customer_name",f.coalesce("city"))).withColumn("market",f.lit("India")).withColumn("platform",f.lit("Sports")).withColumn("channel",f.lit("Energy Bar"))
df_gold.display()


# COMMAND ----------

df_gold=df_gold.withColumnRenamed("customer_id","customer_code")

# COMMAND ----------

df_gold.write.mode("overwrite").format("delta").option("delta.enableChangeDataFeed", "true").saveAsTable(f"{catalog}.gold.new_dim_{data_source}")
df_child=spark.read.table(f"{catalog}.gold.{data_source}").select("customer_code","customer","market","platform","channel")
df_child.display()

# COMMAND ----------

delta_table=DeltaTable.forName(spark, f"{catalog}.gold.dim_{data_source}")
delta_table.alias("t").merge(df_child.alias("s"), "t.customer_code = s.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

