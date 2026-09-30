# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable
from pyspark.sql.window import Window





# COMMAND ----------

# MAGIC %run /Workspace/Users/rohanmanasa26@gmail.com/consolidated_pipeline/setup/alis

# COMMAND ----------

dbutils.widgets.text("catalog","abhi","catalog")
dbutils.widgets.text("data_source","gross_price","data_source")
catalog=dbutils.widgets.get("catalog")
data_source=dbutils.widgets.get("data_source")

base=f"s3://fmcg-project-data-pipeline/{data_source}/*.csv"
print(base)

# COMMAND ----------

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(base)
df.printSchema()


# COMMAND ----------

df.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(f"{catalog}.bronze.new_{data_source}")


# COMMAND ----------

df_bronze=spark.table(f"{catalog}.bronze.{data_source}")
df_bronze.limit(10).display()

# COMMAND ----------



df_silver=df_bronze.withColumn("month",f.coalesce(f.try_to_date(f.col("month"),"yyyy-MM-dd"),
                                              f.try_to_date(f.col("month"),"yyyy/MM/dd"),
                                              f.try_to_date(f.col("month"),"dd-MM-yyyy"),
                                              f.try_to_date(f.col("month"),"dd/MM/yyyy")))
                                     

# COMMAND ----------

df_silver.limit(10).display()

# COMMAND ----------

df_silver=df_silver.withColumn("gross_price",
    f.when(f.col("gross_price").rlike(r'^-?\d+(\.\d+)?$'),
           f.abs(f.col("gross_price").cast("double")))
    .otherwise(f.lit(0.0))
    .cast("double"))
df_silver.limit(10).display()



# COMMAND ----------

df_silver=df_silver.withColumn("product_id",f.col("product_id").cast("string"))
df_silver.limit(10).display()

# COMMAND ----------

df_product=spark.table(f"abhi.silver.products").select("product_id","product_code")
df_silver=df_silver.join(df_product, on="product_id", how="inner")
df_silver.limit(10).display()


# COMMAND ----------

df_silver=df_silver.withColumn("year",f.year("month")).withColumn("is_zero",f.when(f.col("gross_price")==0,1).otherwise(0))

w=Window.partitionBy("product_id","year").orderBy(f.col("is_zero"),f.col("month").desc())
df_silver=df_silver.withColumn("rank",f.row_number().over(w)).filter(f.col("rank")==1).drop("rank")

# COMMAND ----------

df_silver.display()



# COMMAND ----------

df_silver.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(f"{catalog}.silver.new_{data_source}")
 

# COMMAND ----------

df_gold=spark.table(f"{catalog}.silver.new_{data_source}").select("product_code","gross_price","year")
df_gold.limit(10).display()

# COMMAND ----------

df_gold.printSchema()

# COMMAND ----------

df_gold=df_gold.withColumnRenamed("gross_price","price_inr").withColumn("year",f.col("year").cast("bigint")).withColumn("price_inr",f.col("price_inr").cast("bigint"))
df_gold.printSchema()


# COMMAND ----------

df_gold.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(f"{catalog}.gold.new_{data_source}")

# COMMAND ----------

delta_table=DeltaTable.forName(spark,f"{catalog}.gold.dim_{data_source}")
delta_table.alias("t").merge(df_gold.alias("s"),condition="t.product_code=s.product_code").whenMatchedUpdate(
    set={
        "t.price_inr":"s.price_inr",
        "t.year":"s.year"
    }
).whenNotMatchedInsert(
    values={
        "t.product_code":"s.product_code",
        "t.price_inr":"s.price_inr",
        "t.year":"s.year"

    }
).execute()

# COMMAND ----------


