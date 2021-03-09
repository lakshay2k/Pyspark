// Databricks notebook source
// MAGIC %python
// MAGIC sc

// COMMAND ----------

// MAGIC %python
// MAGIC data = [1,2,3,4,5]
// MAGIC dist=sc.parallelize(data)
// MAGIC dist

// COMMAND ----------

// MAGIC %python
// MAGIC dist.count()

// COMMAND ----------

// MAGIC %python
// MAGIC dist.collect()

// COMMAND ----------

// MAGIC %python
// MAGIC dist.first()

// COMMAND ----------

// MAGIC %python
// MAGIC dist.take(3)

// COMMAND ----------

// MAGIC %python
// MAGIC dist1=sc.textFile("dbfs:/FileStore/shared_uploads/500068760@stu.upes.ac.in/data2.txt")
// MAGIC dist1

// COMMAND ----------

// MAGIC %python
// MAGIC dist1.collect()

// COMMAND ----------

// MAGIC %python
// MAGIC dist1.count()

// COMMAND ----------

sc

// COMMAND ----------

val data = Array(1,2,3,4,5)
val dist = sc.parallelize(data)
dist

// COMMAND ----------

dist.collect()

// COMMAND ----------

dist.count()

// COMMAND ----------

dist.take(3)

// COMMAND ----------

val dist1 = dist.map(x=>x+2)
dist1.collect()

// COMMAND ----------

val file = sc.textFile("dbfs:/FileStore/shared_uploads/500068760@stu.upes.ac.in/data2.txt")
file.collect()

// COMMAND ----------


