// Databricks notebook source
// MAGIC %md
// MAGIC Użyj każdą z tych funkcji 
// MAGIC * `unix_timestamp()` 
// MAGIC * `date_format()`
// MAGIC * `to_unix_timestamp()`
// MAGIC * `from_unixtime()`
// MAGIC * `to_date()` 
// MAGIC * `to_timestamp()` 
// MAGIC * `from_utc_timestamp()` 
// MAGIC * `to_utc_timestamp()`

// COMMAND ----------

import org.apache.spark.sql.functions._

val kolumny = Seq("timestamp","unix", "Date")
val dane = Seq(("2015-03-22T14:13:34", 1646641525847L,"May, 2021"),
               ("2015-03-22T15:03:18", 1646641557555L,"Mar, 2021"),
               ("2015-03-22T14:38:39", 1646641578622L,"Jan, 2021"))

var dataFrame = spark.createDataFrame(dane).toDF(kolumny:_*)
  .withColumn("current_date",current_date().as("current_date"))
  .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
display(dataFrame)

// COMMAND ----------

dataFrame.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ## unix_timestamp(..) & cast(..)

// COMMAND ----------

// MAGIC %md
// MAGIC Konwersja **string** to a **timestamp**.
// MAGIC 
// MAGIC Lokalizacja funkcji 
// MAGIC * `pyspark.sql.functions` in the case of Python
// MAGIC * `org.apache.spark.sql.functions` in the case of Scala & Java

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Zmiana formatu wartości timestamp yyyy-MM-dd'T'HH:mm:ss 
// MAGIC `unix_timestamp(..)`
// MAGIC 
// MAGIC Dokumentacja API `unix_timestamp(..)`:
// MAGIC > Convert time string with given pattern (see <a href="http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html" target="_blank">SimpleDateFormat</a>) to Unix time stamp (in seconds), return null if fail.
// MAGIC 
// MAGIC `SimpleDataFormat` is part of the Java API and provides support for parsing and formatting date and time values.

// COMMAND ----------

val nowyunix = dataFrame.select($"timestamp",unix_timestamp($"timestamp","yyyy-MM-dd'T'HH:mm:ss").cast("timestamp")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC 2. Zmień format zgodnie z klasą `SimpleDateFormat`**yyyy-MM-dd HH:mm:ss**
// MAGIC   * a. Wyświetl schemat i dane żeby sprawdzicz czy wartości się zmieniły

// COMMAND ----------

val zmianaFormatu = dataFrame
  .withColumnRenamed("timestamp", "xxxx")
  .select( $"*", unix_timestamp($"xxxxx", "yyyy-MM-dd HH:mm:ss") )

zmianaFormatu.printSchema()

// COMMAND ----------

val tempE = dataFrame
  .withColumnRenamed("timestamp", "xxxx")
  .select( $"*", unix_timestamp($"xxxx", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
  .withColumnRenamed("CAST(unix_timestamp(capturedAt, yyyy-MM-dd'T'HH:mm:ss) AS TIMESTAMP)", "xxxcast")


// COMMAND ----------

// MAGIC %md
// MAGIC ## Stwórz nowe kolumny do DataFrame z wartościami year(..), month(..), dayofyear(..)

// COMMAND ----------

//Zad3
val tempF = dataFrame.withColumn("Year", year(col("timestamp"))).withColumn("Month", month(col("timestamp"))).withColumn("Day of year", dayofyear(col("timestamp")))
//display(tempF)      

//unix_timestamp()
val a1 = dataFrame.select($"timestamp",unix_timestamp($"timestamp","yyyy-MM-dd'T'HH:mm:ss").as("unix_timestamp")).show()

//date_format()
val a2 = dataFrame.select($"timestamp",date_format($"timestamp","yyyy-MM-dd").as("date_format")).show()

//to_unix_timestamp()
val a3 = dataFrame.select($"timestamp",to_unix_timestamp($"timestamp","yyyy-MM-dd").as("to_unix_timestamp")).show()
// error: not found: value to_unix_timestamp

//from_unixtime()
val a4 = dataFrame.select($"unix",from_unixtime($"unix","yyyy-MM-dd").as("from_unixtime")).show()

//to_date()
val a5 = dataFrame.select($"current_timestamp",to_date($"current_timestamp","yyyy-MM-dd").as("to_date")).show()

//to_timestamp()
val a6 = dataFrame.select($"current_date",to_timestamp($"current_date","yyyy-MM-dd").as("to_timestamp")).show()

//to_utc_timestamp()
val a8 = dataFrame.select($"current_date",to_utc_timestamp($"current_date","CET").as("to_utc_timestamp"))
a8.show()

//from_utc_timestamp()
val a7 = a8.select($"to_utc_timestamp",from_utc_timestamp($"to_utc_timestamp","CET").as("from_utc_timestamp")).show()
