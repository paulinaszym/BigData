// Databricks notebook source
//Zad1
import org.apache.spark.sql.types._

val schemat = StructType(Array(
StructField("imdb_title_id",StringType,true),
StructField("ordering",IntegerType,true),
StructField("imdb_name_id",StringType,true),
StructField("category",StringType,true),
StructField("job",StringType,true),
StructField("characters",StringType,true)))

val file = spark.read.format("csv").option("header","true").schema(schemat).load("dbfs:/FileStore/tables/Files/actors.csv/")
display(file)

// COMMAND ----------

//Zad2
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schemat = StructType(Array(
StructField("imdb_title_id",StringType,true),
StructField("ordering",StringType,true),
StructField("imdb_name_id",StringType,true),
StructField("category",StringType,true),
StructField("job",StringType,true),
StructField("characters",StringType,true)))

def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
 val reader = spark.read
  Option(schema).foreach(reader.schema)
  reader.json(sc.parallelize(Array(json)))
}

val f = jsonToDataFrame("""
[
   {
      "imdb_title_id":"tt0000009",
      "ordering":1,
      "imdb_name_id":"nm0063086",
      "category":"actress",
      "job":null,
      "characters":"[Miss Geraldine Holbrook (Miss Jerry)]"
   },
   {
      "imdb_title_id":"tt0000009",
      "ordering":2,
      "imdb_name_id":"nm0183823",
      "category":"actor",
      "job":null,
      "characters":"[Mr. Hamilton]"
   },
   {
      "imdb_title_id":"tt0000009",
      "ordering":3,
      "imdb_name_id":"nm1309758",
      "category":"actor",
      "job":null,
      "characters":"[Chauncey Depew - the Director of the New York Central Railroad]"
   }
]
""", schemat)

f.write.json("dbfs:/FileStore/tables/actors1.json")
val df = spark.read.schema(schemat).json("dbfs:/FileStore/tables/actors1.json")
display(df)

// COMMAND ----------

//Zad4
val f1 = spark.read.format("csv").option("header","true").schema(schemat).option("badRecordsPath","/mnt/source/badrecords").load("dbfs:/FileStore/tables/Files/actors.csv")
val f2 = spark.read.format("csv").option("header","true").schema(schemat).option("mode","PERMISSIVE").load("dbfs:/FileStore/tables/Files/actors.csv")
val f3 = spark.read.format("csv").option("header","true").schema(schemat).option("mode","DROPMALFORMED").load("dbfs:/FileStore/tables/Files/actors.csv")
val f4 = spark.read.format("csv").option("header","true").schema(schemat).option("mode","FAILFAST").load("dbfs:/FileStore/tables/Files/actors.csv")

// COMMAND ----------

//Zad5
val file = spark.read.format("csv").option("header","true").schema(schemat).load("dbfs:/FileStore/tables/Files/actors.csv/")
//file.write.parquet("dbfs:/FileStore/tables/Files/actors.parquet")
val parquetFileDF = spark.read.parquet("dbfs:/FileStore/tables/Files/actors.parquet")
//display(parquetFileDF)
//display(dbutils.fs.ls("dbfs:/FileStore/tables/Files/actors.parquet/"))

//file.write.json("dbfs:/FileStore/tables/Files/actors2.json")
val jsonFileDF = spark.read.json("dbfs:/FileStore/tables/Files/actors2.json")
//display(jsonFileDF)
display(dbutils.fs.ls("dbfs:/FileStore/tables/Files/actors2.json/"))
//w przypadku obu formarów w docelowej ścieżce jest kilka plików, w folderze json maja mniejszy rozmiar
