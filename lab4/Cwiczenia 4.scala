// Databricks notebook source
// MAGIC %md 
// MAGIC Wykożystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()

display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

import org.apache.spark.sql.functions._

val names=tabela.select($"TABLE_NAME").where("TABLE_SCHEMA == 'SalesLT'").as[String].collect.toList

for( i <- names){
  val tab = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()
  
  tab.write.format("delta").mode("overwrite").saveAsTable(i)  
}

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

import org.apache.spark.sql.functions._
//W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach

val low_names = names.map(x => x.toLowerCase())
for( i <- low_names){
  val df = spark.read.format("delta").load(s"/user/hive/warehouse/$i")
  df.select(df.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show //kolumny
  
  df.select(df.columns.map(c => when(col(c).isNull, lit(1)).otherwise(lit(0))).reduce((c1, c2) => c1 + c2) as "sum").show //wiersze
  //Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
  val df11 = df.na.fill("")
  
  //Użyj funkcji drop żeby usunąć nulle,
  val df111 = df.na.drop()  
}

//wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
val df2 = spark.read.format("delta").load("/user/hive/warehouse/salesorderheader")
df2.agg(sum("TaxAmt"),sum("Freight"),avg("TaxAmt"),avg("Freight"),first("TaxAmt"),first("Freight")).show()

//Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg()
val df3 = spark.read.format("delta").load("/user/hive/warehouse/product")
df3.cube("ProductModelId", "Color", "ProductCategoryID").agg(Map("ProductModelId"->"last", "Color"->"first", "Color"->"approx_count_distinct")).show

// COMMAND ----------

// Zadanie 2
// Stwórz 3 funkcje UDF do wybranego zestawu danych,
// a.	Dwie funkcje działające na liczbach, int, double
val plusOneUDF = udf((x: Int) => x + 1)
df3.withColumn("plusOne", plusOneUDF('ProductModelId)).show

val halfUDF = udf((x: Double) => x * 0.5)
df3.withColumn("half", halfUDF('ProductModelId)).show

// b.	Jedna funkcja na string
val upperUDF = udf { s: String => s.toUpperCase }
df3.withColumn("upper", upperUDF('Color)).show

// COMMAND ----------

// Zadanie 3
// Flatten json, wybieranie atrybutów z pliku json.
val df = spark.read.option("multiline","true").json("dbfs:/FileStore/tables/brzydki.json")
display(df)
