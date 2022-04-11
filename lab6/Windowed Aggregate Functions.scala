// Databricks notebook source
//%sql
//Create database if not exists Sample
spark.sql("Create database if not exists Sample")

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
//CREATE TABLE IF NOT EXISTS Sample.Transactions ( AccountId INT, TranDate DATE, TranAmt DECIMAL(8, 2));
val Transactions_schema = new StructType().add(StructField("AccountId", IntegerType, true))
      .add(StructField("TranDate", StringType, true))
      .add(StructField("TranAmt", DoubleType, true))

//CREATE TABLE IF NOT EXISTS Sample.Logical (RowID INT,FName VARCHAR(20), Salary SMALLINT);
val Logical_schema = new StructType().add(StructField("RowID", IntegerType, true))
      .add(StructField("FName", StringType, true))
      .add(StructField("Salary", IntegerType, true))

// COMMAND ----------

// INSERT INTO Sample.Transactions VALUES 
// ( 1, '2011-01-01', 500),
// ( 1, '2011-01-15', 50),
// ( 1, '2011-01-22', 250),
// ( 1, '2011-01-24', 75),
// ( 1, '2011-01-26', 125),
// ( 1, '2011-01-28', 175),
// ( 2, '2011-01-01', 500),
// ( 2, '2011-01-15', 50),
// ( 2, '2011-01-22', 25),
// ( 2, '2011-01-23', 125),
// ( 2, '2011-01-26', 200),
// ( 2, '2011-01-29', 250),
// ( 3, '2011-01-01', 500),
// ( 3, '2011-01-15', 50 ),
// ( 3, '2011-01-22', 5000),
// ( 3, '2011-01-25', 550),
// ( 3, '2011-01-27', 95 ),
// ( 3, '2011-01-30', 2500)
var Transactions = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row( 1, "2011-01-01", 500.0),
Row( 1, "2011-01-15", 50.0),
Row( 1, "2011-01-22", 250.0),
Row( 1, "2011-01-24", 75.0),
Row( 1, "2011-01-26", 125.0),
Row( 1, "2011-01-28", 175.0),
Row( 2, "2011-01-01", 500.0),
Row( 2, "2011-01-15", 50.0),
Row( 2, "2011-01-22", 25.0),
Row( 2, "2011-01-23", 125.0),
Row( 2, "2011-01-26", 200.0),
Row( 2, "2011-01-29", 250.0),
Row( 3, "2011-01-01", 500.0),
Row( 3, "2011-01-15", 50.0),
Row( 3, "2011-01-22", 5000.0),
Row( 3, "2011-01-25", 550.0),
Row( 3, "2011-01-27", 95.0),
Row( 3, "2011-01-30", 2500.0))), Transactions_schema)
display(Transactions)

// COMMAND ----------

// %sql
// INSERT INTO Sample.Logical
// VALUES (1,'George', 800),
// (2,'Sam', 950),
// (3,'Diane', 1100),
// (4,'Nicholas', 1250),
// (5,'Samuel', 1250),
// (6,'Patricia', 1300),
// (7,'Brian', 1500),
// (8,'Thomas', 1600),
// (9,'Fran', 2450),
// (10,'Debbie', 2850),
// (11,'Mark', 2975),
// (12,'James', 3000),
// (13,'Cynthia', 3000),
// (14,'Christopher', 5000);

var Logical = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1,"George", 800),
Row(2,"Sam", 950),
Row(3,"Diane", 1100),
Row(4,"Nicholas", 1250),
Row(5,"Samuel", 1250),
Row(6,"Patricia", 1300),
Row(7,"Brian", 1500),
Row(8,"Thomas", 1600),
Row(9,"Fran", 2450),
Row(10,"Debbie", 2850),
Row(11,"Mark", 2975),
Row(12,"James", 3000),
Row(13,"Cynthia", 3000),
Row(14,"Christopher", 5000))), Logical_schema)
display(Logical)

// COMMAND ----------

// MAGIC %md 
// MAGIC Totals based on previous row

// COMMAND ----------

// %sql
// SELECT AccountId,
// TranDate,
// TranAmt,
// -- running total of all transactions
// SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTotalAmt
// FROM Sample.Transactions ORDER BY AccountId, TranDate;
import org.apache.spark.sql.expressions.Window

val t1 = Window.partitionBy("AccountId").orderBy("TranDate")
val t2 = Transactions.withColumn("RunTotalAmt", sum("TranAmt").over(t1)).orderBy("AccountId","TranDate")
display(t2)

// COMMAND ----------

// %sql
// SELECT AccountId,
// TranDate,
// TranAmt,
// -- running average of all transactions
// AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunAvg,
// -- running total # of transactions
// COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTranQty,
// -- smallest of the transactions so far
// MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunSmallAmt,
// -- largest of the transactions so far
// MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunLargeAmt,
// -- running total of all transactions
// SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) RunTotalAmt
// FROM Sample.Transactions 
// ORDER BY AccountId,TranDate;

val t3 = Transactions.withColumn("RunAvg", avg("TranAmt").over(t1)).withColumn("RunTranQty", count("*").over(t1)).withColumn("RunSmallAmt", min("TranAmt").over(t1)).withColumn("RunLargeAmt", max("TranAmt").over(t1)).withColumn("RunTotalAmt", sum("TranAmt").over(t1)).orderBy("AccountId","TranDate")
display(t3)

// COMMAND ----------

// MAGIC %md 
// MAGIC * Calculating Totals Based Upon a Subset of Rows

// COMMAND ----------

// %sql
// SELECT AccountId,
// TranDate,
// TranAmt,
// -- average of the current and previous 2 transactions
// AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideAvg,
// -- total # of the current and previous 2 transactions
// COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideQty,
// -- smallest of the current and previous 2 transactions
// MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMin,
// -- largest of the current and previous 2 transactions
// MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMax,
// -- total of the current and previous 2 transactions
// SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideTotal,
// ROW_NUMBER() OVER (PARTITION BY AccountId ORDER BY TranDate) AS RN
// FROM Sample.Transactions 
// ORDER BY AccountId, TranDate, RN

val t11 = Window.partitionBy("AccountId").orderBy("TranDate").rowsBetween(-2,Window.currentRow)
val t4 = Transactions
.withColumn("SlideAvg", avg("TranAmt").over(t11))
.withColumn("SlideQty", count("*").over(t11))
.withColumn("SlideMin", min("TranAmt").over(t11))
.withColumn("SlideMax", max("TranAmt").over(t11))
.withColumn("SlideTotal", sum("TranAmt").over(t11))
.withColumn("RN", row_number().over(t1))
.orderBy("AccountId","TranDate","RN")
display(t4)

// COMMAND ----------

// MAGIC %md
// MAGIC * Logical Window

// COMMAND ----------

// %sql
// SELECT RowID,
// FName,
// Salary,
// SUM(Salary) OVER (ORDER BY Salary ROWS UNBOUNDED PRECEDING) as SumByRows,
// SUM(Salary) OVER (ORDER BY Salary RANGE UNBOUNDED PRECEDING) as SumByRange,
// FROM Sample.Logical
// ORDER BY RowID;

val t111 = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
val t1111 = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow)
val t5 = Logical
.withColumn("SlideAvg", sum("Salary").over(t111))
.withColumn("SlideQty", sum("Salary").over(t1111))
.orderBy("RowID")
display(t5)

// COMMAND ----------

// %sql
// SELECT TOP 10
// AccountNumber,
// OrderDate,
// TotalDue,
// ROW_NUMBER() OVER (PARTITION BY AccountNumber ORDER BY OrderDate) AS RN
// FROM Sales.SalesOrderHeader
// ORDER BY AccountNumber; 

val t6 = Transactions.withColumn("RN", row_number().over(t1)).orderBy("AccountId").limit(10)
display(t6)

// COMMAND ----------

//Zadanie 2
//Użyj ostatnich danych i użyj funkcji okienkowych LEAD, LAG, FIRST_VALUE, LAST_VALUE, ROW_NUMBER i DENS_RANK 
//- Każdą z funkcji wykonaj dla ROWS i RANGE i BETWEEN 
val w1 = Window.orderBy("AccountId").rowsBetween(Window.unboundedPreceding, Window.currentRow)
val w2 = Window.orderBy("AccountId").rangeBetween(Window.unboundedPreceding, Window.currentRow)

val df = Transactions
.withColumn("LEAD", lead("TranAmt",1).over(t1))
.withColumn("LAG", lag("TranAmt",1).over(t1))
.withColumn("FIRST_VALUE", first("TranAmt").over(w1))
.withColumn("FIRST_VALUE2", first("TranAmt").over(w2))
.withColumn("LAST_VALUE", last("TranAmt").over(w1))
.withColumn("LAST_VALUE2", last("TranAmt").over(w2))
.withColumn("ROW_NUMBER", row_number().over(w1))
.withColumn("DENS_RANK", dense_rank().over(w1))
display(df)

// COMMAND ----------

//Zadanie 3
//Użyj ostatnich danych i wykonaj połączenia Left Semi Join, Left Anti Join, za każdym razem sprawdź .explain i zobacz jak spark wykonuje połączenia. Jeśli nie będzie danych to trzeba je zmodyfikować żeby zobaczyć efekty. 
var joinType = "left_semi"
val joinExpression = Transactions.col("AccountId") === Logical.col("RowID")
Transactions.join(Logical, joinExpression, joinType).explain()

Transactions.join(Logical, joinExpression, "left_anti").explain()

// COMMAND ----------

//Zadanie 4
//Połącz tabele po tych samych kolumnach i użyj dwóch metod na usunięcie duplikatów.
val df1 = Transactions.withColumnRenamed("AccountId", "RowID")
Logical.join(df1, df1.col("RowID") === Logical.col("RowID")).drop(df1.col("RowID"))

// COMMAND ----------

//Zadanie 5
//W jednym z połączeń wykonaj broadcast join, i sprawdź planie wykonania. 
import org.apache.spark.sql.functions.broadcast
Transactions.join(broadcast(Logical), Transactions.col("AccountId") === Logical.col("RowID")).explain()
