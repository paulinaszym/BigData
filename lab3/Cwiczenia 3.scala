// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

import org.apache.spark.sql.functions._

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

val startTimeMillis = System.currentTimeMillis()

//Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
val namesDf2 = namesDf.withColumn("height_in_feet", $"height"/30.48)

//Odpowiedz na pytanie jakie jest najpopularniesze imię?
val namesDf3 = namesDf2.select($"name").withColumn("x", split(col("name"), " ").getItem(0)).groupBy("x").count().orderBy(desc("count")).show()
//John

//Dodaj kolumnę i policz wiek aktorów
val namesDf444 =namesDf2.select(col("*"),
    when(to_date(col("date_of_birth"),"yyyy-MM-dd").isNotNull,
           to_date(col("date_of_birth"),"yyyy-MM-dd"))
    .when(to_date(col("date_of_birth"),"yyyy MM dd").isNotNull,
           to_date(col("date_of_birth"),"yyyy MM dd"))
    .when(to_date(col("date_of_birth"),"MM/dd/yyyy").isNotNull,
           to_date(col("date_of_birth"),"MM/dd/yyyy"))
    .when(to_date(col("date_of_birth"),"yyyy MMMM dd").isNotNull,
           to_date(col("date_of_birth"),"yyyy MMMM dd"))
    .when(to_date(col("date_of_birth"),"yyyy.MMMM.dd").isNotNull,
             to_date(col("date_of_birth"),"yyyy.MMMM.dd"))                                   
    .when(to_date(col("date_of_birth"),"dd.MMMM.yyyy").isNotNull,
           to_date(col("date_of_birth"),"dd.MMMM.yyyy"))
    .otherwise(null).as("birth_date")).na.drop()
val namesDf44= namesDf333.select(col("*"),
    when(to_date(col("date_of_death"),"yyyy-MM-dd").isNotNull,
           to_date(col("date_of_death"),"yyyy-MM-dd"))
    .when(to_date(col("date_of_death"),"yyyy MM dd").isNotNull,
           to_date(col("date_of_death"),"yyyy MM dd"))
    .when(to_date(col("date_of_death"),"MM/dd/yyyy").isNotNull,
           to_date(col("date_of_death"),"MM/dd/yyyy"))
    .when(to_date(col("date_of_death"),"yyyy MMMM dd").isNotNull,
           to_date(col("date_of_death"),"yyyy MMMM dd"))
    .when(to_date(col("date_of_death"),"yyyy.MMMM.dd").isNotNull,
           to_date(col("date_of_death"),"yyyy.MMMM.dd"))                                     
    .when(to_date(col("date_of_death"),"dd.MMMM.yyyy").isNotNull,
           to_date(col("date_of_death"),"dd.MMMM.yyyy"))
    .otherwise(null).as("death_date")).na.drop()
val namesDf4=namesDf33.withColumn("age",round(months_between(col("death_date"),col("birth_date"),true).divide(12)))

//Usuń kolumny (bio, death_details)
val namesDf5 = namesDf.drop("bio").drop("death_details")

//Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
val namesDf6 = namesDf5.select(namesDf5.columns.map(x => col(x).as(x.capitalize.replace('_',' '))): _*).show(false)

//Posortuj dataframe po imieniu rosnąco
val namesDf7 = namesDf5.orderBy(asc("name"))

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
val endTimeMillis = System.currentTimeMillis()
val namesDf1 = namesDf7.withColumn("time", lit((endTimeMillis - startTimeMillis)/1e9d))
  

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

import org.apache.spark.sql.functions._

val startTimeMillis = System.currentTimeMillis()

//Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
val namesDf22=namesDf.select(col("*"),
    when(to_date(col("date_published"),"yyyy-MM-dd").isNotNull,
           to_date(col("date_published"),"yyyy-MM-dd"))
    .when(to_date(col("date_published"),"yyyy MM dd").isNotNull,
           to_date(col("date_published"),"yyyy MM dd"))
    .when(to_date(col("date_published"),"MM/dd/yyyy").isNotNull,
           to_date(col("date_published"),"MM/dd/yyyy"))
    .when(to_date(col("date_published"),"yyyy MMMM dd").isNotNull,
           to_date(col("date_published"),"yyyy MMMM dd"))
    .when(to_date(col("date_published"),"yyyy.MMMM.dd").isNotNull,
             to_date(col("date_published"),"yyyy.MMMM.dd"))                                   
    .when(to_date(col("date_published"),"dd.MMMM.yyyy").isNotNull,
           to_date(col("date_published"),"dd.MMMM.yyyy"))
    .otherwise(null).as("d"))
val namesDf2=namesDf22.withColumn("time",round(months_between(current_date(),col("d"),true).divide(12)))

//Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
val namesDf3 = namesDf.withColumn("budget2", regexp_extract($"budget", "[0-9]+", 0).cast("int"))

//Usuń wiersze z dataframe gdzie wartości są null
val namesDf4 = namesDf3.na.drop()

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
val endTimeMillis = System.currentTimeMillis()
val namesDf1 = namesDf4.withColumn("time", lit((endTimeMillis - startTimeMillis)/1e9d))


// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

import spark.sqlContext.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
val filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

val startTimeMillis = System.currentTimeMillis()
//Dla każdego z poniższych wyliczeń nie bierz pod uwagę nulls
val namesDf2 = namesDf.na.drop()

//Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
val namesDf3 = namesDf.withColumn("mean",(col("votes_10")+col("votes_9")+col("votes_8")+col("votes_7")+col("votes_6")+col("votes_5")+col("votes_4")+col("votes_3")+col("votes_2")+col("votes_1"))/10)
//val namesDf3 = namesDf2.withColumn("median",)

//Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
val namesDf44 = namesDf3.withColumn("diff1",col("mean")-col("weighted_average_vote"))
val namesDf4 = namesDf44.withColumn("diff2",col("median_vote")-col("weighted_average_vote"))

//Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
val sum1 =  namesDf4.select(avg($"males_allages_avg_vote"))
val sum2 =  namesDf4.select(avg($"females_allages_avg_vote"))
//dziewczyny daja lepsze oceny

//Dla jednej z kolumn zmień typ danych do long
val namesDf6 = namesDf2.withColumn("total_votes",col("total_votes").cast(LongType))

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
val endTimeMillis = System.currentTimeMillis()
val namesDf1 = namesDf6.withColumn("time", lit((endTimeMillis - startTimeMillis)/1e9d))

//Zad3
namesDf6.select($"*").explain()
namesDf6.select($"*").groupBy("votes_10").explain()
display(namesDf4)

// COMMAND ----------

//Zad2
//Jobs - pokazuje status wszystkich zadan w aplikacji 
//Stages - pokazuje obecny status wszystkich faz zadan w aplikacji 
//Storage with RDD size and memory use - pokazuje użycie pamięci i partycje
//Environment - pokazuje informacje o środowisku
//Executors - pokazuje podsumowanie zadań (aktywnych, zakończonych i wszystkich)
//SQL - wyświetla działające, zakończone i nieudane zapytania


// COMMAND ----------

//Zad4
val df = sqlContext.read
      .format("jdbc")
      .option("url", "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
      .option("dbtable", "(SELECT * FROM sys.columns) temp")
      .option("user", "sqladmin")
      .option("password", "$3bFHs56&o123$")
      .load()
display(df)
