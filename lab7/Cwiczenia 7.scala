// Databricks notebook source
// MAGIC %md
// MAGIC Zadanie 1 <br>
// MAGIC Hive częściowo wspiera indeksy, ale trzeba je manualnie zdefiniować. <br>
// MAGIC Tak, pozwalają one szybciej wyciagnąć dane z tabeli. Ich celem ma być poprawa szybkości zapytań na konkretnych kolumnach tabeli. Dzięki temu przy zapytaniu nie wczytujemy całej tabeli na partycji, ale ładujemy tylko jej część.  <br>
// MAGIC Indeks jest utrzymywany w osobnej tabeli, co rozwiązuje problem jego wpływu na dane w oryginalnej tabeli. 

// COMMAND ----------

//Zadanie 3
import org.apache.spark.sql.types._

def clean(db: String){
  val names=spark.catalog.listTables("$db").select($"TABLE_NAME").as[String].collect.toList
  for( i <- names){
    spark.sql(s"DELETE FROM $db.$i")
  }
}

