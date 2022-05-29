// Databricks notebook source
//Zadanie 2
val df = spark.read.format("json").option("multiLine", "true").load("/FileStore/tables/Nested.json")

display(df.withColumn("features", $"pathLinkInfo".dropFields("alternateName","cycleFacility")))

//2a Sprawdź, jak działa ‘foldLeft’ i przerób kilka przykładów z internetu.
List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).foldLeft(0)((m, n) => m + n)

val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
val res = numbers.foldLeft(0)((m, n) => m + n)
println(res)
numbers.foldLeft(0, {(m: Int, n: Int) => m + n})
numbers.foldLeft(0)(_ + _)

val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
val numberFunc = numbers.foldLeft(List[Int]())_

val squares = numberFunc((xs, x) => xs:+ x*x)
print(squares.toString())

val cubes = numberFunc((xs, x) => xs:+ x*x*x)
print(cubes.toString())

// COMMAND ----------

//2b 
val excludedNestedFields = Map("features" -> Array("alternateName", "cycleFacility"))

excludedNestedFields.foldleft(df){(k,fields)=>( 
  k.withColumn("nowacol",col(fields._1).dropfiels(fields._2:_*)))}

