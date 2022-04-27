import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkProject extends App {
    val spark:SparkSession = SparkSession.builder().master("local[1]").getOrCreate()
    val filePath = "C:\\AGH\\Infrastruktura Big Data\\names.csv"
    val namesDf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(filePath)
    val namesDf2 = namesDf.withColumn("height_in_feet", col("height")/30.48)
    namesDf2.show
}