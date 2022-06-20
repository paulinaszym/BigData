package bigdata

import bigdata.case_classes.Flights
import bigdata.datareader.DataReader
import bigdata.transformations.Transformation
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object Main {

  lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Main")
      .getOrCreate();

    import spark.implicits._
    val df = new DataReader().readData(spark,"2010-summary.csv").as[Flights]

    logger.info("Komunikat do logga")

    df.filter(row => new Transformation(150).airport_filtering(row)).show()


  }
}
