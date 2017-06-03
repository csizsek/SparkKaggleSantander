package driver

import model._
import org.apache.spark.sql._
import java.sql.Date

object Aggregate {

  // spark-submit
  //    --class driver.Aggregate
  //    --master local[3]
  //    /vagrant_data/sparkkagglesantander_2.11-1.0.jar
  //    /home/spark/parsed
  //    /home/spark/aggregated
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkKaggleSantander: Load")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputPath = args(0)
    val outputPath = args(1)

    def getOptionInt(row: Row, pos: Int): Option[Int] = {
      try {
        Option(row.getInt(pos))
      } catch {
        case _: Exception => None
      }
    }

    def getOptionString(row: Row, pos: Int): Option[String] = {
      try {
        Option(row.getString(pos))
      } catch {
        case _: Exception => None
      }
    }

    def getOptionDate(row: Row, pos: Int): Option[Date] = {
      try {
        Option(row.getDate(pos))
      } catch {
        case _: Exception => None
      }
    }

    val parsedLines = spark.read.parquet(inputPath).rdd.map(row =>
      RawLine(
        row.getDate(0),
        row.getInt(1),

        getOptionString(row, 2),
        getOptionString(row, 3),
        getOptionString(row, 4),
        getOptionInt(row, 5),
        getOptionDate(row, 6),
        getOptionInt(row, 7),
        getOptionInt(row, 8),
        getOptionInt(row, 9),
        getOptionDate(row, 10),
        getOptionInt(row, 11),
        getOptionString(row, 12),
        getOptionString(row, 13),
        getOptionString(row, 14),
        getOptionInt(row, 15),
        getOptionString(row, 16),
        getOptionString(row, 17),
        getOptionInt(row, 18),
        getOptionInt(row, 19),
        getOptionString(row, 20),
        getOptionInt(row, 21),
        getOptionInt(row, 22),
        getOptionString(row, 23),

        getOptionInt(row, 24),
        getOptionInt(row, 25),
        getOptionInt(row, 26),
        getOptionInt(row, 27),
        getOptionInt(row, 28),
        getOptionInt(row, 29),
        getOptionInt(row, 30),
        getOptionInt(row, 31),
        getOptionInt(row, 32),
        getOptionInt(row, 33),
        getOptionInt(row, 34),
        getOptionInt(row, 35),
        getOptionInt(row, 36),
        getOptionInt(row, 37),
        getOptionInt(row, 38),
        getOptionInt(row, 39),
        getOptionInt(row, 40),
        getOptionInt(row, 41),
        getOptionInt(row, 42),
        getOptionInt(row, 43),
        getOptionInt(row, 44),
        getOptionInt(row, 45),
        getOptionInt(row, 46),
        getOptionInt(row, 47)
      ))

    val kvLines = parsedLines.map(line => (line.customerCode, line))

    val grouped = kvLines.groupByKey()

    val sorted = grouped.mapValues(lines => lines.toList.sortBy(line => line.date.getTime))

    println(grouped.count())
  }
}

