package driver

import org.apache.spark.sql._

object Load {

  // spark-submit
  //    --class driver.Load
  //    --master local[3]
  //    /vagrant_data/sparkkagglesantander_2.11-1.0.jar
  //    /vagrant_data/train_ver2.csv
  //    /home/spark/parsed
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkKaggleSantander: Load")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val inputPath = args(0)
    val outputPath = args(1)

    val rawLines = sc.textFile(inputPath)

    val parsedLines = rawLines.map(line => model.RawLine.mapLine(line))

    val parsedDf = parsedLines.toDF()
    parsedDf.write.parquet(outputPath)
  }
}
