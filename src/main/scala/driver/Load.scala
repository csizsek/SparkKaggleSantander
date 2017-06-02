package driver

import org.apache.spark.sql.SparkSession

object Load {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GitHubPushCounter")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val inputPath = args(0)

    val rawLines = sc.textFile(inputPath)

    rawLines.take(10).foreach(println)

  }

}
