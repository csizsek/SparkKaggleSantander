package driver

import java.sql.Date

import org.apache.spark.sql._

object Load {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GitHubPushCounter")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val inputPath = args(0)
    val outputPath = args(1)

    val rawLines = sc.textFile(inputPath)

    val parsedLines = rawLines.map(line => mapLine(line))

    val parsedDf = parsedLines.toDF()
    parsedDf.write.parquet(outputPath)
  }

  def mapLine(line: String): model.RawLine = {

    def parseString(s: String): Option[String] = {
      if (s.trim.length > 0)
        try {
          Option(s.trim)
        } catch {
          case _: Exception => None
        }
      else
        None
    }

    def parseDate(s: String): Option[Date] = {
      if (s.trim.length > 0)
        try {
          Option(java.sql.Date.valueOf(s.trim))
        } catch {
          case _: Exception => None
        }
      else
        None
    }

    def parseInt(s: String): Option[Int] = {
      if (s.trim.length > 0)
        try {
          Option(s.trim.toInt)
        } catch {
          case _: Exception => None
        }
      else
        None
    }

    val splitLine = line.split(",")

    model.RawLine(
      java.sql.Date.valueOf(splitLine(0).trim),           // date
      splitLine(1).trim.toInt,                            // customerCode
      parseString(splitLine(2)),                          // employeeIndex
      parseString(splitLine(3)),                          // country
      parseString(splitLine(4)),                          // sex
      parseInt(splitLine(5)),                             // age
      parseDate(splitLine(6)),                            // firstContract
      parseInt(splitLine(7)),                             // newCustomer
      parseInt(splitLine(8)),                             // seniority
      parseInt(splitLine(9)),                             // churned
      parseDate(splitLine(10)),                           // churnDate
      parseInt(splitLine(11)),                            // customerType
      parseString(splitLine(12)),                         // customerRelationType
      parseString(splitLine(13)),                         // residenceSame
      parseString(splitLine(14)),                         // foreigner
      parseInt(splitLine(15)),                            // spouse
      parseString(splitLine(16)),                         // joinChannel
      parseString(splitLine(17)),                         // deceased
      parseInt(splitLine(18)),                            // addressType
      parseInt(splitLine(19)),                            // provinceCode
      parseString(splitLine(20)),                         // provinceName
      parseInt(splitLine(21)),                            // active
      parseInt(splitLine(22)),                            // income
      parseString(splitLine(23)),                         // segment
      parseInt(splitLine(24)),                            // ind_ahor_fin_ult1
      parseInt(splitLine(25)),                            // ind_aval_fin_ult1
      parseInt(splitLine(26)),                            // ind_cco_fin_ult1
      parseInt(splitLine(27)),                            // ind_cder_fin_ult1
      parseInt(splitLine(28)),                            // ind_cno_fin_ult1
      parseInt(splitLine(29)),                            // ind_ctju_fin_ult1
      parseInt(splitLine(30)),                            // ind_ctma_fin_ult1
      parseInt(splitLine(31)),                            // ind_ctop_fin_ult1
      parseInt(splitLine(32)),                            // ind_ctpp_fin_ult1
      parseInt(splitLine(33)),                            // ind_deco_fin_ult1
      parseInt(splitLine(34)),                            // ind_deme_fin_ult1
      parseInt(splitLine(35)),                            // ind_dela_fin_ult1
      parseInt(splitLine(36)),                            // ind_ecue_fin_ult1
      parseInt(splitLine(37)),                            // ind_fond_fin_ult1
      parseInt(splitLine(38)),                            // ind_hip_fin_ult1
      parseInt(splitLine(39)),                            // ind_plan_fin_ult1
      parseInt(splitLine(40)),                            // ind_pres_fin_ult1
      parseInt(splitLine(41)),                            // ind_reca_fin_ult1
      parseInt(splitLine(42)),                            // ind_tjcr_fin_ult1
      parseInt(splitLine(43)),                            // ind_valo_fin_ult1
      parseInt(splitLine(44)),                            // ind_viv_fin_ult1
      parseInt(splitLine(45)),                            // ind_nomina_ult1
      parseInt(splitLine(46)),                            // ind_nom_pens_ult1
      parseInt(splitLine(47))                             // ind_recibo_ult1
    )
  }

}
