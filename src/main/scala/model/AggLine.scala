package model

import java.sql.Date

case class AggLine(
                  customerCode: Int,
                  dateFirst: Date,
                  dateLast: Date,
                  employeeIndexMode: Option[String],
                  ageAvg: Option[Double]
                  )