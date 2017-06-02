package model

import java.time.LocalDate

case class RawLine(
                    date: LocalDate,
                    customerCode: BigInt,
                    employeeIndex: Char
                  )

