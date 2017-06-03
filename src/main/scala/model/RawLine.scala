package model

import java.sql.Date

case class RawLine(
                    // 0. The table is partitioned for this column
                    date: Date,

                    // 1. Customer code
                    customerCode: BigInt,

                    // 2. Employee index: A active, B ex employed, F filial, N not employee, P pasive
                    employeeIndex: Option[String],

                    // 3. Customer's Country residence
                    country: Option[String],

                    // 4. Customer's sex
                    sex: Option[String],

                    // 5. Age
                    age: Option[Int],

                    // 6. The date in which the customer became as the first holder of a contract in the bank
                    firstContract: Option[Date],

                    // 7. New customer Index. 1 if the customer registered in the last 6 months
                    newCustomer: Option[Int],

                    // 8. Customer seniority (in months)
                    seniority: Option[Int],

                    // 9. 1 (First/Primary), 99 (Primary customer during the month but not at the end of the month)
                    churned: Option[Int],

                    // 10. Last date as primary customer (if he isn't at the end of the month)
                    churnDate: Option[Date],

                    // 11. Customer type at the beginning of the month ,1 (First/Primary customer), 2 (co-owner ),
                    // P (Potential),3 (former primary), 4(former co-owner)
                    customerType: Option[Int],

                    // 	12. Customer relation type at the beginning of the month, A (active), I (inactive),
                    // P (former customer),R (Potential)
                    customerRelationType: Option[String],

                    // 13. Residence index (S (Yes) or N (No) if the residence country is the same than the bank country)
                    residenceSame: Option[String],

                    // 14. Foreigner index (S (Yes) or N (No) if the customer's birth country is different than the bank
                    // country)
                    foreigner: Option[String],

                    // 15. Spouse index. 1 if the customer is spouse of an employee
                    spouse: Option[Int],

                    // 16. channel used by the customer to join
                    joinChannel: Option[String],

                    // 17. Deceased index. N/S
                    deceased: Option[String],

                    // 18. Addres type. 1, primary address
                    addressType: Option[Int],

                    // 19. Province code (customer's address)
                    provinceCode: Option[Int],

                    // 20. Province name
                    provinceName: Option[String],

                    // 21. Activity index (1, active customer; 0, inactive customer)
                    active: Option[Int],

                    // 22. Gross income of the household
                    income: Option[Int],

                    // 23. segmentation: 01 - VIP, 02 - Individuals 03 - college graduated
                    segment: Option[String],

                    // products
                    ind_ahor_fin_ult1: Option[Int],
                    ind_aval_fin_ult1: Option[Int],
                    ind_cco_fin_ult1: Option[Int],
                    ind_cder_fin_ult1: Option[Int],
                    ind_cno_fin_ult1: Option[Int],
                    ind_ctju_fin_ult1: Option[Int],
                    ind_ctma_fin_ult1: Option[Int],
                    ind_ctop_fin_ult1: Option[Int],
                    ind_ctpp_fin_ult1: Option[Int],
                    ind_deco_fin_ult1: Option[Int],
                    ind_deme_fin_ult1: Option[Int],
                    ind_dela_fin_ult1: Option[Int],
                    ind_ecue_fin_ult1: Option[Int],
                    ind_fond_fin_ult1: Option[Int],
                    ind_hip_fin_ult1: Option[Int],
                    ind_plan_fin_ult1: Option[Int],
                    ind_pres_fin_ult1: Option[Int],
                    ind_reca_fin_ult1: Option[Int],
                    ind_tjcr_fin_ult1: Option[Int],
                    ind_valo_fin_ult1: Option[Int],
                    ind_viv_fin_ult1: Option[Int],
                    ind_nomina_ult1: Option[Int],
                    ind_nom_pens_ult1: Option[Int],
                    ind_recibo_ult1: Option[Int])

object RawLine {
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

  def mapLine(line: String): model.RawLine = {
    val splitLine = line.split(",")
    model.RawLine(
      java.sql.Date.valueOf(splitLine(0).trim), // date
      splitLine(1).trim.toInt, // customerCode
      parseString(splitLine(2)), // employeeIndex
      parseString(splitLine(3)), // country
      parseString(splitLine(4)), // sex
      parseInt(splitLine(5)), // age
      parseDate(splitLine(6)), // firstContract
      parseInt(splitLine(7)), // newCustomer
      parseInt(splitLine(8)), // seniority
      parseInt(splitLine(9)), // churned
      parseDate(splitLine(10)), // churnDate
      parseInt(splitLine(11)), // customerType
      parseString(splitLine(12)), // customerRelationType
      parseString(splitLine(13)), // residenceSame
      parseString(splitLine(14)), // foreigner
      parseInt(splitLine(15)), // spouse
      parseString(splitLine(16)), // joinChannel
      parseString(splitLine(17)), // deceased
      parseInt(splitLine(18)), // addressType
      parseInt(splitLine(19)), // provinceCode
      parseString(splitLine(20)), // provinceName
      parseInt(splitLine(21)), // active
      parseInt(splitLine(22)), // income
      parseString(splitLine(23)), // segment
      parseInt(splitLine(24)), // ind_ahor_fin_ult1
      parseInt(splitLine(25)), // ind_aval_fin_ult1
      parseInt(splitLine(26)), // ind_cco_fin_ult1
      parseInt(splitLine(27)), // ind_cder_fin_ult1
      parseInt(splitLine(28)), // ind_cno_fin_ult1
      parseInt(splitLine(29)), // ind_ctju_fin_ult1
      parseInt(splitLine(30)), // ind_ctma_fin_ult1
      parseInt(splitLine(31)), // ind_ctop_fin_ult1
      parseInt(splitLine(32)), // ind_ctpp_fin_ult1
      parseInt(splitLine(33)), // ind_deco_fin_ult1
      parseInt(splitLine(34)), // ind_deme_fin_ult1
      parseInt(splitLine(35)), // ind_dela_fin_ult1
      parseInt(splitLine(36)), // ind_ecue_fin_ult1
      parseInt(splitLine(37)), // ind_fond_fin_ult1
      parseInt(splitLine(38)), // ind_hip_fin_ult1
      parseInt(splitLine(39)), // ind_plan_fin_ult1
      parseInt(splitLine(40)), // ind_pres_fin_ult1
      parseInt(splitLine(41)), // ind_reca_fin_ult1
      parseInt(splitLine(42)), // ind_tjcr_fin_ult1
      parseInt(splitLine(43)), // ind_valo_fin_ult1
      parseInt(splitLine(44)), // ind_viv_fin_ult1
      parseInt(splitLine(45)), // ind_nomina_ult1
      parseInt(splitLine(46)), // ind_nom_pens_ult1
      parseInt(splitLine(47)) // ind_recibo_ult1
    )
  }
}
