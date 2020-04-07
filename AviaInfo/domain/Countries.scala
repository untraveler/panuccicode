package ru.philit.bigdata.vsu.AviaInfo.domain
import org.apache.spark.sql.types._



object Countries extends Enumeration {

  private val DELIMITER = "\\t"
  val NAME, ISO_CODE, DAFIF_CODE    = Value

  val structType = StructType(
    Seq(
      StructField(NAME.toString, StringType),
      StructField(ISO_CODE.toString, StringType),
      StructField(DAFIF_CODE.toString, StringType)

    )
  )

  def apply(row: String): Countries = {
    val array = row.split(DELIMITER, -1)
    Countries(
      array(NAME.id),
      array(ISO_CODE.id),
      array(DAFIF_CODE.id)
    )

  }


}

case class Countries(
                    name:String,
                    iso_code:String,
                    dafif_code:String
                    )