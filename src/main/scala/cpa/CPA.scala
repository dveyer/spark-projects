package cpa

import utl.SparkBase
import utl.Tools
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark.sql.functions._

class CPA(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName// + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._
  val user = "dmp"
  val pass = "nslasdf44"
  val host = "172.23.24.173"
  val port = 1521
  val db = "BEEDB1"
  val t1 = "dmp_cpa_daily"
  val jdbcUrl = s"jdbc:oracle:thin:@${host}:${port}:${db}"
  var status = true

  def RunJob():Boolean= {
       if (spark != null)
    try {
        //var v_date = "2018-05-30"
        val dcl = spark.read.option("header","true").option("delimiter",";").csv("/dmp/upload/cpa/*.csv")

        //while (v_date <= e_date) {
          val dl = spark.read.parquet("/dmp/daily_stg/tg_det_layer_trans_parq/" + tools.patternToDate(tools.addDays(P_DATE,-3), "ddMMyyyy"))
          //val dl = spark.read.parquet("/dmp/daily_stg/tg_det_layer_trans_parq/31052018")

          val scd = dl.filter($"SRC".isin("CHA", "PSA", "POST") && $"CHARGE_AMT" != 0 && $"COUNTED_CDR_IND" === 1 && $"CALL_TYPE_CODE".isin("V", "S"))
            .join(dcl, dl("BUSINESS_SERVICE_KEY") === dcl("BUSINESS_SERVICE_KEY"))
            .select(
              dl("SUBS_KEY"),
              dl("BAN_KEY"),
              dl("CALL_START_TIME").cast(org.apache.spark.sql.types.DateType).as("TIME_KEY"),
              dcl("BUSINESS_SERVICE_KEY"),
              dcl("BUSINESS_SERVICE_DESC_ENG"),
              dl("CHARGE_AMT"),
              dl("VAT_AMT"),
              when(dl("CHARGE_AMT") > 0, lit("CHARGING EVENT")).otherwise(
                when(dl("CHARGE_AMT") < 0, lit("REFUND EVENT"))).as("TRANS_TYPE"),
              when(dl("CHARGE_AMT") > 0, 1).otherwise(
                when(dl("CHARGE_AMT") < 0, -1)).as("EVENT_COUNTER")
            ).cache()

          println("Data is selected...")

          scd.groupBy($"TIME_KEY",
            $"SUBS_KEY",
            $"BAN_KEY",
            $"BUSINESS_SERVICE_KEY",
            $"BUSINESS_SERVICE_DESC_ENG")
            .agg(
              sum(when($"EVENT_COUNTER".isNull, 0).otherwise($"EVENT_COUNTER")).as("EVENT_COUNT"),
              sum($"CHARGE_AMT" + $"VAT_AMT").as("SERVICE_REVENUE")
            ) //.write.parquet("/dmp/daily_stg/cpa/"+tools.patternToDate(v_date,"ddMMyyyy"))
            .write.mode("append")
            .format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", t1)
            .option("user", user)
            .option("password", pass)
            .save()
          //v_date = tools.addDays(v_date,1)}

    spark.close()
  } catch {
    case e: Exception => println (s"ERROR: $e")
    status = false
    if (spark != null) spark.close ()
  } finally {}
    status
  }
}