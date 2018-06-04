package beemap

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utl.SparkBase
import utl.Tools

import scala.util.control.Breaks.break

class Agg_beemap(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val lcprep_path = propMap("lcprep_path")
  val lcpost_path = propMap("lcpost_path")
  val dmcell_path = propMap("dmcell_path")
  val outroam_path = propMap("outroam_path")
  val outroams_path = propMap("outroams_path")
  val map_kpi_path = propMap("map_kpi_path")
  val kpi_stat_path = propMap("kpi_stat_path")

  val calc_date = propMap("calc_date")
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName// + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  var status = true
  import spark.implicits._

  def RunJob():Boolean= {
    if (spark != null)
      try {

        val co = spark.read.parquet("/dmp/beemap/temp/csv/counters/" + tools.patternToDate(calc_date, "yyyyMM"))
        //val co = spark.read.parquet("/dmp/beemap/temp/kpi_final/" + tools.patternToDate(calc_date, "yyyyMM"))
        val loc = spark.read.parquet("/dmp/beemap/temp/hw_location/" + tools.patternToDate(calc_date, "yyyyMM"))
        val stat = spark.read.parquet("/dmp/beemap/temp/kpi_stations/" + tools.patternToDate(calc_date, "yyyyMM"))
        val sg = spark.read.parquet("/dmp/beemap/temp/site_geo/" + tools.patternToDate(calc_date, "yyyyMM"))

        val t1 = stat.join(loc, stat("site_id") === loc("site_id"))
          .join(co, stat("site_id") === co("site_id"))
          .select(
            co("cell_name"),
            co("enodeb_id"),
            co("enodeb_name"),
            co("cdr_cs_2g"),
            co("CunSSR_CS_2G"),
            co("CDR_CS_3G"),
            co("rab_dr_ps_3g"),
            co("cunssr_cs_3g"),
            co("CunSSR_PS_3G"),
            co("User_Throughput_3G"),
            co("cong_power_ul_dl_3g"),
            co("CONG_CE_UL_DL_3G"),
            co("CONG_CODE_3G"),
            co("CunSSR_PS_4G"),
            co("util_prb_dl_4g"),
            co("RAB_DR_4G"),
            co("User_Throughput_4G"),
            co("site_id"),
            co("lac"),
            co("cell"),
            co("type"),
            co("cell_owner_id"),
            co("site_name"),
            co("region"),
            co("district"),
            co("location"),
            co("location_type"),
            loc("work_count"),
            loc("home_count"),
            stat("VOICE_IN_B_L"),
            stat("VOICE_IN_B_F"),
            stat("VOICE_IN_MEZH"),
            stat("VOICE_OUT_B_L"),
            stat("VOICE_OUT_B_F"),
            stat("VOICE_OUT_MEZH"),
            stat("VOICE_TOTAL"),
            stat("DATA_VOLUME_MB"),
            stat("REVENUE_VOICE"),
            stat("REVENUE_GPRS"),
            stat("REVENUE_OTHER_P"),
            stat("REVENUE_O_R_P"),
            stat("GUEST_VOICE_DURATION_MIN"),
            stat("GUEST_DATA_VOLUME_MB"),
            stat("GUEST_CHARGE_AMT_USD"),
            stat("GUEST_CHARGE_AMT"),
            stat("TOTAL_REVENUE"),
            stat("GRPS_VOICE_MB"),
            stat("INTERCONNECT_EXPENCES"),
            stat("INTERCONNECT_REVENUE"),
            stat("INTERCONNECT_R"),
            stat("GUEST_ROAMING_R_PL"),
            stat("GUEST_ROAMING_MARGIN"),
            stat("RENT_SUM"),
            stat("TOTAL_SUBSCRIBER_AMT"),
            stat("TOTAL_DATA_USERS_AMT"),
            stat("ARPU"),
            stat("ARPU_DATA"),
            stat("MOU"),
            stat("MBOU")
          ).cache()

        t1.join(sg, t1("site_id") === sg("site_id"))
          .groupBy(lit(calc_date).as("TIME_KEY"),
            sg("ID_OBJECT"),
            sg("ID_SUBOBJECT_TYPE"))
          .agg(
            lit(calc_date.substring(0,4)).as("YEAR"),
            lit(calc_date.substring(5,7)).as("MONTH"),
            lit(1).as("DAY"),
            lit(0).as("HOUR"),
            lit(0).as("MINUTE"),
            lit(1).as("ID_TIME_MODE"),
          avg(t1("cdr_cs_2g")).as("CDR_2G"),
          avg(t1("CunSSR_CS_2G")).as("CunSSR_2G"),
          avg(t1("CDR_CS_3G")).as("CDR_CS_3G"),
          avg(t1("rab_dr_ps_3g")).as("RAB_DR_PS_3G"),
          avg(t1("cunssr_cs_3g")).as("CunSSR_CS_3G"),
          avg(t1("CunSSR_PS_3G")).as("CunSSR_PS_3G"),
          avg(t1("User_Throughput_3G")).as("Throughput_3G"),
          avg(t1("cong_power_ul_dl_3g")).as("CONG_POWER_UL_DL_3G"),
          avg(co("CONG_CE_UL_DL_3G")).as("CONG_CE_UL_DL_3G"),
          avg(co("CONG_CODE_3G")).as("CONG_CODE_3G"),
          avg(t1("CunSSR_PS_4G")).as("CunSSR_PS_4G"),
          avg(t1("util_prb_dl_4g")).as("Util_PRB_DL_4G"),
          avg(t1("RAB_DR_4G")).as("RAB_DR_PS_4G"),
          avg(t1("User_Throughput_4G")).as("Throughput_4G"),

          sum(t1("WORK_COUNT")).as("WORK_CTN_COUNT"),
          sum(t1("HOME_COUNT")).as("HOME_CTN_COUNT"),
          sum(t1("VOICE_IN_B_L")).as("VOICE_IN_B_L"),
          sum(t1("VOICE_IN_B_F")).as("VOICE_IN_B_F"),
          sum(t1("VOICE_IN_MEZH")).as("VOICE_IN_MEZH"),
          sum(t1("VOICE_OUT_B_L")).as("VOICE_OUT_B_L"),
          sum(t1("VOICE_OUT_B_F")).as("VOICE_OUT_B_F"),
          sum(t1("VOICE_OUT_MEZH")).as("VOICE_OUT_MEZH"),
          sum(t1("VOICE_TOTAL")).as("VOICE_TOTAL"),
          sum(t1("DATA_VOLUME_MB")).as("DATA_VOLUME_MB"),
          sum(t1("REVENUE_VOICE")).as("REVENUE_VOICE"),
          sum(t1("REVENUE_GPRS")).as("REVENUE_GPRS"),
          sum(t1("REVENUE_OTHER_P")).as("REVENUE_OTHER_P"),
          sum(t1("REVENUE_O_R_P")).as("REVENUE_O_R_P"),
          sum(t1("GUEST_VOICE_DURATION_MIN")).as("GUEST_VOICE_DURATION_MIN"),
          sum(t1("GUEST_DATA_VOLUME_MB")).as("GUEST_DATA_VOLUME_MB"),
          sum(t1("GUEST_CHARGE_AMT_USD")).as("GUEST_CHARGE_AMT_USD"),
          sum(t1("GUEST_CHARGE_AMT")).as("GUEST_CHARGE_AMT"),
          sum(t1("TOTAL_REVENUE")).as("TOTAL_REVENUE"),
          sum(t1("GRPS_VOICE_MB")).as("GRPS_VOICE_MB"),
          sum(t1("INTERCONNECT_EXPENCES")).as("INTERCONNECT_COST"),
          sum(t1("INTERCONNECT_REVENUE")).as("INTERCONNECT_REV"),
//          sum(t1("INTERCONNECT_R")).as("").as("INTERCONNECT_R"),
          sum(t1("GUEST_ROAMING_R_PL")).as("GUEST_ROAMING_R_PL"),
          sum(t1("GUEST_ROAMING_MARGIN")).as("GUEST_ROAMING_MARGIN"),
          sum(t1("RENT_SUM")).as("RENT_SUM"),
          sum(t1("TOTAL_SUBSCRIBER_AMT")).as("TOTAL_SUBSCRIBER_AMT"),
          sum(t1("TOTAL_DATA_USERS_AMT")).as("TOTAL_DATA_USERS_AMT")
//          sum(t1("ARPU")).as("ARPU"),
//          sum(t1("ARPU_DATA")).as("ARPU_DATA"),
//          sum(t1("MOU")).as("MOU"),
//          sum(t1("MBOU")).as("MBOU")
        ).orderBy($"ID_OBJECT",$"ID_SUBOBJECT_TYPE")
          .write.mode("overwrite")
            .parquet("/dmp/beemap/temp/agg_beemap/" + tools.patternToDate(calc_date, "yyyyMM"))

        // Write to MySQL
         val prop = new java.util.Properties
         prop.setProperty("driver", "com.mysql.jdbc.Driver")
         prop.setProperty("user", "DDosmukhamedov")
         prop.setProperty("password", "HP1nvent")

         val url = "jdbc:mysql://kz-dmpignt05:3306/kpi"

           spark.read.parquet("/dmp/beemap/temp/agg_beemap/" + tools.patternToDate(calc_date, "yyyyMM"))
             .write.mode("append").jdbc(url,"kpi_stations",prop)

        spark.close()

      } catch {
        case e: Exception => println(s"ERROR: $e")
          status = false
          if (spark != null) spark.close()
      } finally {}
    status
  }
}