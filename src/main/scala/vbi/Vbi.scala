package vbi

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utl.SparkBase
import utl.Tools

class Vbi(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

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
  import spark.implicits._

  def RunJob():Unit= {
    var status = true
    if (spark != null)
      try {

        val sc = spark.read.parquet("/dmp/daily_stg/otpe/subs_charges_or/*/")
        val ne = spark.read.parquet("/dmp/daily_stg/netl/bi_netl_daily/*/")
        val vc = spark.read.option("delimiter", ";").option("header", "true").csv("/dmp/upload/vbi/vbi_group.csv")
        val dbs = spark.read.parquet("/mnt/gluster-storage/etl/download/biis/dim_business_service/")
        val dbst = spark.read.parquet("/mnt/gluster-storage/etl/download/biis/dim_business_service_type/")

        // vbi charges det
        val vbi_ch_det = sc.filter($"vt_date" < date_sub(current_date, 1) && $"vt_date" >= date_sub(current_date, 31) && $"charge_type".isin("O", "R"))
          .join(vc, sc("charge_type") === vc("charge_type"), "left")
          .groupBy(sc("subs_key"), sc("ban_key"), sc("account_type_key"), sc("business_service_desc_rus"),
            when(vc("CHARGE_GROUP_TYPE").isNull, lit("Others")).otherwise(vc("CHARGE_GROUP_TYPE")).as("CHARGE_GROUP_TYPE"))
          .agg(sum(sc("charge_amt") + sc("vat_amt")).as("charge_amount"))
          .filter($"charge_amount"===0)

        // vbi international
        ne.filter($"vt_date" < date_sub(current_date, 1) && $"vt_date" >= date_sub(current_date, 31) &&
          $"is_roam" === 0 && $"is_international" === 1)
          .groupBy(
            $"ctn",
            $"ban",
            $"account_type_key",
            $"call_type_desc",
            $"ctn_b_country")
          .agg(sum($"charge_amt" + $"vat_amt").as("charge_amount"))
          .filter($"charge_amount" > 0)

        // vbi roaming det
        ne.filter($"vt_date" < date_sub(current_date, 1) && $"vt_date" >= date_sub(current_date, 31) &&
          $"is_roam" === 1 && $"is_international" === 0)
          .groupBy(
            $"ctn",
            $"ban",
            $"account_type_key",
            $"call_type_desc",
            $"roam_network",
            $"roam_country",
            $"roam_zone_desc")
          .agg(sum($"charge_amt" + $"vat_amt").as("charge_amount"))
          .filter($"charge_amount" > 0)

        var df:org.apache.spark.sql.DataFrame=null;

        var i=3
        while (i<33) {
          df = df.union(spark.read.parquet("/dmp/daily_stg/tg_det_layer_trans_parq/"+tools.patternToDate(tools.addDays(P_DATE,-i),"ddMMyyyy")))
          i=i+1
        }

        df.filter(not($"SRC".isin("POST","RHA")))
          .join(dbs, df("BUSINESS_SERVICE_KEY") === dbs("BUSINESS_SERVICE_KEY"),"left")
          .join(dbst, dbs("BUSINESS_SERVICE_TYPE_KEY") === dbst("BUSINESS_SERVICE_TYPE_KEY"),"left")
          .join(vc, df("BUSINESS_SERVICE_KEY") === vc("BUSINESS_SERVICE_KEY"),"left")
          .groupBy(
            df("SUBS_KEY"),
            df("BAN_KEY"))
          .agg(
            sum(when(df("SRC").isin("CHA","PSA","GPRS","ROAM") && dbst("REVENUE_STREAM") === 3, df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)).as("SMS_PG"),
            sum(when(df("SRC").isin("CHA","PSA","GPRS","ROAM") && dbst("REVENUE_STREAM") === 9, df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)).as("ROAMING_PG"),
            sum(when(df("SRC").isin("CHA","PSA","GPRS","ROAM") && dbst("REVENUE_STREAM") === 4, df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)).as("INTERNET_PG"),
            sum(when(df("SRC").isin("CHA","PSA","GPRS","ROAM") && dbst("REVENUE_STREAM").isin(6,5,2) ||
              df("SRC").isin("CHA","PSA","GPRS","ROAM") && dbst("REVENUE_STREAM")===1 && df("CALL_TYPE_CODE")==="V" &&
              df("LOCATION_TYPE_KEY").isin(1,2) && not(df("CONNECTION_TYPE_KEY").isin(1,2,3)),
              df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)+
              when(df("SRC")==="CHR" && df("CHARGE_TYPE").isin("O","R") && vc("CHARGE_GROUP_TYPE")==="VAS",1).otherwise(0)
            ).as("VAS"),
            sum(when(df("SRC").isin("CHA","PSA","GPRS","ROAM") && dbst("REVENUE_STREAM") === 51, df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)).as("MFS"),
            sum(when(df("SRC").isin("CHA","PSA","GPRS","ROAM") && dbst("REVENUE_STREAM").isin(13,10,12), df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)).as("OTHER_SERVICES"),
            sum(when(df("SRC").isin("CHA","PSA","GPRS","ROAM") && dbst("REVENUE_STREAM")===1 && df("CALL_TYPE_CODE")==="V" &&
              df("LOCATION_TYPE_KEY")===3, df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)).as("INTERNATIONAL_PG"),
            sum(when(df("SRC").isin("CHA","PSA","GPRS","ROAM") && dbst("REVENUE_STREAM")===1 && df("CALL_TYPE_CODE")==="V" &&
              df("LOCATION_TYPE_KEY").isin(1,2) && df("CONNECTION_TYPE_KEY")===3 &&
              df("TERM_PARENT_OPERATOR_CODE")===130 && df("ORIG_PARENT_OPERATOR_CODE")===130, df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)).as("ONNET_PG"),
            sum(when(df("SRC").isin("CHA","PSA","GPRS","ROAM") && dbst("REVENUE_STREAM")===1 && df("CALL_TYPE_CODE")==="V" &&
              df("LOCATION_TYPE_KEY").isin(1,2) && df("CONNECTION_TYPE_KEY")===3 &&
              (df("TERM_PARENT_OPERATOR_CODE")===130 || df("ORIG_PARENT_OPERATOR_CODE")===130) &&
              df("ORIG_PARENT_OPERATOR_CODE")!=df("TERM_PARENT_OPERATOR_CODE"), df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)).as("OFFNET_PG"),
            sum(when(df("SRC").isin("CHA","PSA","GPRS","ROAM") && dbst("REVENUE_STREAM")===1 && df("CALL_TYPE_CODE")==="V" &&
              df("LOCATION_TYPE_KEY").isin(1,2) && df("CONNECTION_TYPE_KEY").isin(1,2), df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)).as("CITY_PG"),
            sum(when(df("SRC").isin("CHA","PSA","GPRS","ROAM") && dbst("REVENUE_STREAM")===1 && df("CALL_TYPE_CODE")!="V",
              df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)).as("OTHER_VOICE_PG"),
            sum(when(df("SRC").isin("CHR") && df("CHARGE_TYPE").isin("O","R") && vc("CHARGE_GROUP_TYPE")==="One_time_charge",
              df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)).as("ONE_TIME_SERVICE_FEE"),
            sum(when(df("SRC").isin("CHR") && df("CHARGE_TYPE").isin("O","R") && vc("CHARGE_GROUP_TYPE")==="Regular_fee",
              df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)).as("REGULAR_SERVICE_FEE"),
            sum(when(df("SRC").isin("CHR") && df("CHARGE_TYPE").isin("O","R") && vc("CHARGE_GROUP_TYPE")==="Roaming",
              df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)).as("ROAMING_FEE"),
            sum(when(df("SRC").isin("CHR") && df("CHARGE_TYPE").isin("O","R") &&
              (vc("CHARGE_GROUP_TYPE").isNull || vc("CHARGE_GROUP_TYPE")==="Others"),df("CHARGE_AMT")+df("VAT_AMT")).otherwise(0)).as("OTHERS_FEE")
                  )

      } catch {
        case e: Exception => println (s"ERROR: $e")
          status = false
          if (spark != null) spark.close ()
      } finally {}
    else status = false
  }
}