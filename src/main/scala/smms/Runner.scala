package smms

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utl.SparkBase
import utl.Tools

import scala.util.control.Breaks.break

class Runner(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val sFolder = propMap("sfolder")   // Stagings folder
  val dsFolder = propMap("dsfolder")
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._

  val sl_path = "/dmp/upload/smms/sellers.csv"
  val rq_path = "/mnt/gluster-storage/etl/download/smms/smms/" // + tools.patternToDate(P_DATE,"MM-yyyy") + "/"
  // val x = tools.dateDiff(tools.firstMonthDay(P_DATE,"yyyy-MM-dd"),P_DATE)
  val x = tools.dateDiff("2018-03-01",P_DATE)
  val smms_agg_rating_path = sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-1),"ddMMyyyy") + "/" + "smms_agg_rating"
  val smms_agg_rating_quar_path = sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-1),"ddMMyyyy") + "/" + "smms_agg_rating_quar"
  val smms_agg_rating_month_path = sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-1),"ddMMyyyy") + "/" + "smms_agg_rating_month"
  val smms_agg_rating_week1_path = sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-1),"ddMMyyyy") + "/" + "smms_agg_rating_week1"
  val smms_agg_rating_week_path = sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-1),"ddMMyyyy") + "/" + "smms_agg_rating_week"
  // val smms_agg_rating_parq_path = sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-1),"ddMMyyyy") + "/" + "smms_agg_rating_parq"

  def RunJob():Boolean= {
    var status = true
  /*  var i:Integer = x.toInt
    do {
      if (!RunTask(sFolder, i))
      {
        status = false
        break
      }
      i -= 1
    }
    while(i>0)
    spark.close()
    status
  }

  def RunTask(stg_path: String, i: Integer):Boolean= { */

    if (spark != null)
      try {
        val sl = spark.read.option("header","true").option("delimiter", ";").csv(sl_path)
        val smms_detailed_path = sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-1),"ddMMyyyy") + "/" + "smms_detailed"
        val smms_detailed1_path = sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-1),"ddMMyyyy") + "/" + "smms_detailed1"
        val smms_detailed_parq_path = sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-1),"ddMMyyyy") + "/" + "smms_detailed_parq"
        val smms_detailed1_parq_path = sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-1),"ddMMyyyy") + "/" + "smms_detailed1_parq"
        tools.removeFolder(smms_detailed_path)
        tools.removeFolder(smms_detailed_parq_path)
        tools.removeFolder(smms_detailed1_parq_path)

        //val rq = spark.read.parquet(rq_path + tools.patternToDate(tools.addDays(P_DATE,-i),"ddMMyyyy") + ".parq")
        var m = sl.select(date_format(date_sub(current_date,1), "MM-y")).take(1)(0).toString.replace("[","").replace("]","")
        var d = sl.select(date_format(date_sub(current_date,1), "ddMMy")).take(1)(0).toString.replace("[","").replace("]","")
        val rq = spark.read.parquet(rq_path + m + "/" + d + ".parq")

        val sm = rq.filter($"seller_name".isNotNull).filter(trim($"soc")==="IPACDC_5G").join(
          sl.filter($"seller".isNotNull),trim(rq("phone"))===trim(sl("contact"))).select(
          sl("id"),
          trim(sl("seller")).as("seller"),
          sl("contact"),
          sl("group").as("seller_group"),
          sl("supervisor"),
          sl("expert"),
          sl("ch_super"),
          sl("unit_head"),
          sl("lead"),
          sl("super"),
          sl("dealer"),
          rq("request_command"),
          lit("My Beeline").as("soc"),
          rq("soc_effective_date"),
          sl("number1"),
          sl("number2"),
          sl("number3"),
          sl("number4"),
          sl("number5"),
          sl("number6"),
          sl("number7"),
          lit(0.1).as("mile")
        ).union(
          rq.filter($"subs_type"==="Новый" && trim($"soc").isin("BBCLR1_3D","CLIBS2T30","CHLSUNL") or
            (trim($"soc").isin("BBCLR1_3") && $"related_soc".isin("ID_MODROU"))).join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Роутер").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(8).as("mile"))
        ).union(
          rq.filter((trim($"soc").isin("BBCLR1_2D","CLIBS2M20") or
            (trim($"soc").isin("BBCLR1_2") && $"related_soc".isin("ID_MODROU"))) && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Модем").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(10).as("mile"))
        ).union(
          // Bundles
          rq.filter(trim($"soc").isin("BB_4ALL1") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Все за 1390").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(0.5).as("mile"))
        ).union(
          // Bundles
          rq.filter(trim($"soc").isin("BBCLR1_1") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("ТП Интернет 12 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(0.5).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BB_4ALL2") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Все за 1790").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(1.5).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BB_4ALL3") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Все за 2190").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(3).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BBCLR1_2") && $"subs_type"==="Новый" && $"related_soc".isNull).join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("ТП Интернет 20 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(3).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BB_4ALL4") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Все за 2790").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(4).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BB_4ALL5") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Все за 3990").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(5).as("mile"))
        ).union(
          rq.filter($"subs_type"==="Новый" && (trim($"soc").isin("BBCLR1_3") && $"related_soc".isNull)).join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("ТП Интернет 40 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(5).as("mile"))
        ).union(
          // Upsale
          rq.filter(trim($"soc").isin("BBCLR1_4") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("ТП Интернет 60 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(5).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("2BIPY_12") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Годовой интернет 12 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(8).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("2BIPY_20") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Годовой интернет 20 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(9).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("2BIPY_30") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Годовой интернет 30 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(10).as("mile"))
        ).
          union(
            rq.filter(trim($"soc").isin("BBLSUNL") && $"subs_type"==="Новый").join(
              sl,trim(rq("phone"))===trim(sl("contact")))
              .select(
                sl("ID"),
                trim(sl("seller")).as("seller"),
                sl("contact"),
                sl("group").as("seller_group"),
                sl("supervisor"),
                sl("expert"),
                sl("ch_super"),
                sl("unit_head"),
                sl("lead"),
                sl("super"),
                sl("dealer"),
                rq("request_command"),
                lit("ТП Безлимитище").as("soc"),
                rq("soc_effective_date"),
                sl("number1"),
                sl("number2"),
                sl("number3"),
                sl("number4"),
                sl("number5"),
                sl("number6"),
                sl("number7"),
                lit(6).as("mile"))).
          union(
            rq.filter(trim($"soc").isin("BBLSUNL") && $"subs_type"==="Старый").join(
              sl,trim(rq("phone"))===trim(sl("contact")))
              .select(
                sl("ID"),
                trim(sl("seller")).as("seller"),
                sl("contact"),
                sl("group").as("seller_group"),
                sl("supervisor"),
                sl("expert"),
                sl("ch_super"),
                sl("unit_head"),
                sl("lead"),
                sl("super"),
                sl("dealer"),
                rq("request_command"),
                lit("ТП Безлимитище").as("soc"),
                rq("soc_effective_date"),
                sl("number1"),
                sl("number2"),
                sl("number3"),
                sl("number4"),
                sl("number5"),
                sl("number6"),
                sl("number7"),
                lit(3).as("mile")))
          .union(
            rq.filter(trim($"soc").isin("BB_4ALL1") && $"subs_type"==="Старый").join(
              sl,trim(rq("phone"))===trim(sl("contact"))).select(
              sl("id"),
              trim(sl("seller")).as("seller"),
              sl("contact"),
              sl("group").as("seller_group"),
              sl("supervisor"),
              sl("expert"),
              sl("ch_super"),
              sl("unit_head"),
              sl("lead"),
              sl("super"),
              sl("dealer"),
              rq("request_command"),
              lit("Все за 1390").as("soc"),
              rq("soc_effective_date"),
              sl("number1"),
              sl("number2"),
              sl("number3"),
              sl("number4"),
              sl("number5"),
              sl("number6"),
              sl("number7"),
              lit(0.2).as("mile"))
          ).union(
          rq.filter(trim($"soc").isin("BBCLR1_1") && $"subs_type"==="Старый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("ТП Интернет 12 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(0.2).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BB_4ALL2") && $"subs_type"==="Старый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Все за 1790").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(0.7).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BB_4ALL3") && $"subs_type"==="Старый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Все за 2190").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(1.5).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BBCLR1_2") && $"subs_type"==="Старый" && $"related_soc".isNull).join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("ТП Интернет 20 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(1.5).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BB_4ALL4") && $"subs_type"==="Старый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Все за 2790").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(2).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BB_4ALL5") && $"subs_type"==="Старый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Все за 3990").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(2.5).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BBCLR1_3") && $"subs_type"==="Старый" && $"related_soc".isNull).join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("ТП Интернет 40 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(2.5).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BBCLR1_4") && $"subs_type"==="Старый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("ТП Интернет 60 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(2.5).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("2BIPY_12") && $"subs_type"==="Старый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Годовой интернет 12 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(4).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("2BIPY_20") && $"subs_type"==="Старый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Годовой интернет 20 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(4.5).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("2BIPY_30") && $"subs_type"==="Старый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Годовой интернет 30 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(5).as("mile"))
        )
          .cache()
        // ****************
        val sm1 = rq.filter(trim($"soc").isin("BB_4ALL1") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Все за 1390").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(0.5).as("mile"))
          .union(
            rq.filter(trim($"soc").isin("BB_4ALL2") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Все за 1790").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(1.5).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BB_4ALL3") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Все за 2190").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(3).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BB_4ALL4") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Все за 2790").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(4).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BB_4ALL5") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Все за 3990").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(5).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BBCLR1_1") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("ТП Интернет 12 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(0.5).as("mile"))
        ).union(
          // Bundles
          rq.filter(trim($"soc").isin("BBCLR1_2") && $"related_soc".isNull && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("ТП Интернет 20 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(3).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("BBCLR1_3") && $"related_soc".isNull && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("ТП Интернет 40 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(5).as("mile"))
        ).union(
          // Bundles
          rq.filter(trim($"soc").isin("BBCLR1_4") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("ТП Интернет 60 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(5).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("2BIPY_12") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Годовой интернет 12 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(8).as("mile"))
        ).union(
          rq.filter(trim($"soc").isin("2BIPY_20") && $"subs_type"==="Новый").join(
            sl,trim(rq("phone"))===trim(sl("contact"))).select(
            sl("id"),
            trim(sl("seller")).as("seller"),
            sl("contact"),
            sl("group").as("seller_group"),
            sl("supervisor"),
            sl("expert"),
            sl("ch_super"),
            sl("unit_head"),
            sl("lead"),
            sl("super"),
            sl("dealer"),
            rq("request_command"),
            lit("Годовой интернет 20 ГБ").as("soc"),
            rq("soc_effective_date"),
            sl("number1"),
            sl("number2"),
            sl("number3"),
            sl("number4"),
            sl("number5"),
            sl("number6"),
            sl("number7"),
            lit(9).as("mile"))
        ).union(
            rq.filter(trim($"soc").isin("2BIPY_30") && $"subs_type"==="Новый").join(
              sl,trim(rq("phone"))===trim(sl("contact"))).select(
              sl("id"),
              trim(sl("seller")).as("seller"),
              sl("contact"),
              sl("group").as("seller_group"),
              sl("supervisor"),
              sl("expert"),
              sl("ch_super"),
              sl("unit_head"),
              sl("lead"),
              sl("super"),
              sl("dealer"),
              rq("request_command"),
              lit("Годовой интернет 30 ГБ").as("soc"),
              rq("soc_effective_date"),
              sl("number1"),
              sl("number2"),
              sl("number3"),
              sl("number4"),
              sl("number5"),
              sl("number6"),
              sl("number7"),
              lit(10).as("mile"))
          ).union(
            rq.filter(trim($"soc").isin("BBLSUNL") && $"subs_type"==="Новый").join(
              sl,trim(rq("phone"))===trim(sl("contact")))
              .select(
                sl("ID"),
                trim(sl("seller")).as("seller"),
                sl("contact"),
                sl("group").as("seller_group"),
                sl("supervisor"),
                sl("expert"),
                sl("ch_super"),
                sl("unit_head"),
                sl("lead"),
                sl("super"),
                sl("dealer"),
                rq("request_command"),
                lit("ТП Безлимитище").as("soc"),
                rq("soc_effective_date"),
                sl("number1"),
                sl("number2"),
                sl("number3"),
                sl("number4"),
                sl("number5"),
                sl("number6"),
                sl("number7"),
                lit(6).as("mile")))
          .cache()
        // ****************

        // Detailed seller info
        if (tools.checkExistFolder(sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-2),"ddMMyyyy") + "/" + "smms_detailed_parq")){
          spark.read.parquet(sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-2),"ddMMyyyy") + "/" + "smms_detailed_parq")
            .union(
              sm.select(
                lit(tools.addDays(P_DATE,-1)).as("business_date"),
                $"id",
                $"seller",
                $"contact",
                $"seller_group",
                $"request_command",
                $"soc",
                $"soc_effective_date",
                $"mile").filter($"soc".isNotNull)).orderBy($"seller",$"soc_effective_date".desc)
              .write.parquet(smms_detailed_parq_path)

            spark.read.parquet(smms_detailed_parq_path)
                .select(
                  $"seller",
                  $"contact",
                  $"seller_group",
                  $"request_command",
                  $"soc",
                  $"soc_effective_date",
                  $"mile"
                ).repartition(1)
            .write.format("csv")
            .option("header", "true").option("delimiter",";").option("encoding", "UTF-8")
            .save(smms_detailed_path)
        }
        else {
          sm.select(
            lit(tools.addDays(P_DATE,-1)).as("business_date"),
            $"id",
            $"seller",
            $"contact",
            $"seller_group",
            $"request_command",
            $"soc",
            $"soc_effective_date",
            $"mile").filter($"soc".isNotNull).orderBy($"seller",$"soc_effective_date".desc)
            .write.parquet(smms_detailed_parq_path)

          spark.read.parquet(smms_detailed_parq_path)
                .select(
                  $"seller",
                  $"contact",
                  $"seller_group",
                  $"request_command",
                  $"soc",
                  $"soc_effective_date",
                  $"mile"
                ).repartition(1)
            .write.format("csv")
            .option("header", "true").option("delimiter",";").option("encoding", "UTF-8")
            .save(smms_detailed_path)
        }

        if (tools.checkExistFolder(sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-2),"ddMMyyyy") + "/" + "smms_detailed1_parq")){
          spark.read.parquet(sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-2),"ddMMyyyy") + "/" + "smms_detailed1_parq")
            .union(
              sm1.select(
                lit(tools.addDays(P_DATE,-1)).as("business_date"),
                $"id",
                $"seller",
                $"contact",
                $"seller_group",
                $"request_command",
                $"soc",
                $"soc_effective_date",
                $"mile").filter($"soc".isNotNull)).orderBy($"seller",$"soc_effective_date".desc)
            .write.parquet(smms_detailed1_parq_path)
        }
        else {
          sm1.select(
            lit(tools.addDays(P_DATE,-1)).as("business_date"),
            $"id",
            $"seller",
            $"contact",
            $"seller_group",
            $"request_command",
            $"soc",
            $"soc_effective_date",
            $"mile").filter($"soc".isNotNull).orderBy($"seller",$"soc_effective_date".desc)
            .write.parquet(smms_detailed1_parq_path)
        }

        if (tools.checkExistFolder(sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-1),"ddMMyyyy") + "/" + "smms_detailed_parq")) {

          val d = spark.read.parquet(sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE, -1), "ddMMyyyy") + "/" + "smms_detailed_parq")
              .union(
                spark.read.option("header","true").option("delimiter",";").csv("/dmp/upload/smms/extra_miles.csv")
              )

          tools.removeFolder(smms_agg_rating_path)

          // Aggregated seller & rating info
          val df = d.join(
            sl,trim(d("id"))===trim(sl("id"))).select(
            current_date().as("business_date"),
            d("id"),
            d("seller").as("seller"),
            d("seller_group").as("seller_group"),
            d("contact").as("contact"),
            d("mile").as("mile")).union(
            sl.join(
              d,trim(sl("id"))===trim(d("id")),"left_anti").select(
              current_date.as("business_date"),
              sl("id"),
              sl("seller").as("seller"),
              sl("group").as("seller_group"),
              sl("contact"),
              lit(0).as("mile")
            )).groupBy(
            $"business_date",
            $"id",
            $"seller",
            $"seller_group",
            $"contact").agg(
            round(sum($"mile"),2).as("sum_mile")).select(
            $"business_date",
            $"id",
            $"seller",
            $"seller_group",
            $"contact",
            $"sum_mile",
            when($"seller_group".isNotNull,dense_rank().over(Window.partitionBy($"seller_group").orderBy($"sum_mile".desc))).otherwise(
              dense_rank().over(Window.partitionBy($"seller_group").orderBy($"sum_mile".desc))).as("rating")
          ).filter($"seller".isNotNull)

          df.join(sl,trim(df("id"))===trim(sl("id"))).select(
            df("business_date"),
            df("id"),
            df("seller"),
            df("contact"),
            df("seller_group").as("seller_group"),
            round(df("sum_mile"),2).as("seller_miles_head_ranks"),
            df("rating").as("curr_rating"),
            concat(coalesce(sl("number1"),lit("")),lit(","),
              coalesce(sl("number2"),lit("")),lit(","),
              coalesce(sl("number3"),lit("")),lit(","),
              coalesce(sl("number4"),lit("")),lit(","),
              coalesce(sl("number5"),lit("")),lit(","),
              coalesce(sl("number6"),lit("")),lit(","),
              coalesce(sl("number7"),lit("")),lit(",")
            ).as("heads_phones"),
            lit("seller").as("position")
          ).union(
            df.distinct.join(sl,trim(df("id"))===trim(sl("id"))).groupBy(
              df("business_date"),
              sl("supervisor"),
              sl("number1")).agg(
              countDistinct(df("seller")).as("sel_cnt"),
              sum(df("rating")).as("rat")).select(
              df("business_date"),
              sl("supervisor"),
              sl("number1"),
              $"sel_cnt",$"rat",
              ($"rat"/$"sel_cnt").cast(IntegerType).as("rang")).select(
              df("business_date"),
              lit("").as("id"),
              sl("supervisor"),
              sl("number1"),
              lit("").as("seller_group"),
              $"rang".as("seller_miles_head_ranks"),
              dense_rank().over(Window.orderBy($"rang")).as("curr_rating"),
              lit("").as("heads_phones"),
              lit("monobrand supervisor").as("position")
            )).union(
            df.join(sl.filter($"expert".isNotNull),df("id")===sl("id")).groupBy(
              df("business_date"),
              sl("expert"),
              sl("number2")).agg(
              count(df("seller")).as("sel_cnt"),
              sum(df("rating")).as("rat")).select(
              df("business_date"),
              sl("expert"),
              sl("number2"),
              $"sel_cnt",$"rat",
              ($"rat"/$"sel_cnt").cast(IntegerType).as("rang")).select(
              df("business_date"),
              lit("").as("id"),
              sl("expert"),
              sl("number2"),
              lit("").as("seller_group"),
              $"rang".as("seller_miles_head_ranks"),
              dense_rank().over(Window.orderBy($"rang")).as("curr_rating"),
              lit("").as("heads_phones"),
              lit("expert").as("position")
            )).union(
            df.join(sl.filter($"ch_super".isNotNull),df("id")===sl("id")).groupBy(
              df("business_date"),
              sl("ch_super"),
              sl("number3")).agg(
              count(df("seller")).as("sel_cnt"),
              sum(df("rating")).as("rat")).select(
              df("business_date"),
              sl("ch_super"),
              sl("number3"),
              $"sel_cnt",$"rat",
              ($"rat"/$"sel_cnt").cast(IntegerType).as("rang")).select(
              df("business_date"),
              lit("").as("id"),
              sl("ch_super"),
              sl("number3"),
              lit("").as("seller_group"),
              $"rang".as("seller_miles_head_ranks"),
              dense_rank().over(Window.orderBy($"rang")).as("curr_rating"),
              lit("").as("heads_phones"),
              lit("channel supervisor").as("position")
            )).union(
            df.join(sl.filter($"unit_head".isNotNull),df("id")===sl("id")).groupBy(
              df("business_date"),
              sl("unit_head"),
              sl("number4")).agg(
              count(df("seller")).as("sel_cnt"),
              sum(df("rating")).as("rat")).select(
              df("business_date"),
              sl("unit_head"),
              sl("number4"),
              $"sel_cnt",$"rat",
              ($"rat"/$"sel_cnt").cast(IntegerType).as("rang")).select(
              df("business_date"),
              lit("").as("id"),
              sl("unit_head"),
              sl("number4"),
              lit("").as("seller_group"),
              $"rang".as("seller_miles_head_ranks"),
              dense_rank().over(Window.orderBy($"rang")).as("curr_rating"),
              lit("").as("heads_phones"),
              lit("unit head").as("position")
            )).union(
            df.join(sl.filter($"lead".isNotNull),df("id")===sl("id")).groupBy(
              df("business_date"),
              sl("lead"),
              sl("number5")).agg(
              count(df("seller")).as("sel_cnt"),
              sum(df("rating")).as("rat")).select(
              df("business_date"),
              sl("lead"),
              sl("number5"),
              $"sel_cnt",$"rat",
              ($"rat"/$"sel_cnt").cast(IntegerType).as("rang")).select(
              df("business_date"),
              lit("").as("id"),
              sl("lead"),
              sl("number5"),
              lit("").as("seller_group"),
              $"rang".as("seller_miles_head_ranks"),
              dense_rank().over(Window.orderBy($"rang")).as("curr_rating"),
              lit("").as("heads_phones"),
              lit("lead").as("position")
            )).union(
            df.join(sl.filter($"super".isNotNull),df("id")===sl("id")).groupBy(
              df("business_date"),
              sl("super"),
              sl("number6")).agg(
              count(df("seller")).as("sel_cnt"),
              sum(df("rating")).as("rat")).select(
              df("business_date"),
              sl("super"),
              sl("number6"),
              $"sel_cnt",$"rat",
              ($"rat"/$"sel_cnt").cast(IntegerType).as("rang")).select(
              df("business_date"),
              lit("").as("id"),
              sl("super"),
              sl("number6"),
              lit("").as("seller_group"),
              $"rang".as("seller_miles_head_ranks"),
              dense_rank().over(Window.orderBy($"rang")).as("curr_rating"),
              lit("").as("heads_phones"),
              lit("supervisor").as("position")
            )).union(
            df.join(sl.filter($"dealer".isNotNull),df("id")===sl("id")).groupBy(
              df("business_date"),
              sl("dealer"),
              sl("number7")).agg(
              count(df("seller")).as("sel_cnt"),
              sum(df("rating")).as("rat")).select(
              df("business_date"),
              sl("dealer"),
              sl("number7"),
              $"sel_cnt",$"rat",
              ($"rat"/$"sel_cnt").cast(IntegerType).as("rang")).select(
              df("business_date"),
              lit("").as("id"),
              sl("dealer"),
              sl("number7"),
              lit("").as("seller_group"),
              $"rang".as("seller_miles_head_ranks"),
              dense_rank().over(Window.orderBy($"rang")).as("curr_rating"),
              lit("").as("heads_phones"),
              lit("dealer").as("position")
            ))
            .select(
              $"business_date",
              //$"id",
              $"seller",
              $"contact",
              $"seller_group",
              $"seller_miles_head_ranks",
              $"curr_rating",
              $"heads_phones",
              $"position"
            ).distinct.repartition(1)
            .write.format("csv")
            .option("header", "true").option("delimiter", ";").option("encoding", "UTF-8")
            .save(smms_agg_rating_path)

        }
        println("Finished yearly agg!")
        spark.close()
  }
      catch {
        case e: Exception => println(s"ERROR: $e")
          status = false
          if (spark != null) spark.close()
      } finally { }
    else status = false
    status
  }
}