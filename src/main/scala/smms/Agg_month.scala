package smms

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utl.SparkBase
import utl.Tools

import scala.util.control.Breaks.break

class Agg_month(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val sFolder = propMap("sfolder")   // Stagings folder
  val dsFolder = propMap("dsfolder")
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._

  val sl_path = "/dmp/upload/smms/sellers.csv"
  val smms_agg_rating_month_path = sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-1),"ddMMyyyy") + "/" + "smms_agg_rating_month"
  var status = true

  def RunJob():Boolean= {

    if (spark != null)
      try {
        if (tools.checkExistFolder(smms_agg_rating_month_path)){
          tools.removeFolder(smms_agg_rating_month_path)
        }

        val sl = spark.read.option("header","true").option("delimiter", ";").csv(sl_path)
        val d = spark.read.parquet(sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE, -1), "ddMMyyyy") + "/" + "smms_detailed_parq")
          .union(
            spark.read.option("header","true").option("delimiter",";").csv("/dmp/upload/smms/extra_miles.csv")
          )
          // Aggregated seller & rating info
          if (P_DATE.substring(8,10) == "01") {
            val df = d.filter($"business_date".cast(org.apache.spark.sql.types.DateType) >= tools.addMonths(P_DATE,-1) &&
            $"business_date".cast(org.apache.spark.sql.types.DateType) < P_DATE).join(
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
          ).select(
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
            .save(smms_agg_rating_month_path)
          }
          else {
            val df = d.filter($"business_date".cast(org.apache.spark.sql.types.DateType) >= tools.firstMonthDay(P_DATE, "yyyy-MM-dd") &&
              $"business_date".cast(org.apache.spark.sql.types.DateType) < P_DATE).join(
              sl, trim(d("id")) === trim(sl("id"))).select(
              current_date().as("business_date"),
              d("id"),
              d("seller").as("seller"),
              d("seller_group").as("seller_group"),
              d("contact").as("contact"),
              d("mile").as("mile")).union(
              sl.join(
                d, trim(sl("id")) === trim(d("id")), "left_anti").select(
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
              round(sum($"mile"), 2).as("sum_mile")).select(
              $"business_date",
              $"id",
              $"seller",
              $"seller_group",
              $"contact",
              $"sum_mile",
              when($"seller_group".isNotNull, dense_rank().over(Window.partitionBy($"seller_group").orderBy($"sum_mile".desc))).otherwise(
                dense_rank().over(Window.partitionBy($"seller_group").orderBy($"sum_mile".desc))).as("rating")
            ).filter($"seller".isNotNull)

            df.join(sl, trim(df("id")) === trim(sl("id"))).select(
              df("business_date"),
              df("id"),
              df("seller"),
              df("contact"),
              df("seller_group").as("seller_group"),
              round(df("sum_mile"), 2).as("seller_miles_head_ranks"),
              df("rating").as("curr_rating"),
              concat(coalesce(sl("number1"), lit("")), lit(","),
                coalesce(sl("number2"), lit("")), lit(","),
                coalesce(sl("number3"), lit("")), lit(","),
                coalesce(sl("number4"), lit("")), lit(","),
                coalesce(sl("number5"), lit("")), lit(","),
                coalesce(sl("number6"), lit("")), lit(","),
                coalesce(sl("number7"), lit("")), lit(",")
              ).as("heads_phones"),
              lit("seller").as("position")
            ).select(
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
              .save(smms_agg_rating_month_path)
          }
      }
      catch {
        case e: Exception => println(s"ERROR: $e")
          status = false
          if (spark != null) spark.close()
      } finally { }
    status
  }
}