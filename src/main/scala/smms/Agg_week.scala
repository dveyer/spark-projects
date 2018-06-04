package smms

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utl.SparkBase
import utl.Tools

import scala.util.control.Breaks.break

class Agg_week(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val sFolder = propMap("sfolder")   // Stagings folder
  val dsFolder = propMap("dsfolder")
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  import spark.implicits._

  val sl_path = "/dmp/upload/smms/sellers.csv"
  //val smms_agg_rating_week_path = sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-1),"ddMMyyyy") + "/" + "smms_agg_rating_week"
  val smms_agg_rating_week_path = sFolder + "/" + tools.patternToDate(tools.addDays(P_DATE,-1),"ddMMyyyy") + "/" + "smms_agg_rating_week"
  var v_date = "2018-05-07"
  var e_date = "2018-05-21"
  val sl = spark.read.option("header", "true").option("delimiter", ";").csv(sl_path)
  var status = true

  def RunJob():Boolean= {

    if (spark != null)
      try {
        //if (P_DATE > e_date){
          //tools.removeFolder(smms_agg_rating_week_path)
          //System.exit(0)
        //}
        val d = spark.read.parquet(sFolder + "/21052018/" + "smms_detailed1_parq")
        .union(
          spark.read.option("header","true").option("delimiter",";").csv("/dmp/upload/smms/extra_miles.csv")
        )

        val df = d
          .filter($"business_date".cast(org.apache.spark.sql.types.DateType) >= v_date &&
              $"business_date".cast(org.apache.spark.sql.types.DateType) <= e_date)
            .join(
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
          ).union(
            df.distinct.join(sl, trim(df("id")) === trim(sl("id"))).groupBy(
              df("business_date"),
              sl("supervisor"),
              sl("number1")).agg(
              countDistinct(df("seller")).as("sel_cnt"),
              sum(df("rating")).as("rat")).select(
              df("business_date"),
              sl("supervisor"),
              sl("number1"),
              $"sel_cnt", $"rat",
              ($"rat" / $"sel_cnt").cast(IntegerType).as("rang")).select(
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
            df.join(sl.filter($"expert".isNotNull), df("id") === sl("id")).groupBy(
              df("business_date"),
              sl("expert"),
              sl("number2")).agg(
              count(df("seller")).as("sel_cnt"),
              sum(df("rating")).as("rat")).select(
              df("business_date"),
              sl("expert"),
              sl("number2"),
              $"sel_cnt", $"rat",
              ($"rat" / $"sel_cnt").cast(IntegerType).as("rang")).select(
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
            df.join(sl.filter($"ch_super".isNotNull), df("id") === sl("id")).groupBy(
              df("business_date"),
              sl("ch_super"),
              sl("number3")).agg(
              count(df("seller")).as("sel_cnt"),
              sum(df("rating")).as("rat")).select(
              df("business_date"),
              sl("ch_super"),
              sl("number3"),
              $"sel_cnt", $"rat",
              ($"rat" / $"sel_cnt").cast(IntegerType).as("rang")).select(
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
            df.join(sl.filter($"unit_head".isNotNull), df("id") === sl("id")).groupBy(
              df("business_date"),
              sl("unit_head"),
              sl("number4")).agg(
              count(df("seller")).as("sel_cnt"),
              sum(df("rating")).as("rat")).select(
              df("business_date"),
              sl("unit_head"),
              sl("number4"),
              $"sel_cnt", $"rat",
              ($"rat" / $"sel_cnt").cast(IntegerType).as("rang")).select(
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
            df.join(sl.filter($"lead".isNotNull), df("id") === sl("id")).groupBy(
              df("business_date"),
              sl("lead"),
              sl("number5")).agg(
              count(df("seller")).as("sel_cnt"),
              sum(df("rating")).as("rat")).select(
              df("business_date"),
              sl("lead"),
              sl("number5"),
              $"sel_cnt", $"rat",
              ($"rat" / $"sel_cnt").cast(IntegerType).as("rang")).select(
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
            df.join(sl.filter($"super".isNotNull), df("id") === sl("id")).groupBy(
              df("business_date"),
              sl("super"),
              sl("number6")).agg(
              count(df("seller")).as("sel_cnt"),
              sum(df("rating")).as("rat")).select(
              df("business_date"),
              sl("super"),
              sl("number6"),
              $"sel_cnt", $"rat",
              ($"rat" / $"sel_cnt").cast(IntegerType).as("rang")).select(
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
            df.join(sl.filter($"dealer".isNotNull), df("id") === sl("id")).groupBy(
              df("business_date"),
              sl("dealer"),
              sl("number7")).agg(
              count(df("seller")).as("sel_cnt"),
              sum(df("rating")).as("rat")).select(
              df("business_date"),
              sl("dealer"),
              sl("number7"),
              $"sel_cnt", $"rat",
              ($"rat" / $"sel_cnt").cast(IntegerType).as("rang")).select(
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
            .save(smms_agg_rating_week_path)

        spark.close()
      }
      catch {
        case e: Exception => println(s"ERROR: $e")
          status = false
          if (spark != null) spark.close()
      } finally { }
    status
  }
}