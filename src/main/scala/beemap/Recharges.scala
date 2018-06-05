package beemap

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utl.SparkBase
import utl.Tools

import scala.util.control.Breaks.break

class Recharges(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val calc_date = propMap("calc_date")
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName// + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  var status = true
  import spark.implicits._

  def RunJob():Boolean= {
    if (spark != null)
      try {

        val dim_rech = spark.read.format("csv").option("header","true").option("delimiter","\t").load("/dmp/beemap/dict/dim_recharge_classification.csv")
        dim_rech.createOrReplaceTempView("dim_rech")

        spark.sql(s"""
        select *
        from dim_rech
        where effective_date < '${tools.patternToDate(calc_date,"yyyy")}-${tools.patternToDate(calc_date,"MM")}-${tools.patternToDate(calc_date,"dd")}'
        and '${tools.patternToDate(calc_date,"yyyy")}-${tools.patternToDate(calc_date,"MM")}-${tools.patternToDate(calc_date,"dd")}' < nvl(expiration_date, '2999-12-31')
        """).createOrReplaceTempView("dim_rech")

        val recharges_cell = spark.sql(s"""
    select
        r.fc_lac+0 lac,
        r.fc_cell_id+0 cell,
        sum(case when d.recharge_category = 'Bank' then r.recharge_amt end) bank_sum,
        count(case when d.recharge_category = 'Bank' then 1 end) bank_count,
        sum(case when d.recharge_category = 'Scratch' then r.recharge_amt end) scratch_sum,
        count(case when d.recharge_category = 'Scratch' then 1 end) scratch_count,
        sum(case when d.recharge_category = 'PaymentSystem' then r.recharge_amt end) payment_system_sum,
        count(case when d.recharge_category = 'PaymentSystem' then 1 end) payment_system_count,
        sum(case when d.recharge_category = 'Beeline' then r.recharge_amt end) beeline_sum,
        count(case when d.recharge_category = 'Beeline' then 1 end) beeline_count,
        sum(case when d.recharge_category is null then r.recharge_amt end) other_sum,
        count(case when d.recharge_category is null then 1 end) other_count
    from
    accrech.bi_accrech r
    left join dim_rech d
    on trim(d.mtr_description) = trim(r.description)
    where r.vt_date = '${tools.patternToDate(calc_date,"yyyy")}-${tools.patternToDate(calc_date,"MM")}-${tools.patternToDate(calc_date,"dd")}'
    group by
        r.fc_lac+0,
        r.fc_cell_id+0
""")

        recharges_cell.createOrReplaceTempView("recharges_cell")

        spark.read.parquet("/dmp/beemap/temp/dim_map_cell/"+ tools.patternToDate(calc_date,"yyyyMM")).createOrReplaceTempView("dim_map_cell")

        val recharges_sites = spark.sql("""
        select
            d.site_id,
            sum(r.bank_sum) bank_sum,
            sum(r.bank_count) bank_count,
            sum(r.scratch_sum) scratch_sum,
            sum(r.scratch_count) scratch_count,
            sum(r.payment_system_sum) payment_system_sum,
            sum(r.payment_system_count) payment_system_count,
            sum(r.beeline_sum) beeline_sum,
            sum(r.beeline_count) beeline_count,
            sum(r.other_sum) other_sum,
            sum(r.other_count) other_count
        from recharges_cell r
        join dim_map_cell d
          on r.lac = d.lac
         and r.cell = d.cell
        group by d.site_id
        """)

        recharges_sites.write.mode("overwrite").parquet("/dmp/beemap/temp/recharges/" + tools.patternToDate(P_DATE,"yyyyMM"))

        spark.close()
      } catch {
        case e: Exception => println(s"ERROR: $e")
          status = false
          if (spark != null) spark.close()
      } finally {}
    status
  }
}