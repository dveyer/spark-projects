package beemap

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utl.SparkBase
import utl.Tools

import scala.util.control.Breaks.break

class Tech_KPI(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

  // Initialization ****************************
  val lcprep_path = propMap("lcprep_path")
  val lcpost_path = propMap("lcpost_path")
  val dmcell_path = propMap("dmcell_path")
  val outroam_path = propMap("outroam_path")
  val outroams_path = propMap("outroams_path")
  val map_kpi_path = propMap("map_kpi_path")
  val kpi_stat_path = propMap("kpi_stat_path")
  val kpi_final_cell_path = propMap("kpi_final_cell_path")
  val kpi_final_site_path = propMap("kpi_final_site_path")

  val calc_date = propMap("calc_date")
  val P_DATE = if (pdate.equals("sysdate")) tools.getSysDate("yyyy-MM-dd") else pdate
  val stg_name = this.getClass.getSimpleName// + "_" + tools.patternToDate(P_DATE,"MMyyyy")
  val spark = InitSpark(stg_name.toUpperCase + "_JOB")
  var status = true
  import spark.implicits._

  def RunJob():Boolean= {
    if (spark != null)
      try {

        spark.read.parquet("/dmp/upload/u2000/all_processed_counters/pmexport_"+tools.patternToDate(calc_date, "yyyyMM")+
        "*/*.parquet").createOrReplaceTempView("counters_kpi")

        val counters_df = spark.sql("""
    SELECT
    if(instr(object_name, 'eNodeB')>0, regexp_extract(object_name, 'Cell Name=(.*?),', 1), regexp_extract(object_name, '(LABEL|Label)=([^,]*)', 2)) cell_name,
    if(instr(object_name, 'eNodeB')>0, regexp_extract(object_name, 'eNodeB ID=(.*?),', 1), null) enodeb_id,
    if(instr(object_name, 'eNodeB')>0, regexp_extract(object_name, 'eNodeB Function Name=(.*?),', 1), null) enodeb_name,
    sum(case when counter_id='50331745' then value end) sum_50331745,
    sum(case when counter_id='50331746' then value end) sum_50331746,
    sum(case when counter_id='67179329' then value end) sum_67179329,
    sum(case when counter_id='67179330' then value end) sum_67179330,
    sum(case when counter_id='67179331' then value end) sum_67179331,
    sum(case when counter_id='67179332' then value end) sum_67179332,
    sum(case when counter_id='67179333' then value end) sum_67179333,
    sum(case when counter_id='67179334' then value end) sum_67179334,
    sum(case when counter_id='67179335' then value end) sum_67179335,
    sum(case when counter_id='67179336' then value end) sum_67179336,
    sum(case when counter_id='67179337' then value end) sum_67179337,
    sum(case when counter_id='67179338' then value end) sum_67179338,
    sum(case when counter_id='67179457' then value end) sum_67179457,
    sum(case when counter_id='67179458' then value end) sum_67179458,
    sum(case when counter_id='67179459' then value end) sum_67179459,
    sum(case when counter_id='67179460' then value end) sum_67179460,
    sum(case when counter_id='67179461' then value end) sum_67179461,
    sum(case when counter_id='67179462' then value end) sum_67179462,
    sum(case when counter_id='67179463' then value end) sum_67179463,
    sum(case when counter_id='67179464' then value end) sum_67179464,
    sum(case when counter_id='67179465' then value end) sum_67179465,
    sum(case when counter_id='67179466' then value end) sum_67179466,
    sum(case when counter_id='67179524' then value end) sum_67179524,
    sum(case when counter_id='67179778' then value end) sum_67179778,
    sum(case when counter_id='67179779' then value end) sum_67179779,
    sum(case when counter_id='67179781' then value end) sum_67179781,
    sum(case when counter_id='67179782' then value end) sum_67179782,
    sum(case when counter_id='67179825' then value end) sum_67179825,
    sum(case when counter_id='67179826' then value end) sum_67179826,
    sum(case when counter_id='67179827' then value end) sum_67179827,
    sum(case when counter_id='67179828' then value end) sum_67179828,
    sum(case when counter_id='67179864' then value end) sum_67179864,
    sum(case when counter_id='67179921' then value end) sum_67179921,
    sum(case when counter_id='67179922' then value end) sum_67179922,
    sum(case when counter_id='67179923' then value end) sum_67179923,
    sum(case when counter_id='67179924' then value end) sum_67179924,
    sum(case when counter_id='67179925' then value end) sum_67179925,
    sum(case when counter_id='67179926' then value end) sum_67179926,
    sum(case when counter_id='67179927' then value end) sum_67179927,
    sum(case when counter_id='67179928' then value end) sum_67179928,
    sum(case when counter_id='67179967' then value end) sum_67179967,
    sum(case when counter_id='67189730' then value end) sum_67189730,
    sum(case when counter_id='67190404' then value end) sum_67190404,
    sum(case when counter_id='67190405' then value end) sum_67190405,
    sum(case when counter_id='67190406' then value end) sum_67190406,
    sum(case when counter_id='67190407' then value end) sum_67190407,
    sum(case when counter_id='67190408' then value end) sum_67190408,
    sum(case when counter_id='67190409' then value end) sum_67190409,
    sum(case when counter_id='67190586' then value end) sum_67190586,
    sum(case when counter_id='67192584' then value end) sum_67192584,
    sum(case when counter_id='67193609' then value end) sum_67193609,
    sum(case when counter_id='67193610' then value end) sum_67193610,
    sum(case when counter_id='67193611' then value end) sum_67193611,
    sum(case when counter_id='67193612' then value end) sum_67193612,
    sum(case when counter_id='67193613' then value end) sum_67193613,
    sum(case when counter_id='67193614' then value end) sum_67193614,
    sum(case when counter_id='73421766' then value end) sum_73421766,
    sum(case when counter_id='73421882' then value end) sum_73421882,
    sum(case when counter_id='73421883' then value end) sum_73421883,
    sum(case when counter_id='73421886' then value end) sum_73421886,
    sum(case when counter_id='73422166' then value end) sum_73422166,
    sum(case when counter_id='1278072498' then value end) sum_1278072498,
    sum(case when counter_id='1278072520' then value end) sum_1278072520,
    sum(case when counter_id='1278087421' then value end) sum_1278087421,
    sum(case when counter_id='1278082418' then value end) sum_1278082418,
    sum(case when counter_id='1278082423' then value end) sum_1278082423,
    sum(case when counter_id='1278082424' then value end) sum_1278082424,
    sum(case when counter_id='1278087427' then value end) sum_1278087427,
    sum(case when counter_id='1278087430' then value end) sum_1278087430,
    sum(case when counter_id='1278087432' then value end) sum_1278087432,
    sum(case when counter_id='1282448074' then value end) sum_1282448074,
    sum(case when counter_id='1282448076' then value end) sum_1282448076,
    sum(case when counter_id='1282448077' then value end) sum_1282448077,
    sum(case when counter_id='1282448079' then value end) sum_1282448079,
    sum(case when counter_id='1282448080' then value end) sum_1282448080,
    sum(case when counter_id='1282448082' then value end) sum_1282448082,
    sum(case when counter_id='1282448083' then value end) sum_1282448083,
    sum(case when counter_id='1282448085' then value end) sum_1282448085,
    sum(case when counter_id='1526726658' then value end) sum_1526726658,
    sum(case when counter_id='1526726659' then value end) sum_1526726659,
    sum(case when counter_id='1526726740' then value end) sum_1526726740,
    sum(case when counter_id='1526727544' then value end) sum_1526727544,
    sum(case when counter_id='1526727545' then value end) sum_1526727545,
    sum(case when counter_id='1526727546' then value end) sum_1526727546,
    sum(case when counter_id='1526727547' then value end) sum_1526727547,
    sum(case when counter_id='1526728261' then value end) sum_1526728261,
    sum(case when counter_id='1526728272' then value end) sum_1526728272,
    sum(case when counter_id='1526728273' then value end) sum_1526728273,
    sum(case when counter_id='1526728433' then value end) sum_1526728433,
    sum(case when counter_id='1526729005' then value end) sum_1526729005,
    sum(case when counter_id='1526729015' then value end) sum_1526729015,
    sum(case when counter_id='1278087438' then value end) sum_1278087438,
    sum(case when counter_id='67199781' then value end) sum_67199781,
    sum(case when counter_id='67199632' then value end) sum_67199632,
    sum(case when counter_id='67199630' then value end) sum_67199630,
    sum(case when counter_id='67199631' then value end) sum_67199631,
    sum(case when counter_id='67199628' then value end) sum_67199628,
    sum(case when counter_id='67199629' then value end) sum_67199629,
    sum(case when counter_id='67199626' then value end) sum_67199626,
    sum(case when counter_id='67199627' then value end) sum_67199627,
    sum(case when counter_id='67203812' then value end) sum_67203812,
    sum(case when counter_id='67203816' then value end) sum_67203816,
    sum(case when counter_id='67203818' then value end) sum_67203818,
    sum(case when counter_id='67203819' then value end) sum_67203819,
    sum(case when counter_id='67203820' then value end) sum_67203820,
    sum(case when counter_id='67199622' then value end) sum_67199622,
    sum(case when counter_id='67199780' then value end) sum_67199780,
    sum(case when counter_id='67199623' then value end) sum_67199623,
    sum(case when counter_id='67199621' then value end) sum_67199621,
    sum(case when counter_id='67199624' then value end) sum_67199624,
    sum(case when counter_id='67199625' then value end) sum_67199625,
    sum(case when counter_id='67199620' then value end) sum_67199620,
    sum(case when counter_id='67199619' then value end) sum_67199619,
    sum(case when counter_id='67203803' then value end) sum_67203803,
    sum(case when counter_id='67203807' then value end) sum_67203807,
    sum(case when counter_id='67203809' then value end) sum_67203809,
    sum(case when counter_id='67203810' then value end) sum_67203810,
    sum(case when counter_id='67203811' then value end) sum_67203811,
    sum(case when counter_id='67192486' then value end) sum_67192486,
    sum(case when counter_id='67189840' then value end) sum_67189840,
    sum(case when counter_id='67184032' then value end) sum_67184032,
    sum(case when counter_id='67184031' then value end) sum_67184031,
    sum(case when counter_id='67184030' then value end) sum_67184030,
    sum(case when counter_id='67184029' then value end) sum_67184029,
    sum(case when counter_id='67184028' then value end) sum_67184028,
    sum(case when counter_id='67184027' then value end) sum_67184027,
    sum(case when counter_id='67184026' then value end) sum_67184026,
    sum(case when counter_id='67184025' then value end) sum_67184025,
    sum(case when counter_id='67184024' then value end) sum_67184024,
    sum(case when counter_id='67184023' then value end) sum_67184023,
    sum(case when counter_id='67184022' then value end) sum_67184022,
    sum(case when counter_id='67184021' then value end) sum_67184021,
    sum(case when counter_id='67184020' then value end) sum_67184020,
    sum(case when counter_id='67184019' then value end) sum_67184019,
    sum(case when counter_id='67184018' then value end) sum_67184018,
    sum(case when counter_id='67184017' then value end) sum_67184017,
    sum(case when counter_id='67184016' then value end) sum_67184016,
    sum(case when counter_id='67184015' then value end) sum_67184015,
    sum(case when counter_id='67184014' then value end) sum_67184014,
    sum(case when counter_id='67184011' then value end) sum_67184011,
    sum(case when counter_id='67184010' then value end) sum_67184010,
    sum(case when counter_id='67184009' then value end) sum_67184009,
    sum(case when counter_id='67184012' then value end) sum_67184012,
    sum(case when counter_id='67184006' then value end) sum_67184006,
    sum(case when counter_id='67184008' then value end) sum_67184008,
    sum(case when counter_id='67184007' then value end) sum_67184007,
    sum(case when counter_id='67184005' then value end) sum_67184005,
    sum(case when counter_id='67184004' then value end) sum_67184004,
    sum(case when counter_id='67184003' then value end) sum_67184003,
    sum(case when counter_id='67184002' then value end) sum_67184002,
    sum(case when counter_id='67184001' then value end) sum_67184001,
    sum(case when counter_id='67184000' then value end) sum_67184000,
    sum(case when counter_id='67183999' then value end) sum_67183999,
    sum(case when counter_id='67183998' then value end) sum_67183998,
    sum(case when counter_id='67183997' then value end) sum_67183997,
    sum(case when counter_id='67183996' then value end) sum_67183996,
    sum(case when counter_id='67183995' then value end) sum_67183995,
    sum(case when counter_id='67183994' then value end) sum_67183994,
    sum(case when counter_id='67183993' then value end) sum_67183993,
    sum(case when counter_id='1526728259' then value end) sum_1526728259,
    sum(case when counter_id='1279184419' then value end) sum_1279184419,
    sum(case when counter_id='1279183419' then value end) sum_1279183419
    FROM counters_kpi
    GROUP BY
        if(instr(object_name, 'eNodeB')>0, regexp_extract(object_name, 'Cell Name=(.*?),', 1), regexp_extract(object_name, '(LABEL|Label)=([^,]*)', 2)),
        if(instr(object_name, 'eNodeB')>0, regexp_extract(object_name, 'eNodeB ID=(.*?),', 1), null),
        if(instr(object_name, 'eNodeB')>0, regexp_extract(object_name, 'eNodeB Function Name=(.*?),', 1), null)
    """).cache

        counters_df.write.mode("overwrite").parquet("/dmp/beemap/temp/counters/"+tools.patternToDate(calc_date,"yyyyMM")) // remove temp

        val df = spark.read.parquet("/dmp/beemap/temp/counters/"+tools.patternToDate(calc_date,"yyyyMM"))
        df.createOrReplaceTempView("counters")

        val kpi = spark.sql("""
    select
    cell_name,
    enodeb_id,
    enodeb_name,
    cdr_cs_2g,
    CunSSR_CS_2G,
    CDR_CS_3G,
    rab_dr_ps_3g,
    cunssr_cs_3g,
    CunSSR_PS_3G,
    User_Throughput_3G,
    greatest(
        vk_3g_cs_rab_setup_fr_cong_power_dl_r13,
        vk_3g_cs_rab_setup_fr_cong_power_ul_r13,
        vk_3g_ps_rab_setup_fr_cong_power_dl_r13,
        vk_3g_ps_rab_setup_fr_cong_power_ul_r13,
        vk_3g_rrc_csetup_rej_cong_power_dl_r13,
        vk_3g_rrc_csetup_rej_cong_power_ul_r13
    ) cong_power_ul_dl_3g,
    greatest(
        VK_3G_CS_RAB_Setup_FR_CONG_DL_CE_R13,
        VK_3G_CS_RAB_Setup_FR_CONG_UL_CE_R13,
        VK_3G_PS_RAB_Setup_FR_Cong_DL_CE_R13,
        VK_3G_PS_RAB_Setup_FR_CONG_UL_CE_R13,
        VK_3G_RRC_CSetup_Rej_CONG_DL_CE_R13,
        VK_3G_RRC_CSetup_Rej_CONG_UL_CE_R13
    ) CONG_CE_UL_DL_3G,
    greatest(
        VK_3G_CS_RAB_Setup_FR_CONG_CODE_R13,
        VK_3G_PS_RAB_Setup_FR_CONG_CODE_R13,
        VK_3G_RRC_CSetup_Rej_CONG_CODE_R13
    ) CONG_CODE_3G,
    CunSSR_PS_4G,
    util_prb_dl_4g,
    RAB_DR_4G,
    User_Throughput_4G,
    voice_traf_2g,
    voice_traf_3g,
    kpi_data_traf_gb_2g,
    kpi_data_traf_gb_3g,
    kpi_data_traf_gb_4g
from (
    select
        cell_name,
        enodeb_id,
        enodeb_name,
        (100 * sum_1278072498) / (sum_1278087432 + sum_1278087427 - sum_1282448080 + sum_1282448082 - sum_1282448083 + sum_1282448085 + sum_1282448074 - sum_1282448076 + sum_1282448077 - sum_1282448079 + sum_1278082418 - sum_1278082423 - sum_1278082424) cdr_cs_2g,
        100 * (sum_67179781 - sum_73421883 - sum_73422166 - sum_73421886) / (sum_67179781 + sum_67179782 - sum_73421883 - sum_73421882 + sum_73421766 + sum_67192584) rab_dr_ps_3g,
        100 * (1 - ( (sum_67179457 + sum_67179462 + sum_67179466) / (sum_67179329 + sum_67179334 + sum_67179338) ) * (  (sum_67179827 + sum_67179828) / (sum_67179825 + sum_67179826 - sum_67189730) ) ) cunssr_cs_3g,
        sum_1526726740/sum_1526728433 util_prb_dl_4g,
        (sum_67190407 / (sum_67179825 + sum_67179826)) * 100 VK_3G_CS_RAB_Setup_FR_CONG_DL_CE_R13,
        (sum_67190406 / (sum_67179825 + sum_67179826))*100 VK_3G_CS_RAB_Setup_FR_CONG_UL_CE_R13,
        (sum_67190409 / (sum_67179924 + sum_67179921 + sum_67179923 + sum_67179922)) * 100 VK_3G_PS_RAB_Setup_FR_Cong_DL_CE_R13,
        (sum_67190408 / (sum_67179924 + sum_67179921 + sum_67179923 + sum_67179922)) * 100 VK_3G_PS_RAB_Setup_FR_CONG_UL_CE_R13,
        (sum_67190405 / sum_67190586) * 100 VK_3G_RRC_CSetup_Rej_CONG_DL_CE_R13,
        (sum_67190404 / sum_67190586) * 100 VK_3G_RRC_CSetup_Rej_CONG_UL_CE_R13,
        (sum_67193612 / (sum_67179825 + sum_67179826)) * 100 VK_3G_CS_RAB_Setup_FR_CONG_POWER_DL_R13,
        (sum_67193611 / (sum_67179825 + sum_67179826)) * 100 VK_3G_CS_RAB_Setup_FR_CONG_POWER_UL_R13,
        (sum_67193614 / (sum_67179924 + sum_67179921 + sum_67179923 + sum_67179922)) * 100 VK_3G_PS_RAB_Setup_FR_CONG_POWER_DL_R13,
        (sum_67193613 / (sum_67179924 + sum_67179921 + sum_67179923 + sum_67179922)) * 100 VK_3G_PS_RAB_Setup_FR_CONG_POWER_UL_R13,
        (sum_67193610 / sum_67190586) * 100 VK_3G_RRC_CSetup_Rej_CONG_POWER_DL_R13,
        (sum_67193609 / sum_67190586) * 100 VK_3G_RRC_CSetup_Rej_CONG_POWER_UL_R13,
        (sum_67179864 / (sum_67179825 + sum_67179826)) * 100 VK_3G_CS_RAB_Setup_FR_CONG_CODE_R13,
        (sum_67179967 / (sum_67179924 + sum_67179921 + sum_67179922 + sum_67179923)) * 100 VK_3G_PS_RAB_Setup_FR_CONG_CODE_R13,
        (sum_67179524 / sum_67190586) * 100 VK_3G_RRC_CSetup_Rej_CONG_CODE_R13,
        100 * ( 1 - (( 1 - sum_1278072520 / sum_1278087421 ) * sum_1278087432 / sum_1278087430)) CunSSR_CS_2G,
        100 * (sum_67179778 / (sum_67179779 + sum_67179778)) CDR_CS_3G,
        100 * (1 - ((sum_67179460 + sum_67179459 + sum_67179458 + sum_67179461 + sum_67179465 + sum_67179464 + sum_67179463) / (sum_67179332 + sum_67179331 + sum_67179330 + sum_67179333 + sum_67179337 + sum_67179336 + sum_67179335)) * ((sum_67179928 + sum_67179927 + sum_67179926 + sum_67179925) / (sum_67179924 + sum_67179923 + sum_67179922 + sum_67179921))) CunSSR_PS_3G,
        (sum_50331746/1000)/(sum_50331745*0.002) User_Throughput_3G,
        100 * (1 - (sum_1526727544 / sum_1526727545) * (sum_1526726659 / sum_1526726658) * (sum_1526728273 / sum_1526728272)) CunSSR_PS_4G,
        100 * sum_1526727546 / (sum_1526727547 + sum_1526727546) RAB_DR_4G,
        (sum_1526728261 - sum_1526729005) / sum_1526729015 User_Throughput_4G,
        sum_1278087438 voice_traf_2g,
        ((sum_67199781 + sum_67199632 + sum_67199630 + sum_67199631 + sum_67199628 + sum_67199629 + sum_67199626 + sum_67199627 + sum_67203812 + sum_67203816 + sum_67203818 + sum_67203819 + sum_67203820) + (sum_67199622 + sum_67199780 + sum_67199623 + sum_67199621 + sum_67199624 + sum_67199625 + sum_67199620 + sum_67199619 + sum_67203803 + sum_67203807 + sum_67203809 + sum_67203810 + sum_67203811))/2 voice_traf_3g,
        (sum_1279184419 + sum_1279183419) / 1024 / 1024 / 1024 kpi_data_traf_gb_2g,
        (((sum_67192486 + sum_67189840) / 1000000)+((sum_67184032 + sum_67184031 + sum_67184030 + sum_67184029 + sum_67184028 + sum_67184027 + sum_67184026 + sum_67184025 + sum_67184024 + sum_67184023 + sum_67184022 + sum_67184021 + sum_67184020 + sum_67184019 + sum_67184018 + sum_67184017 + sum_67184016 + sum_67184015 + sum_67184014 + sum_67184011 + sum_67184010 + sum_67184009 + sum_67184012 + sum_67184006 + sum_67184008 + sum_67184007 + sum_67184005 + sum_67184004 + sum_67184003 + sum_67184002 + sum_67184001 + sum_67184000 + sum_67183999 + sum_67183998 + sum_67183997 + sum_67183996 + sum_67183995 + sum_67183994 + sum_67183993) / 8000000))/ 1024 kpi_data_traf_gb_3g,
        (sum_1526728261 + sum_1526728259) / 8000000 / 1024 kpi_data_traf_gb_4g
     from counters
    ) a
""")

        kpi.createOrReplaceTempView("kpi")
        kpi.write.mode("overwrite").parquet("/dmp/beemap/temp/kpi/"+tools.patternToDate(calc_date,"yyyyMM"))

        spark.read.parquet("/dmp/beemap/temp/dim_map_cell/"+tools.patternToDate(calc_date,"yyyyMM")).createOrReplaceTempView("dim_map_cell")

        val kpi_kcell = spark.read.format("csv").option("header","true").option("delimiter",";").load("/dmp/beemap/temp/kcell_data/"+tools.patternToDate(calc_date,"yyyyMM")+"*_kpis.csv")
        val counters_kcell = spark.read.format("csv").option("header","true").option("delimiter",";").load("/dmp/beemap/temp/kcell_data/"+tools.patternToDate(calc_date,"yyyyMM")+"*_counters.csv")

        kpi_kcell.createOrReplaceTempView("kpi_kcell")
        counters_kcell.createOrReplaceTempView("counters_kcell")
        val kpi_kcell_1 = spark.sql("""
    select
        site_name,
        cell_fdd cell_name,
        sum((TRAFFIC_DL_MBYTE + TRAFFIC_UL_MBYTE) * PRB_UTIL) / sum(TRAFFIC_DL_MBYTE + TRAFFIC_UL_MBYTE) util_prb_dl_4g,
        sum((TRAFFIC_DL_MBYTE + TRAFFIC_UL_MBYTE) * E_RAB_DR) / sum(TRAFFIC_DL_MBYTE + TRAFFIC_UL_MBYTE) RAB_DR_4G,
        sum((TRAFFIC_DL_MBYTE + TRAFFIC_UL_MBYTE) * LTE_USER_THROUGHPUT_DL) / sum(TRAFFIC_DL_MBYTE + TRAFFIC_UL_MBYTE) User_Throughput_4G
    from kpi_kcell
    group by site_name, cell_fdd
""")

        val kpi_kcell_2 = spark.sql("""
    select
        site_name,
        cell_fdd cell_name,
        100 * (1 - (sum(pmRrcConnEstabSucc)/(sum(pmRrcConnEstabAtt) - sum(pmRrcConnEstabAttReatt))) * (sum(pmS1SigConnEstabSucc)/sum(pmS1SigConnEstabAtt)) * (sum(pmErabEstabSuccInit)/sum(pmErabEstabAttInit))) CunSSR_PS_4G
    from
    counters_kcell
    group by site_name, cell_fdd
""")

        val kpi_kcell_f =
          kpi_kcell_1.
            join(kpi_kcell_2, Seq("site_name", "cell_name"), "full").
            select(
              $"site_name",
              $"cell_name",
              $"util_prb_dl_4g",
              $"RAB_DR_4G",
              $"User_Throughput_4G",
              $"CunSSR_PS_4G"
            )

        kpi_kcell_f.write.mode("overwrite").parquet("/dmp/beemap/temp/kpi_kcell/"+tools.patternToDate(calc_date,"yyyyMM"))
        kpi_kcell_f.createOrReplaceTempView("kpi_kcell")
        val kpi_kcell_final = spark.sql("""
        select
        k.util_prb_dl_4g,
        k.RAB_DR_4G,
        k.User_Throughput_4G,
        k.CunSSR_PS_4G,
        d.*
        from (
        select
            k.site_name,
            k.cell_name,
            max(k.util_prb_dl_4g) over(partition by k.site_name) util_prb_dl_4g,
            max(k.RAB_DR_4G) over(partition by k.site_name) RAB_DR_4G,
            min(k.User_Throughput_4G) over(partition by k.site_name) User_Throughput_4G,
            max(k.CunSSR_PS_4G) over(partition by k.site_name) CunSSR_PS_4G
        from kpi_kcell k
    ) k
    join dim_map_cell d
    on trim(lcase(d.cell_name)) = trim(lcase(k.cell_name))
""")
        kpi_kcell_final.createOrReplaceTempView("kpi_kcell_final")

        val kpi_final = spark.sql("""
    select
        k.cell_name,
        k.enodeb_id,
        k.enodeb_name,
        k.cdr_cs_2g,
        k.CunSSR_CS_2G,
        k.CDR_CS_3G,
        k.rab_dr_ps_3g,
        k.cunssr_cs_3g,
        k.CunSSR_PS_3G,
        k.User_Throughput_3G,
        k.cong_power_ul_dl_3g,
        k.CONG_CE_UL_DL_3G,
        k.CONG_CODE_3G,
        k.CunSSR_PS_4G,
        k.util_prb_dl_4g,
        k.RAB_DR_4G,
        k.User_Throughput_4G,
        d.site_id,
        d.lac,
        d.cell,
        d.type,
        d.cell_owner_id,
        d.site_name,
        d.region,
        d.district,
        d.location,
        d.location_type
    from kpi k join dim_map_cell d on trim(k.cell_name)=trim(d.cell_name) and d.type<>'4G'
    union all
    select
        k.cell_name,
        k.enodeb_id,
        k.enodeb_name,
        null, null, null, null, null,
        null, null, null, null, null,
        max(CunSSR_PS_4G) CunSSR_PS_4G,
        max(util_prb_dl_4g) util_prb_dl_4g,
        max(RAB_DR_4G) RAB_DR_4G,
        min(User_Throughput_4G) User_Throughput_4G,
        d.site_id,
        d.lac,
        d.cell,
        d.type,
        d.cell_owner_id,
        d.site_name,
        d.region,
        d.district,
        d.location,
        d.location_type
    from kpi k join dim_map_cell d on k.enodeb_id=d.cell and d.type='4G' and case when k.cell_name rlike '^[0-9]+\-[0-9]+$' then 'kcell' else 'beeline' end = case when d.cell_name rlike '^[0-9]+\-[0-9]+$' then 'kcell' else 'beeline' end
    group by
        k.cell_name,
        k.enodeb_id,
        k.enodeb_name,
        d.site_id,
        d.lac,
        d.cell,
        d.type,
        d.cell_owner_id,
        d.site_name,
        d.region,
        d.district,
        d.location,
        d.location_type
    union all
    select
        k.cell_name,
        k.cell enodeb_id,
        k.site_name enodeb_name,
        null, null, null, null, null,
        null, null, null, null, null,
        CunSSR_PS_4G,
        util_prb_dl_4g,
        RAB_DR_4G,
        User_Throughput_4G,
        site_id,
        lac,
        cell,
        type,
        cell_owner_id,
        site_name,
        region,
        district,
        location,
        location_type
    from kpi_kcell_final k
    """)

        kpi_final.createOrReplaceTempView("kpi_final")
        kpi_final.write.mode("overwrite").parquet(kpi_final_cell_path + tools.patternToDate(calc_date,"yyyyMM"))

        spark.sql("""
        select
        k.site_id,
        max(k.cdr_cs_2g) CDR_2G,
        max(k.CunSSR_CS_2G) CunSSR_2G,
        max(k.cunssr_cs_3g) CunSSR_CS_3G,
        max(k.CDR_CS_3G) CDR_CS_3G,
        max(k.CunSSR_PS_3G) CunSSR_PS_3G,
        max(k.rab_dr_ps_3g) rab_dr_ps_3g,
        min(k.User_Throughput_3G) Throughput_3G,
        max(k.CONG_CODE_3G) CONG_CODE_3G,
        max(k.CONG_CE_UL_DL_3G) CONG_CE_UL_DL_3G,
        max(k.cong_power_ul_dl_3g) cong_power_ul_dl_3g,
        max(k.RAB_DR_4G) RAB_DR_PS_4G,
        max(k.CunSSR_PS_4G) CunSSR_PS_4G,
        min(k.User_Throughput_4G) Throughput_4G,
        max(k.util_prb_dl_4g) util_prb_dl_4g
          from kpi_final k
        group by k.site_id
        """).write.mode("overwrite").parquet(kpi_final_site_path + tools.patternToDate(calc_date,"yyyyMM"))

        spark.close()
      } catch {
        case e: Exception => println (s"ERROR: $e")
          status = false
          if (spark != null) spark.close ()
      } finally {}
    status
  }
}