package beemap

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utl.SparkBase
import utl.Tools

import scala.util.control.Breaks.break

class Beemap1(propMap: Map[String, String], sparkMap: Map[String, String], pdate: String, tools: Tools) extends SparkBase(propMap, sparkMap) {

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

    //val (s_year, s_month, s_day) = ("2018", "03", "01")
    //val (e_year, e_month, e_day) = ("2018", "04", "01")

    val det_df = spark.read.parquet ("/dmp/daily_stg/tg_det_layer_trans_parq/*" +
    tools.patternToDate (calc_date, "MMyyyy") )
    det_df.createOrReplaceTempView ("det_df")

    spark.read.parquet ("/mnt/gluster-storage/etl/download/biis/dim_business_service").createOrReplaceTempView ("dim_bs")
    spark.read.parquet ("/mnt/gluster-storage/etl/download/biis/dim_business_service_type").createOrReplaceTempView ("dim_bs_type")
    spark.read.format ("csv").option ("header", "true").load ("/dmp/beemap/dict/not_usable_at.csv").createOrReplaceTempView ("not_usable_at")
    spark.read.format ("csv").option ("header", "true").load ("/dmp/beemap/dict/not_usable_pp.csv").createOrReplaceTempView ("not_usable_pp")

    val prep_df = spark.sql ("""
       SELECT SUBS_KEY, --АГРЕГИРУЕМ ДАННЫЕ ДО УНИКАЛЬНОСТИ : НОМЕР LAC CELL
       CAST(LAC AS LONG) LAC,
       CAST(CELL_ID AS LONG) CELL,
       SUM(CASE
             WHEN D.call_type_code = 'V' AND D.CALL_DIRECTION_IND = '1' AND
                  D.LOCATION_TYPE_KEY IN ('1', '2') AND
                  D.CONNECTION_TYPE_KEY NOT IN ('1', '2') THEN
              D.ACTUAL_CALL_DURATION_SEC / 60
             ELSE
              0
           END) AS VOICE_IN_B_L, --ГОЛОС ВХОЯДЩИЙ МЕСТНЫЙ
       SUM(CASE
             WHEN D.call_type_code = 'V' AND D.CALL_DIRECTION_IND = '1' AND
                  D.LOCATION_TYPE_KEY IN ('1', '2') AND
                  D.CONNECTION_TYPE_KEY IN ('1', '2') THEN
              D.ACTUAL_CALL_DURATION_SEC / 60
             ELSE
              0
           END) AS VOICE_IN_B_F, --ГОЛОС ВХОЯДЩИЙ ФИКСА
       SUM(CASE
             WHEN D.call_type_code = 'V' AND D.CALL_DIRECTION_IND = '1' AND
                  D.LOCATION_TYPE_KEY IN ('3') THEN
              D.ACTUAL_CALL_DURATION_SEC / 60
             ELSE
              0
           END) AS VOICE_IN_MEZH, --ГОЛОС ВХОЯДЩИЙ МЕЖНАР
       --OUT
       SUM(CASE
             WHEN D.call_type_code = 'V' AND D.CALL_DIRECTION_IND = '2' AND
                  D.LOCATION_TYPE_KEY IN ('1', '2') AND
                  D.CONNECTION_TYPE_KEY NOT IN ('1', '2') THEN
              D.ACTUAL_CALL_DURATION_SEC / 60
             ELSE
              0
           END) AS VOICE_OUT_B_L, --ГОЛОС ИСХОДЯЩИЙ МЕСТНЫЙ
       SUM(CASE
             WHEN D.call_type_code = 'V' AND D.CALL_DIRECTION_IND = '2' AND
                  D.LOCATION_TYPE_KEY IN ('1', '2') AND
                  D.CONNECTION_TYPE_KEY IN ('1', '2') THEN
              D.ACTUAL_CALL_DURATION_SEC / 60
             ELSE
              0
           END) AS VOICE_OUT_B_F, --ГОЛОС ИСХОДЯЩИЙ ФИКСА
       SUM(CASE
             WHEN D.call_type_code = 'V' AND D.CALL_DIRECTION_IND = '2' AND
                  D.LOCATION_TYPE_KEY IN ('3') THEN
              D.ACTUAL_CALL_DURATION_SEC / 60
             ELSE
              0
           END) AS VOICE_OUT_MEZH, --ГОЛОС ИСХОДЯЩИЙ МЕЖНАР
       SUM(CASE
             WHEN D.call_type_code = 'V' THEN
              D.ACTUAL_CALL_DURATION_SEC / 60
             ELSE
              0
           END) AS VOICE_TOTAL, --ГОЛОС ОБЩИЙ
       --GRS
       SUM(CASE
             WHEN D.call_type_code = 'G' THEN
              D.ROUNDED_DATA_VOLUME / 1024 / 1024
             ELSE
              0
           END) AS DATA_VOLUME_MB, --ОБЪЕМ ИНТЕРНЕТ-ТРАФИКА (МБ)
       --REVENUE
       SUM(CASE
             WHEN D.call_type_code = 'V' THEN
              D.CHARGE_AMT --+ D.ADDITIONAL_CHARGE_AMT
             ELSE
              0
           END) AS REVENUE_VOICE, --СПИСАНИЯ ЗА ГОЛОС
       SUM(CASE
             WHEN D.call_type_code = 'G' THEN
              D.CHARGE_AMT --+ D.ADDITIONAL_CHARGE_AMT
             ELSE
              0
           END) AS REVENUE_GPRS, --СПИСАНИЯ ЗА GPRS
       SUM(CASE
             WHEN D.call_type_code NOT IN ('V', 'G') AND
                  D.SRC NOT IN ('CHR', 'POST') THEN
              D.CHARGE_AMT --+ D.ADDITIONAL_CHARGE_AMT
             ELSE
              0
           END) AS REVENUE_OTHER, --СПИСАНИЯ ЗА ОСТАЛЬНОЙ ТРАФИК
       SUM(CASE
             WHEN D.SRC IN ('CHR') THEN
              D.CHARGE_AMT
             ELSE
              0
           END) AS REVENUE_O_R
        from det_df d
        join dim_bs dbs --ПРИВЯЗЫВАЕМ СПРАВОЧНИК  БУ
          on d.business_service_key = dbs.business_service_key
         --and d.src in ('CHR')
        join dim_bs_type dbst --ПРИВЯЗЫВАЕМ СПРАВОЧНИК  ТБУ
          on dbs.business_service_type_key = dbst.business_service_type_key
         --and d.src in ('CHR')
       where (((d.SRC in ('CHR') and
             d.CHARGE_TYPE != 'N' --ИСКЛЮЧАЕМ ТИП "N"
             and d.ban_key > 0 -- по-хорошему здесь нужно такое условие: SEGMENT_KEY != '-99' -- ИСКЛЮЧАЕМ СПИСАНИЯ, ГДЕ НЕ ОПРЕДЕЛЕН СЕГМЕНТ
             ) or d.SRC in ('CHA', 'GPRS', 'PSA', 'ROAM')))
         and DBST.REVENUE_STREAM != 9 --ИСКЛЮЧАЕМ 9 СТРИМ, ROAMING
         and d.ACCOUNT_TYPE_CODE NOT IN (SELECT * FROM NOT_USABLE_AT)
         AND d.PRICE_PLAN_KEY NOT IN (SELECT * FROM NOT_USABLE_PP)
       group by SUBS_KEY, CAST(LAC AS LONG), CAST(CELL_ID AS LONG)
       """)

    prep_df.createOrReplaceTempView ("prep_df")

    val lac_cell_det = spark.sql ("""
       SELECT LAC,
       CELL,
       COUNT(DISTINCT SUBS_KEY) AS TOTAL_SUBSCRIBER_AMT, --ОБЩЕЕ КОЛИЧЕСТВО АБОНЕНТОВ,ПОБЫВАВШИХ НА CELL
       COUNT(DISTINCT(CASE
                        WHEN DATA_VOLUME_MB > 0 THEN
                         SUBS_KEY
                        ELSE
                         NULL
                      END)) AS TOTAL_DATA_USERS_AMT, --РАССЧИТЫВАЕМ КОЛИЧЕСТВО GPRS-ПОЛЬЗОВАТЕЛЕЙ
       SUM(VOICE_IN_B_L) AS VOICE_IN_B_L,
       SUM(VOICE_IN_B_F) AS VOICE_IN_B_F,
       SUM(VOICE_IN_MEZH) AS VOICE_IN_MEZH,
       SUM(VOICE_OUT_B_L) AS VOICE_OUT_B_L,
       SUM(VOICE_OUT_B_F) AS VOICE_OUT_B_F,
       SUM(VOICE_OUT_MEZH) AS VOICE_OUT_MEZH,
       SUM(VOICE_TOTAL) AS VOICE_TOTAL,
       SUM(DATA_VOLUME_MB) AS DATA_VOLUME_MB,
       SUM(REVENUE_VOICE) AS REVENUE_VOICE,
       SUM(REVENUE_GPRS) AS REVENUE_GPRS,
       SUM(REVENUE_OTHER_P) AS REVENUE_OTHER_P,
       SUM(REVENUE_O_R_P) AS REVENUE_O_R_P
  FROM (SELECT A.SUBS_KEY, --СТРОКИ УНИКАЛЬНЫ ДЛЯ ПОЛЕЙ: НОМЕР LAC CELL
               A.LAC,
               A.CELL,
               A.VOICE_IN_B_L,
               A.VOICE_IN_B_F,
               A.VOICE_IN_MEZH,
               A.VOICE_OUT_B_L,
               A.VOICE_OUT_B_F,
               A.VOICE_OUT_MEZH,
               A.VOICE_TOTAL,
               A.DATA_VOLUME_MB,
               A.REVENUE_VOICE,
               A.REVENUE_GPRS,
               (CASE
                 WHEN SUM(A.VOICE_TOTAL) OVER(PARTITION BY A.SUBS_KEY) > 0 THEN
                  (SUM(A.REVENUE_OTHER) OVER(PARTITION BY A.SUBS_KEY)) *
                  ((A.VOICE_TOTAL / SUM(A.VOICE_TOTAL)
                   OVER(PARTITION BY A.SUBS_KEY)))
                 WHEN SUM(A.VOICE_TOTAL) OVER(PARTITION BY A.SUBS_KEY) = 0 AND
                      SUM(A.DATA_VOLUME_MB)
                  OVER(PARTITION BY A.SUBS_KEY) > 0 THEN
                  (SUM(A.REVENUE_OTHER) OVER(PARTITION BY A.SUBS_KEY)) *
                  (A.DATA_VOLUME_MB / SUM(A.DATA_VOLUME_MB)
                   OVER(PARTITION BY A.SUBS_KEY))
                 ELSE
                  A.REVENUE_OTHER
               END) AS REVENUE_OTHER_P, --РАСПРЕДЕЛЕНИЕ ЧАСТИ ТРАФИКОВОЙ ВЫРУЧКИ ПО CELL
               --ЕСЛИ ЕСТЬ ТРАФИК, ТО ВЫРУЧКА РАСПРЕДЕЛЯЕТСЯ СОГЛАСНО ПРОЦЕНТУ
               --ЕСЛИ ТРАФИКА НЕТ ТО ВЫРУЧКА НЕ ПЕРЕРАСПРЕДЕЛЯЕТСЯ, А ОСТАЕТСЯ НА ТОЙ СВЯЗКЕ LAC+CELL
               --НА КОТОРОЙ ОНА ИЗНАЧАЛЬНО БЫЛА СГЕНЕРИРОВАНА
               (CASE
                 WHEN SUM(A.VOICE_TOTAL) OVER(PARTITION BY A.SUBS_KEY) > 0 THEN
                  (SUM(A.REVENUE_O_R) OVER(PARTITION BY A.SUBS_KEY)) *
                  ((A.VOICE_TOTAL / SUM(A.VOICE_TOTAL)
                   OVER(PARTITION BY A.SUBS_KEY)))
                 WHEN SUM(A.VOICE_TOTAL) OVER(PARTITION BY A.SUBS_KEY) = 0 AND
                      SUM(A.DATA_VOLUME_MB)
                  OVER(PARTITION BY A.SUBS_KEY) > 0 THEN
                  (SUM(A.REVENUE_O_R) OVER(PARTITION BY A.SUBS_KEY)) *
                  (A.DATA_VOLUME_MB / SUM(A.DATA_VOLUME_MB)
                   OVER(PARTITION BY A.SUBS_KEY))
                 ELSE
                  A.REVENUE_O_R
               END) AS REVENUE_O_R_P --РАСПРЕДЕЛЕНИЕ РАЗОВЫХ И ПЕРИОДИЧЕСКИХ СПИСАНИЙ ПО CELL
        --ЕСЛИ ЕСТЬ ТРАФИК, ТО ВЫРУЧКА РАСПРЕДЕЛЯЕТСЯ СОГЛАСНО ПРОЦЕНТУ
        --ЕСЛИ ТРАФИКА НЕТ ТО ВЫРУЧКА НЕ ПЕРЕРАСПРЕДЕЛЯЕТСЯ, А ОСТАЕТСЯ НА ТОЙ СВЯЗКЕ LAC+CELL
        --НА КОТОРОЙ ОНА ИЗНАЧАЛЬНО БЫЛА СГЕНЕРИРОВАНА
          FROM prep_df A)
             WHERE 1 = 1
             GROUP BY LAC, CELL
            """).cache

    lac_cell_det.createOrReplaceTempView ("lac_cell_det")

    lac_cell_det.write.mode ("overwrite").parquet (lcprep_path + tools.patternToDate (calc_date, "yyyyMM") )


    val post_df = spark.sql ("""
    SELECT SUBS_KEY, --АГРЕГИРУЕМ ДАННЫЕ ДО УНИКАЛЬНОСТИ : НОМЕР LAC CELL
           BAN_KEY,
           CAST(LAC AS LONG) LAC,
           CAST(CELL_ID AS LONG) CELL,
           --IN
           SUM(CASE
                 WHEN D.CALL_TYPE_CODE = 'V' AND D.CALL_DIRECTION_IND = '1' AND
                      D.LOCATION_TYPE_KEY IN ('1', '2') AND
                      D.CONNECTION_TYPE_KEY NOT IN ('1', '2') THEN
                  D.ACTUAL_CALL_DURATION_SEC / 60
                 ELSE
                  0
               END) AS VOICE_IN_B_L, --ГОЛОС ВХОЯДЩИЙ МЕСТНЫЙ
           SUM(CASE
                 WHEN D.CALL_TYPE_CODE = 'V' AND D.CALL_DIRECTION_IND = '1' AND
                      D.LOCATION_TYPE_KEY IN ('1', '2') AND
                      D.CONNECTION_TYPE_KEY IN ('1', '2') THEN
                  D.ACTUAL_CALL_DURATION_SEC / 60
                 ELSE
                  0
               END) AS VOICE_IN_B_F, --ГОЛОС ВХОЯДЩИЙ ФИКСА
           SUM(CASE
                 WHEN D.CALL_TYPE_CODE = 'V' AND D.CALL_DIRECTION_IND = '1' AND
                      D.LOCATION_TYPE_KEY IN ('3') THEN
                  D.ACTUAL_CALL_DURATION_SEC / 60
                 ELSE
                  0
               END) AS VOICE_IN_MEZH, --ГОЛОС ВХОЯДЩИЙ МЕЖНАР
           --OUT
           SUM(CASE
                 WHEN D.CALL_TYPE_CODE = 'V' AND D.CALL_DIRECTION_IND = '2' AND
                      D.LOCATION_TYPE_KEY IN ('1', '2') AND
                      D.CONNECTION_TYPE_KEY NOT IN ('1', '2') THEN
                  D.ACTUAL_CALL_DURATION_SEC / 60
                 ELSE
                  0
               END) AS VOICE_OUT_B_L, --ГОЛОС ИСХОДЯЩИЙ МЕСТНЫЙ
           SUM(CASE
                 WHEN D.CALL_TYPE_CODE = 'V' AND D.CALL_DIRECTION_IND = '2' AND
                      D.LOCATION_TYPE_KEY IN ('1', '2') AND
                      D.CONNECTION_TYPE_KEY IN ('1', '2') THEN
                  D.ACTUAL_CALL_DURATION_SEC / 60
                 ELSE
                  0
               END) AS VOICE_OUT_B_F, --ГОЛОС ИСХОДЯЩИЙ ФИКСА
           SUM(CASE
                 WHEN D.CALL_TYPE_CODE = 'V' AND D.CALL_DIRECTION_IND = '2' AND
                      D.LOCATION_TYPE_KEY IN ('3') THEN
                  D.ACTUAL_CALL_DURATION_SEC / 60
                 ELSE
                  0
               END) AS VOICE_OUT_MEZH, --ГОЛОС ИСХОДЯЩИЙ МЕЖНАР
           SUM(CASE
                 WHEN D.CALL_TYPE_CODE = 'V' THEN
                  D.ACTUAL_CALL_DURATION_SEC / 60
                 ELSE
                  0
               END) AS VOICE_TOTAL, --ГОЛОС ОБЩИЙ
           --GRRS
           SUM(CASE
                 WHEN D.CALL_TYPE_CODE = 'G' THEN
                  D.ROUNDED_DATA_VOLUME / 1024 / 1024
                 ELSE
                  0
               END) AS DATA_VOLUME_MB --GPRS ТРАФИК (МЕГАБАЙТЫ)
      FROM det_df D
      join dim_bs dbs --ПРИВЯЗЫВАЕМ СПРАВОЧНИК  БУ
        on d.business_service_key = dbs.business_service_key
      join dim_bs_type dbst --ПРИВЯЗЫВАЕМ СПРАВОЧНИК  ТБУ
        on dbs.business_service_type_key = dbst.business_service_type_key
     WHERE D.SRC in ('POST')
       and DBST.REVENUE_STREAM != 9 --ИСКЛЮЧАЕМ 9 СТРИМ, ROAMING
       and d.ACCOUNT_TYPE_CODE NOT IN (SELECT * FROM NOT_USABLE_AT)
       AND d.PRICE_PLAN_KEY NOT IN (SELECT * FROM NOT_USABLE_PP)
     group by SUBS_KEY, BAN_KEY, CAST(LAC AS LONG), CAST(CELL_ID AS LONG)
     """)

    post_df.createOrReplaceTempView ("post_df")

    val karkaz_charge_1 = spark.read.parquet ("/mnt/gluster-storage/etl/download/karkaz_bc/charge/" +
    tools.patternToDate (calc_date, "MM-yyyy") )
    val karkaz_charge_2 = spark.read.parquet ("/mnt/gluster-storage/etl/download/karkaz_bc/charge/" +
    tools.patternToDate (tools.addMonths (calc_date, 1), "MM-yyyy") + "/0[1-5]*")
    karkaz_charge_1.union (karkaz_charge_2).cache.createOrReplaceTempView ("karkaz_charge")
    spark.read.parquet ("/mnt/gluster-storage/etl/download/karkaz_bc/billing_account").createOrReplaceTempView ("karkaz_billing_account")

    val post_charges_1 = spark.sql (s"""
    SELECT
        C.BAN,
        sum(C.ACTV_AMT) AS REVENUE
    FROM karkaz_charge C
    join karkaz_billing_account B
      ON C.BAN = B.BAN
   WHERE SUBSTR(C.PRIOD_CVRG_ST_DATE,1,7) = '${tools.patternToDate (calc_date, "yyyy")}-${tools.patternToDate (calc_date, "MM")}'
        AND C.PRIOD_CVRG_ST_DATE IS NOT NULL
        AND TRIM(C.INVOICE_TYPE) IN ('B','BN','ET')
        AND TRIM(B.ACCOUNT_TYPE) NOT IN ('214','215')
    GROUP BY C.BAN
""")

    spark.read.parquet ("/mnt/gluster-storage/etl/download/karkaz_bc/invoice_item/*/*").createOrReplaceTempView ("karkaz_invoice_item")
    val post_charges_2 = spark.sql (s"""
    SELECT
        C.BAN,
        SUM(C.ACTV_AMT) AS REVENUE
    FROM karkaz_charge C
    JOIN karkaz_invoice_item t
      ON TRIM(C.DOCUMENT_NUMBER) = TRIM(T.INVOICE_NUMBER)
     AND C.BAN = T.BAN
    JOIN karkaz_invoice_item Q
      ON TRIM(T.RELAT_CRD_NOTE_NO) = TRIM(Q.INVOICE_NUMBER)
     AND C.BAN = T.BAN
     AND TRIM(Q.INV_TYPE) IN ('B','BN','ET')
    JOIN KARKAZ_BILLING_ACCOUNT B
      ON C.BAN = B.BAN
   WHERE SUBSTR(C.PRIOD_CVRG_ST_DATE,1,7) = '${tools.patternToDate (calc_date, "yyyy")}-${tools.patternToDate (calc_date, "MM")}'
    AND SUBSTR(C.Priod_Cvrg_Nd_Date,1,7) = '${tools.patternToDate (calc_date, "yyyy")}-${tools.patternToDate (calc_date, "MM")}'
    AND C.PRIOD_CVRG_ST_DATE IS NOT NULL
    AND TRIM(C.INVOICE_TYPE) IN ('C')
    AND TRIM(B.ACCOUNT_TYPE) NOT IN ('214','215')
    AND T.RELAT_CRD_NOTE_NO IS NOT NULL
    GROUP BY C.BAN
""")

    val post_charges_3 = spark.sql (s"""
    SELECT
        C.BAN,
        SUM(C.ACTV_AMT) AS REVENUE
    FROM karkaz_charge C
    JOIN karkaz_invoice_item t
      ON TRIM(C.DOCUMENT_NUMBER) = TRIM(T.INVOICE_NUMBER)
     AND C.BAN = T.BAN
    JOIN karkaz_invoice_item Q
      ON TRIM(T.RELAT_CRD_NOTE_NO) = TRIM(Q.INVOICE_NUMBER)
     AND C.BAN = T.BAN
     AND TRIM(Q.INV_TYPE) IN ('C')
    JOIN KARKAZ_BILLING_ACCOUNT B
      ON C.BAN = B.BAN
   WHERE SUBSTR(C.PRIOD_CVRG_ST_DATE,1,7) = '${tools.patternToDate (calc_date, "yyyy")}-${tools.patternToDate (calc_date, "MM")}'
    AND SUBSTR(C.Priod_Cvrg_Nd_Date,1,7) = '${tools.patternToDate (calc_date, "yyyy")}-${tools.patternToDate (calc_date, "MM")}'
    AND C.PRIOD_CVRG_ST_DATE IS NOT NULL
    AND TRIM(C.INVOICE_TYPE) IN ('RV')
    AND TRIM(B.ACCOUNT_TYPE) NOT IN ('214','215')
    AND T.RELAT_CRD_NOTE_NO IS NOT NULL
   GROUP BY C.BAN
""")

    val post_charges = post_charges_1.union (post_charges_2).union (post_charges_3)
    post_charges.createOrReplaceTempView ("post_charges")

    val lac_cell_post = spark.sql ("""
    SELECT LAC,
           CELL,
           SUM(TOTAL_SUBSCRIBER_AMT) AS TOTAL_SUBSCRIBER_AMT,
           SUM(TOTAL_DATA_USERS_AMT) AS TOTAL_DATA_USERS_AMT,
           SUM(VOICE_IN_B_L) AS VOICE_IN_B_L,
           SUM(VOICE_IN_B_F) AS VOICE_IN_B_F,
           SUM(VOICE_IN_MEZH) AS VOICE_IN_MEZH,
           SUM(VOICE_OUT_B_L) AS VOICE_OUT_B_L,
           SUM(VOICE_OUT_B_F) AS VOICE_OUT_B_F,
           SUM(VOICE_OUT_MEZH) AS VOICE_OUT_MEZH,
           SUM(VOICE_TOTAL) AS VOICE_TOTAL,
           SUM(DATA_VOLUME_MB) AS DATA_VOLUME_MB,
           SUM(REVENUE_P) AS REVENUE_P
      FROM (SELECT A.BAN_KEY,
                   A.LAC,
                   A.CELL,
                   A.TOTAL_SUBSCRIBER_AMT,
                   A.TOTAL_DATA_USERS_AMT,
                   A.VOICE_IN_B_L,
                   A.VOICE_IN_B_F,
                   A.VOICE_IN_MEZH,
                   A.VOICE_OUT_B_L,
                   A.VOICE_OUT_B_F,
                   A.VOICE_OUT_MEZH,
                   A.VOICE_TOTAL,
                   A.DATA_VOLUME_MB,
                   A.REVENUE,
                   SUM(A.VOICE_TOTAL) OVER(PARTITION BY A.BAN_KEY) AS TOTAL_VOICE,
                   SUM(A.DATA_VOLUME_MB) OVER(PARTITION BY A.BAN_KEY) AS TOTAL_GPRS,
                   (CASE
                     WHEN SUM(A.VOICE_TOTAL) OVER(PARTITION BY A.BAN_KEY) > 0 THEN
                      (A.VOICE_TOTAL / SUM(A.VOICE_TOTAL)
                       OVER(PARTITION BY A.BAN_KEY))
                     WHEN SUM(A.VOICE_TOTAL) OVER(PARTITION BY A.BAN_KEY) = 0 AND
                          SUM(A.DATA_VOLUME_MB)
                      OVER(PARTITION BY A.BAN_KEY) > 0 THEN
                      (A.DATA_VOLUME_MB / SUM(A.DATA_VOLUME_MB)
                       OVER(PARTITION BY A.BAN_KEY))
                     ELSE
                      1
                   END) AS USAGE_PERCENT,
                   (CASE
                     WHEN SUM(A.VOICE_TOTAL) OVER(PARTITION BY A.BAN_KEY) > 0 THEN
                      (SUM(A.REVENUE) OVER(PARTITION BY A.BAN_KEY)) *
                      ((A.VOICE_TOTAL / SUM(A.VOICE_TOTAL)
                       OVER(PARTITION BY A.BAN_KEY)))
                     WHEN SUM(A.VOICE_TOTAL) OVER(PARTITION BY A.BAN_KEY) = 0 AND
                          SUM(A.DATA_VOLUME_MB)
                      OVER(PARTITION BY A.BAN_KEY) > 0 THEN
                      (SUM(A.REVENUE) OVER(PARTITION BY A.BAN_KEY)) *
                      (A.DATA_VOLUME_MB / SUM(A.DATA_VOLUME_MB)
                       OVER(PARTITION BY A.BAN_KEY))
                     ELSE
                      A.REVENUE
                   END) AS REVENUE_P
              FROM (SELECT BAN_KEY,
                           LAC,
                           CELL,
                           COUNT(DISTINCT SUBS_KEY) AS TOTAL_SUBSCRIBER_AMT,
                           COUNT(DISTINCT(CASE
                                            WHEN DATA_VOLUME_MB > 0 THEN
                                             SUBS_KEY
                                            ELSE
                                             NULL
                                          END)) AS TOTAL_DATA_USERS_AMT,
                           SUM(VOICE_IN_B_L) AS VOICE_IN_B_L,
                           SUM(VOICE_IN_B_F) AS VOICE_IN_B_F,
                           SUM(VOICE_IN_MEZH) AS VOICE_IN_MEZH,
                           SUM(VOICE_OUT_B_L) AS VOICE_OUT_B_L,
                           SUM(VOICE_OUT_B_F) AS VOICE_OUT_B_F,
                           SUM(VOICE_OUT_MEZH) AS VOICE_OUT_MEZH,
                           SUM(VOICE_TOTAL) AS VOICE_TOTAL,
                           SUM(DATA_VOLUME_MB) AS DATA_VOLUME_MB,
                           SUM(REVENUE) AS REVENUE
                      from (SELECT T.SUBS_KEY,
                                   T.BAN_KEY,
                                   t.lac,
                                   t.cell,
                                   T.VOICE_IN_B_L,
                                   T.VOICE_IN_B_F,
                                   T.VOICE_IN_MEZH,
                                   T.VOICE_OUT_B_L,
                                   T.VOICE_OUT_B_F,
                                   T.VOICE_OUT_MEZH,
                                   T.VOICE_TOTAL,
                                   T.DATA_VOLUME_MB,
                                   0 AS REVENUE
                              FROM post_df T
                            union all
                            SELECT null as SUBS_KEY,
                                   J.BAN as BAN_KEY,
                                   0 as LAC,
                                   0 as CELL,
                                   0 AS VOICE_IN_B_L,
                                   0 AS VOICE_IN_B_F,
                                   0 AS VOICE_IN_MEZH,
                                   0 AS VOICE_OUT_B_L,
                                   0 AS VOICE_OUT_B_F,
                                   0 AS VOICE_OUT_MEZH,
                                   0 AS VOICE_TOTAL,
                                   0 AS DATA_VOLUME_MB,
                                   SUM(J.REVENUE) AS REVENUE
                              FROM post_charges J
                             GROUP BY J.BAN)
                     group by BAN_KEY, LAC, CELL) A)
              GROUP BY LAC, CELL
              """)

    lac_cell_post.write.mode ("overwrite").parquet (lcpost_path + tools.patternToDate (calc_date, "yyyyMM") )


    // Step 2
    spark.read.parquet ("/mnt/gluster-storage/etl/download/biis/fct_outcollect_usage/" + tools.patternToDate (calc_date, "MM-yyyy") )
    .createOrReplaceTempView ("fct_outcollect_usage")
    spark.read.parquet ("/mnt/gluster-storage/etl/download/biis/fct_currency_rate/" + tools.patternToDate (calc_date, "MM-yyyy") )
    .createOrReplaceTempView ("fct_currency_rate")
    spark.read.parquet ("/mnt/gluster-storage/etl/download/beemap/cat_cell_used").createOrReplaceTempView ("cat_cell_used")
    spark.read.parquet ("/mnt/gluster-storage/etl/download/beemap/cat_site_used").createOrReplaceTempView ("cat_site_used")

    val dim_map_cell = spark.sql (s"""
    select c.site_id,
       c.cell_name,
       c.lac,
       c.cell,
       c.type,
       c.exploit_start,
       c.exploit_end,
       c.cell_owner_id,
       s.site_name,
       s.site_name_reverse,
       s.lat,
       s.lng,
       s.region,
       s.district,
       s.location,
       s.location_type,
       s.region_id,
       s.district_id,
       s.location_id,
       s.exploit_start site_exploit_start,
       s.exploit_end site_exploit_end
  from cat_cell_used c
  join cat_site_used s
    on c.site_id + 0 = s.site_id + 0
 where c.exploit_start < '${tools.patternToDate (tools.addMonths (calc_date, 1), "yyyy")}-
      ${tools.patternToDate (tools.addMonths (calc_date, 1), "MM")}-
      ${tools.patternToDate (tools.addMonths (calc_date, 1), "dd")}'
   and nvl(c.exploit_end, '2999-12-31') >= '${tools.patternToDate (calc_date, "yyyy")}-${tools.patternToDate (calc_date, "MM")}-${tools.patternToDate (calc_date, "dd")}'
""")
    dim_map_cell.createOrReplaceTempView ("dim_map_cell")
    // This step should not be here - everything in cat_cell_used and cat_site_used must be correct (no dups)
    val dim_map_cell_correct = spark.sql ("select * from (select t.*, row_number() over(partition by lac,cell order by exploit_start) rn from dim_map_cell t) where rn=1")
    dim_map_cell_correct.createOrReplaceTempView ("dim_map_cell_correct")
    dim_map_cell_correct.write.mode ("overwrite").parquet (dmcell_path + tools.patternToDate (calc_date, "yyyyMM") )

    val guest_roam = spark.sql ("""
    SELECT
      T.UNIQUE_ID, --IMSI АБОНЕНТА
      T.LOCATION_AREA, --LAC
      T.CELL_ID, --CELL
      T.CALL_TYPE_CODE,
      SUM(T.CALL_DURATION_SEC) AS DURATION_SEC,
      SUM(T.DATA_VOLUME) AS DATA_VOLUME,
      SUM(T.CHARGE_AMT) AS CHARGE_AMT_USD,--ВЫРУЧКА USD
      SUM(T.CHARGE_AMT*R.CURRENCY_RATE) AS CHARGE_AMT --ВЫРУЧКА ТГ
     FROM FCT_OUTCOLLECT_USAGE T
     JOIN FCT_CURRENCY_RATE R
       ON substr(T.CALL_START_TIME, 1, 10) = substr(R.RATE_DATE_KEY, 1, 10)
      AND trim(R.CURRENCY_CODE_FROM) = 'D'
      AND trim(R.CURRENCY_CODE_TO) = 'P'
    WHERE trim(T.COUNTED_CDR_IND) = '1'
    GROUP BY T.UNIQUE_ID,
      T.LOCATION_AREA,
      T.CELL_ID,
      T.CALL_TYPE_CODE
""")

    guest_roam.createOrReplaceTempView ("guest_roam")

    val guest_r_cell_rep = spark.sql ("""
    SELECT NVL(T.LOCATION_AREA + 0, 0) AS LAC,
           NVL(T.CELL_ID + 0, 0) AS CELL,
           COUNT(DISTINCT T.UNIQUE_ID) AS UNIQ_GUEST_AMT,
           COUNT(DISTINCT(CASE
                            WHEN T.CALL_TYPE_CODE = 'G' AND T.DATA_VOLUME > 0 THEN
                             T.UNIQUE_ID
                            ELSE
                             null
                          END)) AS GUEST_DATA_USERS_AMT,
           SUM(CASE
                 WHEN CALL_TYPE_CODE = 'V' THEN
                  DURATION_SEC / 60
                 ELSE
                  0
               END) AS VOICE_DURATION_MIN,
           SUM(CASE
                 WHEN CALL_TYPE_CODE = 'G' THEN
                  DATA_VOLUME
                 ELSE
                  0
               END) AS DATA_VOLUME_MB,
           SUM(CHARGE_AMT_USD) AS CHARGE_AMT_USD,
           SUM(CHARGE_AMT) AS CHARGE_AMT
      FROM guest_roam T
     where trim(t.location_area) != 'ZZZZZ'
     GROUP BY NVL(T.LOCATION_AREA + 0, 0), NVL(T.CELL_ID + 0, 0)
""")

    guest_r_cell_rep.write.mode ("overwrite").parquet (outroam_path + tools.patternToDate (calc_date, "yyyyMM") )

    val guest_r_site_rep = spark.sql ("""
    SELECT NVL(I.SITE_ID, 0) AS SITE_ID,
           COUNT(DISTINCT T.UNIQUE_ID) AS UNIQ_GUEST_AMT,
           COUNT(DISTINCT(CASE
                            WHEN T.CALL_TYPE_CODE = 'G' AND T.DATA_VOLUME > 0 THEN
                             T.UNIQUE_ID
                            ELSE
                             NULL
                          END)) AS GUEST_DATA_USERS_AMT,
           SUM(CASE
                 WHEN CALL_TYPE_CODE = 'V' THEN
                  DURATION_SEC / 60
                 ELSE
                  0
               END) AS VOICE_DURATION_MIN,
           SUM(CASE
                 WHEN CALL_TYPE_CODE = 'G' THEN
                  DATA_VOLUME
                 ELSE
                  0
               END) AS DATA_VOLUME_MB,
           SUM(CHARGE_AMT_USD) AS CHARGE_AMT_USD,
           SUM(CHARGE_AMT) AS CHARGE_AMT
      FROM guest_roam T
      LEFT JOIN dim_map_cell_correct I
        ON T.LOCATION_AREA + 0 = I.LAC + 0
       AND T.CELL_ID + 0 = I.CELL + 0
     where t.location_area != 'ZZZZZ'
     GROUP BY NVL(I.SITE_ID, 0)
""")

    guest_r_site_rep.write.mode ("overwrite").parquet (outroams_path + tools.patternToDate (calc_date, "yyyyMM") )

    // Step 3
    spark.read.parquet (lcprep_path + tools.patternToDate (calc_date, "yyyyMM") ).createOrReplaceTempView ("lac_cell_prep_stg")
    spark.read.parquet (lcpost_path + tools.patternToDate (calc_date, "yyyyMM") ).createOrReplaceTempView ("lac_cell_post_stg")
    spark.read.parquet (outroam_path + tools.patternToDate (calc_date, "yyyyMM") ).createOrReplaceTempView ("out_roam_cell")
    spark.read.parquet (dmcell_path + tools.patternToDate (calc_date, "yyyyMM") ).createOrReplaceTempView ("dim_map_cell")

    val map_rep = spark.sql ("""
  SELECT LAC,
         CELL,
         SUM(TOTAL_SUBSCRIBER_AMT) AS TOTAL_SUBSCRIBER_AMT,
         SUM(TOTAL_DATA_USERS_AMT) AS TOTAL_DATA_USERS_AMT,
         SUM(VOICE_IN_B_L) AS VOICE_IN_B_L,
         SUM(VOICE_IN_B_F) AS VOICE_IN_B_F,
         SUM(VOICE_IN_MEZH) AS VOICE_IN_MEZH,
         SUM(VOICE_OUT_B_L) AS VOICE_OUT_B_L,
         SUM(VOICE_OUT_B_F) AS VOICE_OUT_B_F,
         SUM(VOICE_OUT_MEZH) AS VOICE_OUT_MEZH,
         SUM(VOICE_TOTAL) AS VOICE_TOTAL,
         SUM(DATA_VOLUME_MB) AS DATA_VOLUME_MB,
         SUM(REVENUE_VOICE) AS REVENUE_VOICE,
         SUM(REVENUE_GPRS) AS REVENUE_GPRS,
         SUM(REVENUE_OTHER_P) AS REVENUE_OTHER_P,
         SUM(REVENUE_O_R_P) AS REVENUE_O_R_P,
         SUM(UNIQ_GUEST_AMT) AS UNIQ_GUEST_AMT,
         SUM(GUEST_DATA_USERS_AMT) AS GUEST_DATA_USERS_AMT,
         SUM(GUEST_VOICE_DURATION_MIN) AS GUEST_VOICE_DURATION_MIN,
         SUM(GUEST_DATA_VOLUME_MB) AS GUEST_DATA_VOLUME_MB,
         SUM(GUEST_CHARGE_AMT_USD) AS GUEST_CHARGE_AMT_USD,
         SUM(GUEST_CHARGE_AMT) AS GUEST_CHARGE_AMT,
         SUM(REVENUE_VOICE) + SUM(REVENUE_GPRS) +
         SUM(REVENUE_OTHER_P) + SUM(REVENUE_O_R_P) AS TOTAL_REVENUE
          FROM (
          SELECT NVL(A.LAC, 0) AS LAC,
                 NVL(A.CELL, 0) AS CELL,
                 A.TOTAL_SUBSCRIBER_AMT,
                 A.TOTAL_DATA_USERS_AMT,
                 A.VOICE_IN_B_L,
                 A.VOICE_IN_B_F,
                 A.VOICE_IN_MEZH,
                 A.VOICE_OUT_B_L,
                 A.VOICE_OUT_B_F,
                 A.VOICE_OUT_MEZH,
                 A.VOICE_TOTAL,
                 A.DATA_VOLUME_MB,
                 A.REVENUE_VOICE,
                 A.REVENUE_GPRS,
                 A.REVENUE_OTHER_P,
                 A.REVENUE_O_R_P,
                 0 AS UNIQ_GUEST_AMT,
                 0 AS GUEST_DATA_USERS_AMT,
                 0 AS GUEST_VOICE_DURATION_MIN,
                 0 AS GUEST_DATA_VOLUME_MB,
                 0 AS GUEST_CHARGE_AMT_USD,
                 0 AS GUEST_CHARGE_AMT
            FROM lac_cell_prep_stg A
          UNION ALL
          SELECT NVL(B.LAC, 0) AS LAC,
                 NVL(B.CELL, 0) AS CELL,
                 B.TOTAL_SUBSCRIBER_AMT,
                 B.TOTAL_DATA_USERS_AMT,
                 B.VOICE_IN_B_L,
                 B.VOICE_IN_B_F,
                 B.VOICE_IN_MEZH,
                 B.VOICE_OUT_B_L,
                 B.VOICE_OUT_B_F,
                 B.VOICE_OUT_MEZH,
                 B.VOICE_TOTAL,
                 B.DATA_VOLUME_MB,
                 0 AS REVENUE_VOICE,
                 0 AS REVENUE_GPRS,
                 0 AS REVENUE_OTHER_P,
                 B.REVENUE_P AS REVENUE_O_R_P,
                 0 AS UNIQ_GUEST_AMT,
                 0 AS GUEST_DATA_USERS_AMT,
                 0 AS GUEST_VOICE_DURATION_MIN,
                 0 AS GUEST_DATA_VOLUME_MB,
                 0 AS GUEST_CHARGE_AMT_USD,
                 0 AS GUEST_CHARGE_AMT
            FROM lac_cell_post_stg B
          UNION ALL
          SELECT C.LAC,
                 C.CELL,
                 0 AS TOTAL_SUBSCRIBER_AMT,
                 0 AS TOTAL_DATA_USERS_AMT,
                 0 AS VOICE_IN_B_L,
                 0 AS VOICE_IN_B_F,
                 0 AS VOICE_IN_MEZH,
                 0 AS VOICE_OUT_B_L,
                 0 AS VOICE_OUT_B_F,
                 0 AS VOICE_OUT_MEZH,
                 0 AS VOICE_TOTAL,
                 0 AS DATA_VOLUME_MB,
                 0 AS REVENUE_VOICE,
                 0 AS REVENUE_GPRS,
                 0 AS REVENUE_OTHER_P,
                 0 AS REVENUE_O_R_P,
                 C.UNIQ_GUEST_AMT,
                 C.GUEST_DATA_USERS_AMT,
                 C.VOICE_DURATION_MIN   AS GUEST_VOICE_DURATION_MIN,
                 C.DATA_VOLUME_MB       AS GUEST_DATA_VOLUME_MB,
                 C.CHARGE_AMT_USD       AS GUEST_CHARGE_AMT_USD,
                 C.CHARGE_AMT           AS GUEST_CHARGE_AMT
            FROM out_roam_cell C)
         GROUP BY LAC, CELL
""")

    map_rep.createOrReplaceTempView ("map_rep")

    val map_kpi_cell = spark.sql ("""
    SELECT A.*,
           CASE
             WHEN A.TOTAL_SUBSCRIBER_AMT > 0 THEN
              A.TOTAL_REVENUE / A.TOTAL_SUBSCRIBER_AMT
             ELSE
              0
           END AS ARPU,
           CASE
             WHEN A.TOTAL_DATA_USERS_AMT > 0 THEN
              A.REVENUE_GPRS / A.TOTAL_DATA_USERS_AMT
             ELSE
              0
           END AS ARPU_DATA,
           CASE
             WHEN A.TOTAL_SUBSCRIBER_AMT > 0 THEN
              A.VOICE_TOTAL / A.TOTAL_SUBSCRIBER_AMT
             ELSE
              0
           END AS MOU,
           CASE
             WHEN A.TOTAL_DATA_USERS_AMT > 0 THEN
              A.DATA_VOLUME_MB / A.TOTAL_DATA_USERS_AMT
             ELSE
              0
           END AS MBOU,
           CASE
             WHEN B.TYPE = '2G' THEN
              (((A.VOICE_TOTAL * 60 * (2 * (16 * 0.25 + 8 * 0.75))) /
              (8 * (1024 * 1024))) * 1024) + A.DATA_VOLUME_MB
             WHEN B.TYPE = '3G' THEN
              (((A.VOICE_TOTAL * 60 * 21.4) / (8 * (1024 * 1024))) * 1024) +
              A.DATA_VOLUME_MB
             WHEN B.TYPE = '4G' THEN
              A.DATA_VOLUME_MB
             ELSE
              0
           END AS GRPS_VOICE_MB,
           0 AS INTERCONNECT_EXPENCES,
           0 AS INTERCONNECT_REVENUE,
           0 AS INTERCONNECT_R,
           0 AS GUEST_ROAMING_R_PL,
           0 AS GUEST_ROAMING_MARGIN
      FROM map_rep A
      LEFT JOIN dim_map_cell B
        ON A.LAC + 0 = B.LAC + 0
       AND A.CELL + 0 = B.CELL + 0
""")
    map_kpi_cell.write.mode ("overwrite").parquet (map_kpi_path + tools.patternToDate (calc_date, "yyyyMM") )

    // Step 4
    val site_user_df = spark.sql ("""
    SELECT d.SUBS_KEY,
           c.SITE_ID,
           MAX(CASE
                 WHEN D.CALL_TYPE_CODE = 'G' AND D.ROUNDED_DATA_VOLUME > 0 THEN
                  1
                 ELSE
                  0
               END) AS GRPS_IND,
           MAX(CASE
                 WHEN C.TYPE = '3G' AND
                      (D.CALL_TYPE_CODE = 'V' AND D.ACTUAL_CALL_DURATION_SEC > 0 OR
                      D.CALL_TYPE_CODE = 'G' AND D.ROUNDED_DATA_VOLUME > 0) THEN
                  1
                 ELSE
                  0
               END) AS IND_3G
      from det_df d
      join dim_map_cell c
        on CAST(d.LAC AS LONG) = CAST(c.LAC AS LONG)
       and CAST(d.CELL_ID AS LONG) = CAST(c.CELL AS LONG)
     where d.SRC in ('CHR', 'POST', 'CHA', 'PSA', 'GPRS', 'ROAM')
     group by d.SUBS_KEY, c.SITE_ID
""")

    site_user_df.createOrReplaceTempView ("site_user_df")

    val site_geo = spark.sql ("""
    SELECT DISTINCT
      1 AS ID_OBJECT_TYPE,
      1 AS ID_SUBOBJECT_TYPE,
      A.SITE_ID AS ID_OBJECT,
      A.SITE_ID
    FROM dim_map_cell A
    UNION ALL
    SELECT DISTINCT
      1 AS ID_OBJECT_TYPE,
      4 AS ID_SUBOBJECT_TYPE,
      A.LOCATION_ID AS ID_OBJECT,
      A.SITE_ID
    FROM dim_map_cell A
    WHERE A.LOCATION_ID IS NOT NULL
    UNION ALL
    SELECT DISTINCT
      1 AS ID_OBJECT_TYPE,
      3 AS ID_SUBOBJECT_TYPE,
      A.DISTRICT_ID AS ID_OBJECT,
      A.SITE_ID
    FROM dim_map_cell A
    WHERE A.DISTRICT_ID IS NOT NULL
    UNION ALL
    SELECT DISTINCT
      1 AS ID_OBJECT_TYPE,
      2 AS ID_SUBOBJECT_TYPE,
      A.REGION_ID AS ID_OBJECT,
      A.SITE_ID
    FROM dim_map_cell A
    WHERE A.REGION_ID IS NOT NULL
    UNION ALL
    SELECT DISTINCT
      1 AS ID_OBJECT_TYPE,
      6 AS ID_SUBOBJECT_TYPE,
      1 AS ID_OBJECT,
      A.SITE_ID
    FROM dim_map_cell A
""")
    site_geo.write.mode ("overwrite").parquet ("/dmp/beemap/temp/site_geo/" + tools.patternToDate (calc_date, "yyyyMM") )
    site_geo.createOrReplaceTempView ("site_geo")

    val users_geo = spark.sql ("""
  select B.ID_OBJECT_TYPE,
         B.ID_SUBOBJECT_TYPE,
         B.ID_OBJECT,
         COUNT(DISTINCT A.SUBS_KEY) AS TOTAL_SUBSCRIBER_AMT,
         COUNT(DISTINCT(CASE
                          WHEN A.GRPS_IND > 0 THEN
                           A.SUBS_KEY
                          ELSE
                           NULL
                        END)) AS TOTAL_DATA_USERS_AMT,
         COUNT(DISTINCT(CASE
                          WHEN A.IND_3G > 0 THEN
                           A.SUBS_KEY
                          ELSE
                           NULL
                        END)) AS TOTAL_3G_USERS_AMT
    from site_user_df A
    join site_geo B
      on A.SITE_ID = B.SITE_ID
   GROUP BY B.ID_OBJECT_TYPE, B.ID_SUBOBJECT_TYPE, B.ID_OBJECT
""").cache

    users_geo.write.mode ("overwrite").parquet ("/dmp/beemap/temp/users_geo/" + tools.patternToDate (calc_date, "yyyyMM") )

    spark.read.parquet ("/dmp/beemap/temp/map_kpi_cell/" + tools.patternToDate (calc_date, "yyyyMM") ).createOrReplaceTempView ("map_kpi_cell")
    spark.read.parquet ("/dmp/beemap/temp/dim_map_cell/" + tools.patternToDate (calc_date, "yyyyMM") ).createOrReplaceTempView ("dim_map_cell")
    spark.read.parquet ("/dmp/beemap/temp/users_geo/" + tools.patternToDate (calc_date, "yyyyMM") ).createOrReplaceTempView ("users_geo")

    val kpi_stations = spark.sql ("""
    SELECT C.*,
           NVL(E.TOTAL_SUBSCRIBER_AMT, 0) AS TOTAL_SUBSCRIBER_AMT,
           NVL(E.TOTAL_DATA_USERS_AMT, 0) AS TOTAL_DATA_USERS_AMT,
           CASE
             WHEN NVL(E.TOTAL_SUBSCRIBER_AMT, 0) > 0 THEN
              C.TOTAL_REVENUE / NVL(E.TOTAL_SUBSCRIBER_AMT, 0)
             ELSE
              0
           END AS ARPU,
           CASE
             WHEN NVL(E.TOTAL_DATA_USERS_AMT, 0) > 0 THEN
              C.REVENUE_GPRS / NVL(E.TOTAL_DATA_USERS_AMT, 0)
             ELSE
              0
           END AS ARPU_DATA,
           CASE
             WHEN NVL(E.TOTAL_SUBSCRIBER_AMT, 0) > 0 THEN
              C.VOICE_TOTAL / NVL(E.TOTAL_SUBSCRIBER_AMT, 0)
             ELSE
              0
           END AS MOU,
           CASE
             WHEN NVL(E.TOTAL_DATA_USERS_AMT, 0) > 0 THEN
              C.DATA_VOLUME_MB / NVL(E.TOTAL_DATA_USERS_AMT, 0)
             ELSE
              0
           END AS MBOU
      FROM (SELECT NVL(D.SITE_ID, 0) AS SITE_ID,
                   SUM(A.VOICE_IN_B_L) AS VOICE_IN_B_L,
                   SUM(A.VOICE_IN_B_F) AS VOICE_IN_B_F,
                   SUM(A.VOICE_IN_MEZH) AS VOICE_IN_MEZH,
                   SUM(A.VOICE_OUT_B_L) AS VOICE_OUT_B_L,
                   SUM(A.VOICE_OUT_B_F) AS VOICE_OUT_B_F,
                   SUM(A.VOICE_OUT_MEZH) AS VOICE_OUT_MEZH,
                   SUM(A.VOICE_TOTAL) AS VOICE_TOTAL,
                   SUM(A.DATA_VOLUME_MB) AS DATA_VOLUME_MB,
                   SUM(A.REVENUE_VOICE) AS REVENUE_VOICE,
                   SUM(A.REVENUE_GPRS) AS REVENUE_GPRS,
                   SUM(A.REVENUE_OTHER_P) AS REVENUE_OTHER_P,
                   SUM(A.REVENUE_O_R_P) AS REVENUE_O_R_P,
                   SUM(A.GUEST_VOICE_DURATION_MIN) AS GUEST_VOICE_DURATION_MIN,
                   SUM(A.GUEST_DATA_VOLUME_MB) AS GUEST_DATA_VOLUME_MB,
                   SUM(A.GUEST_CHARGE_AMT_USD) AS GUEST_CHARGE_AMT_USD,
                   SUM(A.GUEST_CHARGE_AMT) AS GUEST_CHARGE_AMT,
                   SUM(A.TOTAL_REVENUE) AS TOTAL_REVENUE,
                   SUM(A.GRPS_VOICE_MB) AS GRPS_VOICE_MB,
                   SUM(A.INTERCONNECT_EXPENCES) AS INTERCONNECT_EXPENCES,
                   SUM(A.INTERCONNECT_REVENUE) AS INTERCONNECT_REVENUE,
                   SUM(A.INTERCONNECT_R) AS INTERCONNECT_R,
                   SUM(A.GUEST_ROAMING_R_PL) AS GUEST_ROAMING_R_PL,
                   SUM(A.GUEST_ROAMING_MARGIN) AS GUEST_ROAMING_MARGIN,
                   0 AS RENT_SUM
              FROM map_kpi_cell A
              LEFT JOIN dim_map_cell D
                ON A.CELL = D.CELL
               AND A.LAC = D.LAC
             GROUP BY NVL(D.SITE_ID, 0)) C
          LEFT JOIN (SELECT T.ID_OBJECT SITE_ID,
                        T.TOTAL_SUBSCRIBER_AMT,
                        T.TOTAL_DATA_USERS_AMT
                   FROM users_geo T
                  WHERE T.ID_SUBOBJECT_TYPE = 1) E
          ON C.SITE_ID = E.SITE_ID
        """)

    kpi_stations.write.mode("overwrite").parquet(kpi_stat_path + tools.patternToDate (calc_date, "yyyyMM"))

    // Work & home locations
    val loc = spark.read.parquet("/dmp/monthly_stg/agg_subs_location_prior_hw_cell_monthly/"+tools.patternToDate(calc_date,"MMyyyy"))
    val dmc = spark.read.parquet("/dmp/beemap/temp/dim_map_cell/"+tools.patternToDate(calc_date,"yyyyMM"))

    loc.join(dmc,loc("lac")===dmc("lac") && loc("cell_id")===dmc("cell"))
      .groupBy(dmc("site_id"),loc("home_work_ind"))
      .agg(countDistinct(loc("subs_key")).as("count"))
      .filter($"home_work_ind".isin(1,2))
      .groupBy($"site_id")
      .agg(sum(when($"home_work_ind"===1,$"count").otherwise(0)).as("work_count"),
        sum(when($"home_work_ind"===2,$"count").otherwise(0)).as("home_count"))
      .write.mode("overwrite").parquet("/dmp/beemap/temp/hw_location/"+tools.patternToDate (calc_date, "yyyyMM"))

    spark.close()
  } catch {
    case e: Exception => println (s"ERROR: $e")
    status = false
    if (spark != null) spark.close ()
  } finally {}
    status
  }
}