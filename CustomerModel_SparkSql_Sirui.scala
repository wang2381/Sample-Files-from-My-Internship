

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object newuser_Sirui {
  def main(args: Array[String]): Unit = {
    println("program starting")

    def deletePath(hdfs: FileSystem, outputpath: String) = {
      val output_path = new Path(outputpath)
      if (hdfs.exists(output_path)) hdfs.delete(output_path, true)
    }



    val base = "hdfs://nameservice1"

    val sc = new SparkContext(new SparkConf().setAppName("Sirui"))

    val spark = SparkSession.builder().appName("Sirui")
      .enableHiveSupport()
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.broadcastTimeout", "20000")
      .config("spark.driver.maxResultSize", "25g")
      .config("spark.sql.shuffle.partitions", "600")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.shuffle.file.buffer.kb", "10240")
      .config("spark.storage.memoryFraction", "0.2")
      .config("spark.shuffle.memoryFraction", "0.6")
      .config("spark.sql.crossJoin.enabled", "true").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(base), new org.apache.hadoop.conf.Configuration())


    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormatnew: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd.HHmmss")
    val timePos = dateFormatnew.format(new Date()).toString
    println("start=============" + dateFormat.format(new Date()))


    var now = System.nanoTime

    println("start time is "+ now)

    val outputPath = "/user/stevenLiu/"+"Sirui" + timePos +"data"
    val headerPath = "/user/stevenLiu/" + "Sirui" + timePos +"header"

    deletePath(hdfs, outputPath)

    val bi_Sirui_newisntall_0807=spark.sql(
      """
        select
  t.*
  from
  (select
  a.site_id,

  device_id,
  a.customer_user_id cookie_id,
  from_unixtime(unix_timestamp(a.install_time),'yyyyMMdd') data_installed,
  a.channel_name channel,
  a.media_source  source,
  a.medium,
  a.campaign,
  a.platform,
  a.country_code country,
  a.area,
  a.city,
  a.ip,
  a.device_brand,
  a.device_model,
  a.os_version,
  row_number() over(partition by a.customer_user_id,a.site_id order by  a.install_time) rank
  from  dpr.dpr_install_log a
  where a.is_dirty = 0
  and a.country_code in ('OM','BH','SA','LB','JO','KW','AE','QA')
  and   from_unixtime(unix_timestamp(a.install_time),'yyyyMMdd')>='20180601'
  and   from_unixtime(unix_timestamp(a.install_time),'yyyyMMdd')<='20180801'
  and  a.site_id in (600,900))t
  where t.rank=1

      """.stripMargin)




    bi_Sirui_newisntall_0807.createOrReplaceTempView("bi_Sirui_newisntall_0807")


    val bi_Sirui_newisntall_cookieid=spark.sql("select  b.cookie_id ,b.site_id from bi_Sirui_newisntall_0807  b " +
      "group by b.cookie_id ,b.site_id")

    bi_Sirui_newisntall_cookieid.createOrReplaceTempView("bi_Sirui_newisntall_cookieid")



    /* val bi_Sirui_eventlog_0807=spark.sql(
       """
          SELECT a.*
          FROM
            (SELECT a.data_date,
                    a.siteid,
                    a.cookieid,
                    count(DISTINCT CASE
                                       WHEN a.eventkey='key_addtobag_result'
                                            AND RESULT='success' THEN a.eventid
                                   END) cart_num,
                    count(DISTINCT CASE
                                       WHEN a.eventkey IN ('search_hotsearch_click','search_searchhistory_click','search_click','search_hint_click') THEN a.eventid
                                   END) search_num,
                    count(DISTINCT a.eventid) event_num,
                    count(DISTINCT CASE
                                       WHEN a.eventkey='goodsdetail_view' THEN a.eventid
                                   END) detail_num
             FROM ods.ods_event_log a
             WHERE a.data_date>='20180501'
               AND a.data_date<='20180805'
               AND a.siteid IN (600,
                                900)
             GROUP BY a.data_date,
                      a.siteid,
                      a.cookieid)a
       """.stripMargin) */

    val bi_Sirui_eventlog_0807=spark.sql("select * from   zybiro.bi_stevenLiu_test where data_date>='20180501'" +
      "and data_date<='20180805'")


    bi_Sirui_eventlog_0807.createOrReplaceTempView("bi_Sirui_eventlog_0807")







    val bi_Sirui_eventlog_cookie=spark.sql("""select
                                           a.*
                                           from
                                            bi_Sirui_eventlog_0807 a
                                           join
                                            bi_Sirui_newisntall_cookieid  b on a.cookieid=b.cookie_id  and b.site_id=a.siteid""")
    bi_Sirui_eventlog_cookie.createOrReplaceTempView("bi_Sirui_eventlog_cookie")


    val bi_Sirui_cat_0807=spark.sql(
      """
        select
  a.site_id
  ,a.data_date
  ,a.cookie_id
  ,a.1st_cat
  ,a.goods_id
  from  dw.dw_cookie_dau_goods_relation  a
  where a.data_date>='20180501'
  and a.data_date<='20180805'
  and a.site_id in (600,900)
  and a.is_visit=1
  group by a.data_date,a.site_id,a.cookie_id,a.1st_cat,a.goods_id
      """.stripMargin)

    bi_Sirui_cat_0807.createOrReplaceTempView("bi_Sirui_cat_0807")










    val bi_Sirui_cat_cookie=spark.sql(
      """
         select
         a.*
         from
          bi_Sirui_cat_0807 a
         join
          bi_Sirui_newisntall_cookieid  b on a.cookie_id=b.cookie_id  and b.site_id=a.site_id
      """.stripMargin)

    bi_Sirui_cat_cookie.createOrReplaceTempView("bi_Sirui_cat_cookie")








    val bi_Sirui_dau_visit_0807=spark.sql(
      """
         select
          a.data_date,
          a.site_id,
          a.cookie_id,
          page_views,
          visit_time,
          a.is_reg,
          a.is_paid,
          a.paid_orders,
          a.paid_order_amount
          from
          dw.dw_cookie_dau_visit a
          where a.data_date>='20180501'
          and a.data_date<='20180805'
          and a.site_id in (600,900)
      """.stripMargin)

    bi_Sirui_dau_visit_0807.createOrReplaceTempView("bi_Sirui_dau_visit_0807")








    val bi_Sirui_dau_visit_cookie=spark.sql(
      """
         select
         a.*
         from
          bi_Sirui_dau_visit_0807 a
         join
          bi_Sirui_newisntall_cookieid  b on a.cookie_id=b.cookie_id  and b.site_id=a.site_id
      """.stripMargin)

    bi_Sirui_dau_visit_cookie.createOrReplaceTempView("bi_Sirui_dau_visit_cookie")











    val bi_Sirui_newinsatall_res_0807_c=spark.sql(
      """
         select
         a.site_id,
         a.device_id,
         a.cookie_id,
         a.data_installed,
         a.channel,
         a.source,
         a.medium,
         a.campaign,
         a.platform,
         a.country,
         a.area,
         a.city,
         a.ip,
         a.device_brand,
         a.device_model,
         a.os_version,
         case when b.cookie_id is null or  a.data_installed<=from_unixtime(unix_timestamp(b.first_install_time),'yyyyMMdd')  then 0 else 1 end  is_his_install,

         sum(case when c.data_date=a.data_installed then c.cart_num else 0 end ) day1_cart_num,
         sum(case when c.data_date=a.data_installed then c.search_num else 0 end ) day1_search_num,
         sum(case when c.data_date=a.data_installed then c.event_num else 0 end ) day1_event_num,
         sum(case when c.data_date=a.data_installed then c.detail_num else 0 end ) day1_detail_num,

         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then c.cart_num else 0 end ) day2_cart_num,
         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then c.search_num else 0 end ) day2_search_num,
         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then c.event_num else 0 end ) day2_event_num,
         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then c.detail_num else 0 end ) day2_detail_num,

         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then c.cart_num else 0 end ) day3_cart_num,
         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then c.search_num else 0 end ) day3_search_num,
         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then c.event_num else 0 end ) day3_event_num,
         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then c.detail_num else 0 end ) day3_detail_num,

         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then c.cart_num else 0 end ) day4_cart_num,
         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then c.search_num else 0 end ) day4_search_num,
         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then c.event_num else 0 end ) day4_event_num,
         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then c.detail_num else 0 end ) day4_detail_num,


         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
         and  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2
         then c.cart_num else 0 end ) day1_3_cart_num,

         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
         and    datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2
          then c.search_num else 0 end ) day1_3_search_num,

         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
         and    datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2  then c.event_num else 0 end ) day1_3_event_num,

         sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
         and   datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2  then c.detail_num else 0 end ) day1_3_detail_num

         from
          bi_Sirui_newisntall_0807  a
         left join
         dw.dw_cookie_first_install b on  a.site_id=b.site_id  and a.cookie_id=b.cookie_id
         left join
          bi_Sirui_eventlog_cookie c on  a.site_id=c.siteid  and a.cookie_id=c.cookieid
         group  by
         a.site_id,
         a.device_id,
         a.cookie_id,
         a.data_installed,
         a.channel,
         a.source,
         a.medium,
         a.campaign,
         a.platform,
         a.country,
         a.area,
         a.city,
         a.ip,
         a.device_brand,
         a.device_model,
         a.os_version,
         case when b.cookie_id is null or  a.data_installed<=from_unixtime(unix_timestamp(b.first_install_time),'yyyyMMdd')  then 0 else 1 end
      """.stripMargin)


    bi_Sirui_newinsatall_res_0807_c.createOrReplaceTempView("bi_Sirui_newinsatall_res_0807_c")



println("aaaaaaajjjjjjjjjjjk")


    val bi_Sirui_newinsatall_res_0807_e=spark.sql(
      """
        select
  a.site_id,
  a.device_id,
  a.cookie_id,
  a.data_installed,
  a.channel,
  a.source,
  a.medium,
  a.campaign,
  a.platform,
  a.country,
  a.area,
  a.city,
  a.ip,
  a.device_brand,
  a.device_model,
  a.os_version,
  sum(case when e.data_date=a.data_installed then e.visit_time else 0 end ) day1_time,
  sum(case when e.data_date=a.data_installed then e.page_views else 0 end ) day1_pv,
  sum(case when e.data_date=a.data_installed  and e.is_reg=1 then 1 else 0 end ) day1_is_reg,
  sum(case when e.data_date=a.data_installed then e.paid_orders else 0 end ) day1_order_num,
  sum(case when e.data_date=a.data_installed then e.paid_order_amount else 0 end ) day1_order_value,

  sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then e.visit_time else 0 end ) day2_time,
  sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then e.page_views else 0 end ) day2_pv,
  sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  and e.is_reg=1 then 1 else 0 end ) day2_is_reg,
  sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1 then e.paid_orders else 0 end ) day2_order_num,
  sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then e.paid_order_amount else 0 end ) day2_order_value,


  sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then e.visit_time else 0 end ) day3_time,
  sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then e.page_views else 0 end ) day3_pv,
  sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  and e.is_reg=1 then 1 else 0 end ) day3_is_reg,
  sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2 then e.paid_orders else 0 end ) day3_order_num,
  sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then e.paid_order_amount else 0 end ) day3_order_value,


  sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then e.visit_time else 0 end ) day4_time,
  sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then e.page_views else 0 end ) day4_pv,
  sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  and e.is_reg=1 then 1 else 0 end ) day4_is_reg,
  sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3 then e.paid_orders else 0 end ) day4_order_num,
  sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then e.paid_order_amount else 0 end ) day4_order_value,



  sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
  and   datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2
  then e.visit_time else 0 end ) day1_3_time,

  sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
  and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2
   then e.page_views else 0 end ) day1_3_pv,

  max(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
  and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2
  and e.is_reg=1 then 1 else 0 end ) day1_3_is_reg,


  sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
  and datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2
  then e.paid_orders else 0 end ) day1_3_order_num,

  sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
  and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2  then e.paid_order_amount else 0 end ) day1_3_order_value,


  max(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
  and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6
  and e.cookie_id is not null then 1 else 0 end ) active_in_5_7


  from
   bi_Sirui_newisntall_0807  a
  left join
   bi_Sirui_dau_visit_cookie e  on    a.site_id=e.site_id  and a.cookie_id=e.cookie_id
  group  by
  a.site_id,
  a.device_id,
  a.cookie_id,
  a.data_installed,
  a.channel,
  a.source,
  a.medium,
  a.campaign,
  a.platform,
  a.country,
  a.area,
  a.city,
  a.ip,
  a.device_brand,
  a.device_model,
  a.os_version
      """.stripMargin)

    bi_Sirui_newinsatall_res_0807_e.createOrReplaceTempView("bi_Sirui_newinsatall_res_0807_e")





    val bi_Sirui_newinsatall_res_0807_d=spark.sql(
      """
         select
  a.site_id,
  a.device_id,
  a.cookie_id,
  a.data_installed,
  a.channel,
  a.source,
  a.medium,
  a.campaign,
  a.platform,
  a.country,
  a.area,
  a.city,
  a.ip,
  a.device_brand,
  a.device_model,
  a.os_version,
  count(distinct case when d.data_date=a.data_installed then d.goods_id end ) day1_prod_num,
  count(distinct case when d.data_date=a.data_installed then d.1st_cat end ) day1_cat1_num,

  count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then d.goods_id end ) day2_prod_num,
  count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then d.1st_cat end ) day2_cat1_num,


  count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then d.goods_id end ) day3_prod_num,
  count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then d.1st_cat end ) day3_cat1_num,

  count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then d.goods_id end ) day4_prod_num,
  count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then d.1st_cat end ) day4_cat1_num,



  count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
  and   datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2 then d.goods_id end ) day1_3_prod_num,

  count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
  and   datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2  then d.1st_cat end ) day1_3_cat1_num


  from
   bi_Sirui_newisntall_0807  a
  left join
    bi_Sirui_cat_cookie d  on  a.site_id=d.site_id  and a.cookie_id=d.cookie_id
  group  by
  a.site_id,
  a.device_id,
  a.cookie_id,
  a.data_installed,
  a.channel,
  a.source,
  a.medium,
  a.campaign,
  a.platform,
  a.country,
  a.area,
  a.city,
  a.ip,
  a.device_brand,
  a.device_model,
  a.os_version
      """.stripMargin)

    bi_Sirui_newinsatall_res_0807_d.createOrReplaceTempView("bi_Sirui_newinsatall_res_0807_d")





    val bi_Sirui_newinsatall_res_0807=spark.sql(
      """
         select
  a.site_id,
  a.device_id,
  a.cookie_id,
  a.data_installed,
  a.channel,
  a.source,
  a.medium,
  a.campaign,
  a.platform,
  a.country,
  a.area,
  a.city,
  a.ip,
  a.device_brand,
  a.device_model,
  a.os_version,
  a.is_his_install,
  a.day1_cart_num,
  a.day1_search_num,
  a.day1_event_num,
  a.day1_detail_num,
  a.day2_cart_num,
  a.day2_search_num,
  a.day2_event_num,
  a.day2_detail_num,
  a.day3_cart_num,
  a.day3_search_num,
  a.day3_event_num,
  a.day3_detail_num,
  a.day4_cart_num,
  a.day4_search_num,
  a.day4_event_num,
  a.day4_detail_num,
  a.day1_3_cart_num,
  a.day1_3_search_num,
  a.day1_3_event_num,
  a.day1_3_detail_num,
  b.day1_time,
  b.day1_pv,
  b.day1_is_reg,
  b.day1_order_num,
  b.day1_order_value,
  b.day2_time,
  b.day2_pv,
  b.day2_is_reg,
  b.day2_order_num,
  b.day2_order_value,
  b.day3_time,
  b.day3_pv,
  b.day3_is_reg,
  b.day3_order_num,
  b.day3_order_value,
  b.day4_time,
  b.day4_pv,
  b.day4_is_reg,
  b.day4_order_num,
  b.day4_order_value,
  b.day1_3_time,
  b.day1_3_pv,
  b.day1_3_is_reg,
  b.day1_3_order_num,
  b.day1_3_order_value,
  b.active_in_5_7,
  c.day1_prod_num,
  c.day1_cat1_num,
  c.day2_prod_num,
  c.day2_cat1_num,
  c.day3_prod_num,
  c.day3_cat1_num,
  c.day4_prod_num,
  c.day4_cat1_num,
  c.day1_3_prod_num,
  c.day1_3_cat1_num
  from
   bi_Sirui_newinsatall_res_0807_c  a
  left join
   bi_Sirui_newinsatall_res_0807_e b   on
    a.site_id=b.site_id
  and  a.cookie_id=b.cookie_id
  left join
   bi_Sirui_newinsatall_res_0807_d c  on
    a.site_id=c.site_id
  and a.cookie_id=c.cookie_id
      """.stripMargin
    )
      bi_Sirui_newinsatall_res_0807.createOrReplaceTempView("bi_Sirui_newinsatall_res_0807")







    val bi_Sirui_newinsatall_outflow_res0807=spark.sql(
      """
         select
  nvl(a.site_id,0) site_id,
 nvl(a.device_id,0) device_id,
  nvl(a.cookie_id,0) cookie_id,
   nvl(a.data_installed,0) data_installed,
   nvl(a.channel,0) channel,
   nvl(a.source,0) source,
   nvl(a.medium,0) medium,
   nvl(a.campaign,0) campaign,
   nvl(a.platform,0) platform,
   nvl(a.country,0) country,
   nvl(a.area,0) area,
   nvl(a.city,0) city,
   nvl(a.ip,0) ip,
   nvl(a.device_brand,0) device_brand,
   nvl(a.device_model,0) device_model,
   nvl(a.os_version,0) os_version,
   nvl(a.is_his_install,0) is_his_install,
  nvl(day1_time,0) day1_time,
  nvl(day1_pv,0) day1_pv,
  nvl(day1_is_reg,0) day1_is_reg,
  nvl(day1_cart_num,0) day1_cart_num,
  nvl(day1_search_num,0) day1_search_num,
  nvl(day1_event_num,0) day1_event_num,
  nvl(day1_detail_num,0) day1_detail_num,
  nvl(day1_prod_num,0) day1_prod_num,
  nvl(day1_cat1_num,0) day1_cat1_num,
  nvl(day1_order_num,0) day1_order_num,
  nvl(day1_order_value,0) day1_order_value,
  nvl(day2_time,0) day2_time,
  nvl(day2_pv,0) day2_pv,
  nvl(day2_is_reg,0) day2_is_reg,
  nvl(day2_cart_num,0) day2_cart_num,
  nvl(day2_search_num,0) day2_search_num,
  nvl(day2_event_num,0) day2_event_num,
  nvl(day2_detail_num,0) day2_detail_num,
  nvl(day2_prod_num,0) day2_prod_num,
  nvl(day2_cat1_num,0) day2_cat1_num,
  nvl(day2_order_num,0) day2_order_num,
  nvl(day2_order_value,0) day2_order_value,
  nvl(day3_time,0) day3_time,
  nvl(day3_pv,0) day3_pv,
  nvl(day3_is_reg,0) day3_is_reg,
  nvl(day3_cart_num,0) day3_cart_num,
  nvl(day3_search_num,0) day3_search_num,
  nvl(day3_event_num,0) day3_event_num,
  nvl(day3_detail_num,0) day3_detail_num,
  nvl(day3_prod_num,0) day3_prod_num,
  nvl(day3_cat1_num,0) day3_cat1_num,
  nvl(day3_order_num,0) day3_order_num,
  nvl(day3_order_value,0) day3_order_value,
  nvl(day4_time,0) day4_time,
  nvl(day4_pv,0) day4_pv,
  nvl(day4_is_reg,0) day4_is_reg,
  nvl(day4_cart_num,0) day4_cart_num,
  nvl(day4_search_num,0) day4_search_num,
  nvl(day4_event_num,0) day4_event_num,
  nvl(day4_detail_num,0) day4_detail_num,
  nvl(day4_prod_num,0) day4_prod_num,
  nvl(day4_cat1_num,0) day4_cat1_num,
  nvl(day4_order_num,0) day4_order_num,
  nvl(day4_order_value,0) day4_order_value,
  nvl(day1_3_time,0) day1_3_time,
  nvl(day1_3_pv,0) day1_3_pv,
  nvl(day1_3_is_reg,0) day1_3_is_reg,
  nvl(day1_3_cart_num,0) day1_3_cart_num,
  nvl(day1_3_search_num,0) day1_3_search_num,
  nvl(day1_3_event_num,0) day1_3_event_num,
  nvl(day1_3_detail_num,0) day1_3_detail_num,
  nvl(day1_3_prod_num,0) day1_3_prod_num,
  nvl(day1_3_cat1_num,0) day1_3_cat1_num,
  nvl(day1_3_order_num,0) day1_3_order_num,
  nvl(day1_3_order_value,0) day1_3_order_value,
  nvl(active_in_5_7,0) active_in_5_7
  from
   bi_Sirui_newinsatall_res_0807 a
      """.stripMargin)


    bi_Sirui_newinsatall_outflow_res0807.createOrReplaceTempView("bi_Sirui_newinsatall_outflow_res0807")



val lastone=spark.sql(
  """
    SELECT a.*,
           CASE
              WHEN b.site_id IS NOT NULL
                  AND b.cookie_id IS NOT NULL THEN 1
              ELSE 0
          END AS is_login
    FROM bi_Sirui_newinsatall_outflow_res0807 a
    LEFT JOIN
      (SELECT DISTINCT a.site_id,
                       a.cookie_id
       FROM dw.dw_cookie_user_relation a
       WHERE a.userid>0
         AND a.site_id IN (600,
                           900)) b ON a.site_id=b.site_id
    AND a.cookie_id=b.cookie_id
    WHERE length(a.cookie_id)=36
  """.stripMargin)


lastone.createOrReplaceTempView("lastone")
    /*
    val lastone = spark.sql(
      """
        select a.* , b.userid from bi_Sirui_newinsatall_outflow_res a left join
        dw.dw_cookie_user_relation b on a.cookie_id=b.cookie_id


      """.stripMargin)

    lastone.createOrReplaceTempView("lastone")

*/
    lastone.rdd.map(line =>
      line.toSeq.map(_.toString.replaceAll("\n"," ").replaceAll("\t"," ")).mkString("\t")
    ).repartition(1).saveAsTextFile(outputPath)


println("11123")
    val names=Array(lastone.columns.mkString("\t"))

    sc.parallelize(names).repartition(1).saveAsTextFile(headerPath)


    val timeElapsed = (System.nanoTime - now)/1e9d


    val minute=timeElapsed/60

    println("the total running time is : " + minute+ "minutes")


  }
}
