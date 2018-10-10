------Feature Extraction for Customer Historical Activities


------Customers who had APP installed longer than 90 days

drop  table  zybiro.bi_Sirui_newisntall_0511;
create  table   zybiro.bi_Sirui_newisntall_0511 as
select  
a.site_id,
--nvl(a.idfa,nvl(a.advertising_id,a.android_id)) device_id1,
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
a.os_version
from  dpr.dpr_install_log a 
where a.is_dirty = 0
and   from_unixtime(unix_timestamp(a.install_time),'yyyyMMdd')>='20180101'
and   from_unixtime(unix_timestamp(a.install_time),'yyyyMMdd')<='20180209'
and  a.site_id in (600,900);


drop  table  zybiro.bi_Sirui_newisntall_cookieid;
create  table   zybiro.bi_Sirui_newisntall_cookieid as
select  b.cookie_id ,b.site_id from zybiro.bi_Sirui_newisntall_0511  b 
group by b.cookie_id ,b.site_id;




------Modify 20180101-20180222 Customer Event Table


drop  table  zybiro.bi_Sirui_eventlog_0511;
create  table   zybiro.bi_Sirui_eventlog_0511 as
select 
a.*
from 
(select
a.data_date
,a.siteid
,a.cookieid
,count(distinct case  when a.eventkey='key_addtobag_result' and  result='success' then a.eventid end ) cart_num
,count(distinct case  when a.eventkey in ('search_hotsearch_click','search_searchhistory_click','search_click','search_hint_click') then a.eventid end ) search_num
,count(distinct a.eventid ) event_num
,count(distinct case  when a.eventkey='goodsdetail_view' then a.eventid end ) detail_num
---,count(distinct case when  a.eventkey='goodsdetail_view' then a.goodsid end ) prod_num
from 
ods.ods_event_log a 
where a.data_date>='20180101'
and a.data_date<='20180222'
and a.siteid in (600,900)
group by a.data_date,a.siteid,a.cookieid)a



drop  table  zybiro.bi_Sirui_eventlog_cookie;
create  table   zybiro.bi_Sirui_eventlog_cookie as
select 
a.*
from 
zybiro.bi_Sirui_eventlog_0511 a 
join 
zybiro.bi_Sirui_newisntall_cookieid  b on a.cookieid=b.cookie_id  and b.site_id=a.siteid ;


---How many categories of merchandise have been viewed by the customer in 1 to 3 days


drop  table  zybiro.bi_Sirui_cat_0511;
create  table   zybiro.bi_Sirui_cat_0511 as
select
a.site_id
,a.data_date
,a.cookie_id
,a.1st_cat 
,a.goods_id
from  dw.dw_cookie_dau_goods_relation  a 
where a.data_date>='20180101'
and a.data_date<='20180222'
and a.site_id in (600,900)
and a.is_visit=1
group by a.data_date,a.site_id,a.cookie_id,a.1st_cat,a.goods_id;

 
drop  table  zybiro.bi_Sirui_cat_cookie;
create  table   zybiro.bi_Sirui_cat_cookie as
select 
a.*
from 
zybiro.bi_Sirui_cat_0511 a 
join 
zybiro.bi_Sirui_newisntall_cookieid  b on a.cookie_id=b.cookie_id  and b.site_id=a.site_id ;
 

 
------dau_visi
------
drop  table   zybiro.bi_Sirui_dau_visit_0511;
create  table   zybiro.bi_Sirui_dau_visit_0511 as
select 
 a.data_date,
 a.site_id,
 a.cookie_id,
 page_views,
 visit_time,
 a.reg,
 a.is_paid,
 a.paid_orders, 
 a.paid_order_amount
 from 
 dw.dw_cookie_dau_visit a 
 where a.data_date>='20180101'
 and a.data_date<='20180510'
 and a.site_id in (600,900);
 
 
drop  table  zybiro.bi_Sirui_dau_visit_cookie;
create  table   zybiro.bi_Sirui_dau_visit_cookie as
select 
a.*
from 
zybiro.bi_Sirui_dau_visit_0511 a 
join 
zybiro.bi_Sirui_newisntall_cookieid  b on a.cookie_id=b.cookie_id  and b.site_id=a.site_id ;
 
 
 ---------Due to the size of the data, using 3 sub-tables-----
 -------------------------------
create  table   zybiro.bi_Sirui_newinsatall_res_0512_c as
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
-----day1
sum(case when c.data_date=a.data_installed then c.cart_num else 0 end ) day1_cart_num,
sum(case when c.data_date=a.data_installed then c.search_num else 0 end ) day1_search_num,
sum(case when c.data_date=a.data_installed then c.event_num else 0 end ) day1_event_num,
sum(case when c.data_date=a.data_installed then c.detail_num else 0 end ) day1_detail_num,
-----day2
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then c.cart_num else 0 end ) day2_cart_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then c.search_num else 0 end ) day2_search_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then c.event_num else 0 end ) day2_event_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then c.detail_num else 0 end ) day2_detail_num,
------day3
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then c.cart_num else 0 end ) day3_cart_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then c.search_num else 0 end ) day3_search_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then c.event_num else 0 end ) day3_event_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then c.detail_num else 0 end ) day3_detail_num,
--------day4
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then c.cart_num else 0 end ) day4_cart_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then c.search_num else 0 end ) day4_search_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then c.event_num else 0 end ) day4_event_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then c.detail_num else 0 end ) day4_detail_num,
--------day5
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then c.cart_num else 0 end ) day5_cart_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then c.search_num else 0 end ) day5_search_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then c.event_num else 0 end ) day5_event_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then c.detail_num else 0 end ) day5_detail_num,
--------day6
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then c.cart_num else 0 end ) day6_cart_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then c.search_num else 0 end ) day6_search_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then c.event_num else 0 end ) day6_event_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then c.detail_num else 0 end ) day6_detail_num,
--------day7
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then c.cart_num else 0 end ) day7_cart_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then c.search_num else 0 end ) day7_search_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then c.event_num else 0 end ) day7_event_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then c.detail_num else 0 end ) day7_detail_num,
-----day1-day3
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
and  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2
then c.cart_num else 0 end ) day1_3_cart_num,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
and    datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2
 then c.search_num else 0 end ) day1_3_search_num,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0 
and    datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2  then c.event_num else 0 end ) day1_3_event_num,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0 
and   datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2  then c.detail_num else 0 end ) day1_3_detail_num,
-----day5-day7
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
and    datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6
then c.cart_num else 0 end ) day5_7_cart_num,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
and    datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6
 then c.search_num else 0 end ) day5_7_search_num,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4 
and    datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6  then c.event_num else 0 end ) day5_7_event_num,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4 
and   datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6  then c.detail_num else 0 end ) day5_7_detail_num,

-------True Labels			
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0 
and  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6  then c.event_num else 0 end ) day1_7_event_num,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=7 
and  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=13  then c.event_num else 0 end ) day8_14_event_num

from
zybiro.bi_Sirui_newisntall_0511  a 
left join 
dw.dw_cookie_first_install b on  a.site_id=b.site_id  and a.cookie_id=b.cookie_id 
left join 
zybiro.bi_Sirui_eventlog_cookie c on  a.site_id=c.siteid  and a.cookie_id=c.cookieid
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
case when b.cookie_id is null or  a.data_installed<=from_unixtime(unix_timestamp(b.first_install_time),'yyyyMMdd')  then 0 else 1 end ;

 
 
 
create  table   zybiro.bi_Sirui_newinsatall_res_0512_e as
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


sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then e.visit_time else 0 end ) day5_time,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then e.page_views else 0 end ) day5_pv,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  and e.is_reg=1 then 1 else 0 end ) day5_is_reg,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4 then e.paid_orders else 0 end ) day5_order_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then e.paid_order_amount else 0 end ) day5_order_value,


sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then e.visit_time else 0 end ) day6_time,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then e.page_views else 0 end ) day6_pv,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  and e.is_reg=1 then 1 else 0 end ) day6_is_reg,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5 then e.paid_orders else 0 end ) day6_order_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then e.paid_order_amount else 0 end ) day6_order_value,


sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then e.visit_time else 0 end ) day7_time,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then e.page_views else 0 end ) day7_pv,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  and e.is_reg=1 then 1 else 0 end ) day7_is_reg,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6 then e.paid_orders else 0 end ) day7_order_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then e.paid_order_amount else 0 end ) day7_order_value,


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



sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
and   datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6
then e.visit_time else 0 end ) day5_7_time,

sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6
 then e.page_views else 0 end ) day5_7_pv,
 
max(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4 
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6
and e.is_reg=1 then 1 else 0 end ) day5_7_is_reg,


sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4 
and datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6
then e.paid_orders else 0 end ) day5_7_order_num,

sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6  then e.paid_order_amount else 0 end ) day5_7_order_value,

max(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=7 
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=13
and e.is_paid=1 then 1 else 0 end ) purchased_in_8_14,

max(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=7 
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=13
and e.cookie_id is not null then 1 else 0 end ) active_in_8_14,

max(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=7 
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=89
and e.is_paid=1 then 1 else 0 end ) purchased_in_8_90,

max(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=83 
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=89
and e.cookie_id is not null then 1 else 0 end ) active_in_84_90	
from
zybiro.bi_Sirui_newisntall_0511  a 
left join 
zybiro.bi_Sirui_dau_visit_cookie e  on    a.site_id=e.site_id  and a.cookie_id=e.cookie_id
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
a.os_version; 
 
 
create  table   zybiro.bi_Sirui_newinsatall_res_0512_d as
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

count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then d.goods_id end ) day5_prod_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then d.1st_cat end ) day5_cat1_num,

count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then d.goods_id end ) day6_prod_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then d.1st_cat end ) day6_cat1_num,


count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then d.goods_id end ) day7_prod_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then d.1st_cat end ) day7_cat1_num,


count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
and   datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2 then d.goods_id end ) day1_3_prod_num,

count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
and   datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2  then d.1st_cat end ) day1_3_cat1_num,


count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
and   datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6 then d.goods_id end ) day5_7_prod_num,

count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
and   datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4),substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6  then d.1st_cat end ) day5_7_cat1_num
from
zybiro.bi_Sirui_newisntall_0511  a 
left join 
 zybiro.bi_Sirui_cat_cookie d  on  a.site_id=d.site_id  and a.cookie_id=d.cookie_id
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
a.os_version;

 
 

------Combining sub-tables

drop  table    zybiro.bi_Sirui_newinsatall_res_0512;
create  table   zybiro.bi_Sirui_newinsatall_res_0512 as
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
a.day5_cart_num,
a.day5_search_num,
a.day5_event_num,
a.day5_detail_num,
a.day6_cart_num,
a.day6_search_num,
a.day6_event_num,
a.day6_detail_num,
a.day7_cart_num,
a.day7_search_num,
a.day7_event_num,
a.day7_detail_num,
a.day1_3_cart_num,
a.day1_3_search_num,
a.day1_3_event_num,
a.day1_3_detail_num,
a.day5_7_cart_num,
a.day5_7_search_num,
a.day5_7_event_num,
a.day5_7_detail_num,	
a.day1_7_event_num,
a.day8_14_event_num,
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
b.day5_time,
b.day5_pv,
b.day5_is_reg,
b.day5_order_num,
b.day5_order_value,
b.day6_time,
b.day6_pv,
b.day6_is_reg,
b.day6_order_num,
b.day6_order_value,
b.day7_time,
b.day7_pv,
b.day7_is_reg,
b.day7_order_num,
b.day7_order_value,
b.day1_3_time,
b.day1_3_pv,
b.day1_3_is_reg,
b.day1_3_order_num,
b.day1_3_order_value,
b.day5_7_time,
b.day5_7_pv, 
b.day5_7_is_reg,
b.day5_7_order_num,
b.day5_7_order_value,
b.purchased_in_8_14,
b.active_in_8_14,
b.purchased_in_8_90,
b.active_in_84_90,
c.day1_prod_num,
c.day1_cat1_num,
c.day2_prod_num,
c.day2_cat1_num,
c.day3_prod_num,
c.day3_cat1_num,
c.day4_prod_num,
c.day4_cat1_num,
c.day5_prod_num,
c.day5_cat1_num,
c.day6_prod_num,
c.day6_cat1_num,
c.day7_prod_num,
c.day7_cat1_num,
c.day1_3_prod_num,
c.day1_3_cat1_num,
c.day5_7_prod_num,
c.day5_7_cat1_num
	
from 
zybiro.bi_Sirui_newinsatall_res_0512_c  a 
left join 
zybiro.bi_Sirui_newinsatall_res_0512_e b   on  
      a.site_id=b.site_id
and a.device_id=b.device_id
and a.cookie_id=b.cookie_id
and a.data_installed=b.data_installed
and a.channel=b.channel
and a.source=b.source
and a.medium=b.medium 
and a.campaign=b.campaign
and a.platform=b.platform
and a.country=b.country
and a.area=b.area
and a.city=b.city
and a.ip=b.ip
and a.device_brand=b.device_brand
and a.device_model=b.device_model
and a.os_version=b.os_version
left join
zybiro.bi_Sirui_newinsatall_res_0512_d c  on 
a.site_id=c.site_id
and a.device_id=c.device_id
and a.cookie_id=c.cookie_id
and a.data_installed=c.data_installed
and a.channel=c.channel
and a.source=c.source
and a.medium=c.medium 
and a.campaign=c.campaign
and a.platform=c.platform
and a.country=c.country
and a.area=c.area
and a.city=c.city
and a.ip=c.ip
and a.device_brand=c.device_brand
and a.device_model=c.device_model
and a.os_version=c.os_version
group by
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
a.day5_cart_num,
a.day5_search_num,
a.day5_event_num,
a.day5_detail_num,
a.day6_cart_num,
a.day6_search_num,
a.day6_event_num,
a.day6_detail_num,
a.day7_cart_num,
a.day7_search_num,
a.day7_event_num,
a.day7_detail_num,
a.day1_3_cart_num,
a.day1_3_search_num,
a.day1_3_event_num,
a.day1_3_detail_num,
a.day5_7_cart_num,
a.day5_7_search_num,
a.day5_7_event_num,
a.day5_7_detail_num,	
a.day1_7_event_num,
a.day8_14_event_num,
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
b.day5_time,
b.day5_pv,
b.day5_is_reg,
b.day5_order_num,
b.day5_order_value,
b.day6_time,
b.day6_pv,
b.day6_is_reg,
b.day6_order_num,
b.day6_order_value,
b.day7_time,
b.day7_pv,
b.day7_is_reg,
b.day7_order_num,
b.day7_order_value,
b.day1_3_time,
b.day1_3_pv,
b.day1_3_is_reg,
b.day1_3_order_num,
b.day1_3_order_value,
b.day5_7_time,
b.day5_7_pv, 
b.day5_7_is_reg,
b.day5_7_order_num,
b.day5_7_order_value,
b.purchased_in_8_14,
b.active_in_8_14,
b.purchased_in_8_90,
b.active_in_84_90,
c.day1_prod_num,
c.day1_cat1_num,
c.day2_prod_num,
c.day2_cat1_num,
c.day3_prod_num,
c.day3_cat1_num,
c.day4_prod_num,
c.day4_cat1_num,
c.day5_prod_num,
c.day5_cat1_num,
c.day6_prod_num,
c.day6_cat1_num,
c.day7_prod_num,
c.day7_cat1_num,
c.day1_3_prod_num,
c.day1_3_cat1_num,
c.day5_7_prod_num,
c.day5_7_cat1_num;



---make the result table: bi_Sirui_newinsatall_res_0512

drop  table   zybiro.bi_Sirui_newinsatall_outflow_res;
create  table   zybiro.bi_Sirui_newinsatall_outflow_res as
select 
a.site_id,
a.device_id, 
a.cookie_id,
a.data_installed,
a.channel,
a.platform,
a.country,
a.area,
a.city,
a.ip,
a.device_brand, 
a.device_model,
a.os_version,
a.is_his_install,

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

nvl(day5_time,0) day5_time,
nvl(day5_pv,0) day5_pv,
nvl(day5_is_reg,0) day5_is_reg,
nvl(day5_cart_num,0) day5_cart_num,
nvl(day5_search_num,0) day5_search_num,
nvl(day5_event_num,0) day5_event_num,
nvl(day5_detail_num,0) day5_detail_num,
nvl(day5_prod_num,0) day5_prod_num,
nvl(day5_cat1_num,0) day5_cat1_num,
nvl(day5_order_num,0) day5_order_num,
nvl(day5_order_value,0) day5_order_value,

nvl(day6_time,0) day6_time,
nvl(day6_pv,0) day6_pv,
nvl(day6_is_reg,0) day6_is_reg,
nvl(day6_cart_num,0) day6_cart_num,
nvl(day6_search_num,0) day6_search_num,
nvl(day6_event_num,0) day6_event_num,
nvl(day6_detail_num,0) day6_detail_num,
nvl(day6_prod_num,0) day6_prod_num,
nvl(day6_cat1_num,0) day6_cat1_num,
nvl(day6_order_num,0) day6_order_num,
nvl(day6_order_value,0) day6_order_value,

nvl(day7_time,0) day7_time,
nvl(day7_pv,0) day7_pv,
nvl(day7_is_reg,0) day7_is_reg,
nvl(day7_cart_num,0) day7_cart_num,
nvl(day7_search_num,0) day7_search_num,
nvl(day7_event_num,0) day7_event_num,
nvl(day7_detail_num,0) day7_detail_num,
nvl(day7_prod_num,0) day7_prod_num,
nvl(day7_cat1_num,0) day7_cat1_num,
nvl(day7_order_num,0) day7_order_num,
nvl(day7_order_value,0) day7_order_value,

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

nvl(day5_7_time,0) day5_7_time,
nvl(day5_7_pv,0) day5_7_pv,
nvl(day5_7_is_reg,0) day5_7_is_reg,
nvl(day5_7_cart_num,0) day5_7_cart_num,
nvl(day5_7_search_num,0) day5_7_search_num,
nvl(day5_7_event_num,0) day5_7_event_num,
nvl(day5_7_detail_num,0) day5_7_detail_num,
nvl(day5_7_prod_num,0) day5_7_prod_num,
nvl(day5_7_cat1_num,0) day5_7_cat1_num,
nvl(day5_7_order_num,0) day5_7_order_num,
nvl(day5_7_order_value,0) day5_7_order_value,
nvl(purchased_in_8_14,0) purchased_in_8_14,
nvl(active_in_8_14,0) active_in_8_14,
case when     nvl(day8_14_event_num,0) /nvl(day1_7_event_num,0)<=0.5 then 1 else 0 end  activity_half_reduced_8_14,
nvl(purchased_in_8_90,0) purchased_in_8_90,
nvl(active_in_84_90,0) active_in_84_90
from
zybiro.bi_Sirui_newinsatall_res_0512 a;






/* create  table   zybiro.bi_Sirui_newinsatall_res_0511_2 as
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
-----day1
sum(case when e.data_date=a.data_installed then e.visit_time else 0 end ) day1_time,
sum(case when e.data_date=a.data_installed then e.page_views else 0 end ) day1_pv,
sum(case when e.data_date=a.data_installed  and e.is_reg=1 then 1 else 0 end ) day1_is_reg,
sum(case when c.data_date=a.data_installed then c.cart_num else 0 end ) day1_cart_num,
sum(case when c.data_date=a.data_installed then c.search_num else 0 end ) day1_search_num,
sum(case when c.data_date=a.data_installed then c.event_num else 0 end ) day1_event_num,
sum(case when c.data_date=a.data_installed then c.detail_num else 0 end ) day1_detail_num,
count(distinct case when d.data_date=a.data_installed then d.goods_id end ) day1_prod_num,
count(distinct case when d.data_date=a.data_installed then d.1st_cat end ) day1_cat1_num,
sum(case when e.data_date=a.data_installed then e.paid_orders else 0 end ) day1_order_num,
sum(case when e.data_date=a.data_installed then e.paid_order_amount else 0 end ) day1_order_value,
-----day2
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then e.visit_time else 0 end ) day2_time,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then e.page_views else 0 end ) day2_pv,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  and e.is_reg=1 then 1 else 0 end ) day2_is_reg,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then c.cart_num else 0 end ) day2_cart_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then c.search_num else 0 end ) day2_search_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then c.event_num else 0 end ) day2_event_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then c.detail_num else 0 end ) day2_detail_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then d.goods_id end ) day2_prod_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then d.1st_cat end ) day2_cat1_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1 then e.paid_orders else 0 end ) day2_order_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=1  then e.paid_order_amount else 0 end ) day2_order_value,
------day3
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then e.visit_time else 0 end ) day3_time,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then e.page_views else 0 end ) day3_pv,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  and e.is_reg=1 then 1 else 0 end ) day3_is_reg,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then c.cart_num else 0 end ) day3_cart_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then c.search_num else 0 end ) day3_search_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then c.event_num else 0 end ) day3_event_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then c.detail_num else 0 end ) day3_detail_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then d.goods_id end ) day3_prod_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then d.1st_cat end ) day3_cat1_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2 then e.paid_orders else 0 end ) day3_order_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=2  then e.paid_order_amount else 0 end ) day3_order_value,

--------day4
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then e.visit_time else 0 end ) day4_time,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then e.page_views else 0 end ) day4_pv,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  and e.is_reg=1 then 1 else 0 end ) day4_is_reg,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then c.cart_num else 0 end ) day4_cart_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then c.search_num else 0 end ) day4_search_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then c.event_num else 0 end ) day4_event_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then c.detail_num else 0 end ) day4_detail_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then d.goods_id end ) day4_prod_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then d.1st_cat end ) day4_cat1_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3 then e.paid_orders else 0 end ) day4_order_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=3  then e.paid_order_amount else 0 end ) day4_order_value,

--------day5
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then e.visit_time else 0 end ) day5_time,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then e.page_views else 0 end ) day5_pv,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  and e.is_reg=1 then 1 else 0 end ) day5_is_reg,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then c.cart_num else 0 end ) day5_cart_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then c.search_num else 0 end ) day5_search_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then c.event_num else 0 end ) day5_event_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then c.detail_num else 0 end ) day5_detail_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then d.goods_id end ) day5_prod_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then d.1st_cat end ) day5_cat1_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4 then e.paid_orders else 0 end ) day5_order_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=4  then e.paid_order_amount else 0 end ) day5_order_value,

--------day6
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then e.visit_time else 0 end ) day6_time,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then e.page_views else 0 end ) day6_pv,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  and e.is_reg=1 then 1 else 0 end ) day6_is_reg,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then c.cart_num else 0 end ) day6_cart_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then c.search_num else 0 end ) day6_search_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then c.event_num else 0 end ) day6_event_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then c.detail_num else 0 end ) day6_detail_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then d.goods_id end ) day6_prod_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then d.1st_cat end ) day6_cat1_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5 then e.paid_orders else 0 end ) day6_order_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=5  then e.paid_order_amount else 0 end ) day6_order_value,
--------day7
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then e.visit_time else 0 end ) day7_time,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then e.page_views else 0 end ) day7_pv,
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  and e.is_reg=1 then 1 else 0 end ) day7_is_reg,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then c.cart_num else 0 end ) day7_cart_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then c.search_num else 0 end ) day7_search_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then c.event_num else 0 end ) day7_event_num,
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then c.detail_num else 0 end ) day7_detail_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then d.goods_id end ) day7_prod_num,
count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then d.1st_cat end ) day7_cat1_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6 then e.paid_orders else 0 end ) day7_order_num,
sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )=6  then e.paid_order_amount else 0 end ) day7_order_value,
-----day1-day3
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
and   datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2
then e.visit_time else 0 end ) day1_3_time,

sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2
 then e.page_views else 0 end ) day1_3_pv,
 
max(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0 
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2
and e.is_reg=1 then 1 else 0 end ) day1_3_is_reg,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
and  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2
then c.cart_num else 0 end ) day1_3_cart_num,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
and    datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2
 then c.search_num else 0 end ) day1_3_search_num,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0 
and    datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2  then c.event_num else 0 end ) day1_3_event_num,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0 
and   datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2  then c.detail_num else 0 end ) day1_3_detail_num,

count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
and   datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2 then d.goods_id end ) day1_3_prod_num,

count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
and   datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2  then d.1st_cat end ) day1_3_cat1_num,

sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0 
and datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2
then e.paid_orders else 0 end ) day1_3_order_num,

sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=2  then e.paid_order_amount else 0 end ) day1_3_order_value,

-----day5-day7
sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
and   datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6
then e.visit_time else 0 end ) day5_7_time,

sum(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6
 then e.page_views else 0 end ) day5_7_pv,
 
max(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4 
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6
and e.is_reg=1 then 1 else 0 end ) day5_7_is_reg,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
and    datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6
then c.cart_num else 0 end ) day5_7_cart_num,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
and    datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6
 then c.search_num else 0 end ) day5_7_search_num,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4 
and    datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6  then c.event_num else 0 end ) day5_7_event_num,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4 
and   datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6  then c.detail_num else 0 end ) day5_7_detail_num,

count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
and   datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6 then d.goods_id end ) day5_7_prod_num,

count(distinct case when  datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
and   datediff(concat_ws('-',  substr(d.data_date,1,4), substr(d.data_date,5,2),substr(d.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6  then d.1st_cat end ) day5_7_cat1_num,

sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4 
and datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6
then e.paid_orders else 0 end ) day5_7_order_num,

sum(case when datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=4
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6  then e.paid_order_amount else 0 end ) day5_7_order_value,

--------True labels
			
max(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=7 
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=13
and e.is_paid=1 then 1 else 0 end ) purchased_in_8_14,

max(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=7 
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=13
and e.cookie_id is not null then 1 else 0 end ) active_in_8_14,
----maidian
sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=0 
and  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=6  then c.event_num else 0 end ) day1_7_event_num,

sum(case when  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=7 
and  datediff(concat_ws('-',  substr(c.data_date,1,4), substr(c.data_date,5,2),substr(c.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=13  then c.event_num else 0 end ) day8_14_event_num,

max(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=7 
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=89
and e.is_paid=1 then 1 else 0 end ) purchased_in_8_90,

max(case when  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )>=83 
and  datediff(concat_ws('-',  substr(e.data_date,1,4), substr(e.data_date,5,2),substr(e.data_date,7,2)) , concat_ws('-',  substr(a.data_installed,1,4), substr(a.data_installed,5,2),substr(a.data_installed,7,2)) )<=89
and e.cookie_id is not null then 1 else 0 end ) active_in_84_90	
from
zybiro.bi_Sirui_newisntall_0511  a 
left join 
dw.dw_cookie_first_install b on  a.site_id=b.site_id  and a.cookie_id=b.cookie_id 
left join 
zybiro.bi_Sirui_eventlog_cookie c on  a.site_id=c.siteid  and a.cookie_id=c.cookieid
left join 
 zybiro.bi_Sirui_cat_cookie d  on  a.site_id=d.site_id  and a.cookie_id=d.cookie_id
left join 
zybiro.bi_Sirui_dau_visit_cookie e  on    a.site_id=e.site_id  and a.cookie_id=e.cookie_id
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
case when b.cookie_id is null or  a.data_installed<=from_unixtime(unix_timestamp(b.first_install_time),'yyyyMMdd')  then 0 else 1 end ;
 */
 
 






