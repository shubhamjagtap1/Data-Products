------------------------------------------------Module Information------------------------------------------------
--  Purpose             :   CALLS AND CALL TYPES Data Product
--  Workstream          :   International Cloud Foundation(ICF)
--  Output Value        :   Can view G360 Calls at an ID, therapy area and detailed product level
--  Pre-requisites      :   Call Detail TDV layer
--  Last changed on     :   18 July 2024
--  Last changed by     :   Aditee Walia
--  Reason for change   :   Initial Commit
--  Author              :   Gilead Life Sciences
------------------------------------------------------------------------------------------------------------------------
--The main purpose of 'CALLS AND CALL TYPES' data product is to be able to view G360 Calls at an ID, date and detailed product level to easily see metrics like number of calls, number of products detailed, number of calls per TA and categorize whether a call is a promotional call or not. 
--Creating a Common Table Expression  called 'call_CTE' to get required attributes  from Call Detail TDVs
--Updated for medical calls data and created a CTE for fecthing samples and discussions Data
--ICF-2502 :Modified code(field_team_type) to fetch medical data

 with call_CTE as (
 select distinct {affiliate_name} as afl_nm
,{affiliate_code} as afl_id
,{country_code} as src_cntry_id
,{brick_name} as brik_nm
,{brick_id} as brik_id
,{affiliate_region_name} as regn_nm
,{salesforce_name} as sf_nm
,{state_name} as pri_st_cd
,{city_name} as pri_city
,{zip_post_code} as pri_zip
,{primary_address_line_1} as pri_addr_ln1
,{primary_address_line_2} as pri_addr_ln2
,{territory_name} as terr_nm
,{territory_id} as terr_id
,{unassigned_flag} as unasgn_flg
,{therapy_area_TA} as ta
,{product_name} as prod_nm
,{source_product_name} as src_prod_nm
,{source_account_id} as src_acnt_id
,{source_account_name} as src_acnt_nm
,{source_brick_id} as src_brik_id
,{source_brick_name} as src_brik_nm
,{source_master_account_id} as src_mastr_acnt_id
,{source_master_account_name} as src_mastr_acnt_nm
,{source_parent_brick_id} as src_prnt_brik_id
,{source_parent_brick_name} as src_prnt_brik_nm
,{source_user_id} as src_user_id
,{hcp_name} as cust_nm
,{hcp_eid} as cust_id
,{cegedim_id} as cegedim
,{veeva_id} as veeva_id
,{primary_phone_number} as pri_phn_num
,{master_account_id} as prnt_cust_id
,{master_account_name} as prnt_cust_nm
,{classification} as clsfn
,{source_classification} as src_clsfn
,{primary_specialty_code} as pri_spec_cd
,{credential} as cdtl
,{credential_code} as cdtl_cd
,{customer_speciality} as spec
,{customer_status} as cust_stat
,{national_professional_id} as ntnl_prfssnal
,{entity_class_code} as ety_clas_cd
,{entity_class_description} as ety_clas_desc
,{parent_entity_class_code} as prnt_ety_clas_cd
,{parent_entity_class_description} as prnt_ety_clas_desc
,{role} as role
,{parent_customer_status} as prnt_cust_stat
,{parent_customer_type} as prnt_cust_type
,{parent_practicioner_type} as prnt_pracr_type
,{parent_primary_address_line_1} as prnt_pri_addr_ln1
,{parent_primary_address_line_2} as prnt_pri_addr_ln2
,{parent_primary_city} as prnt_pri_city
,{parent_primary_country} as prnt_pri_cntry
,{parent_primary_phone_number} as prnt_pri_phn_num
,{parent_primary_specialty_code} as prnt_pri_spec_cd
,{call_date} as call_dt
,{rsm_name} as rsm_nm
,{ts_name} as ts_nm
,{call_id} as call_id
,{call_name} as call_nm
,{call_status} as call_stat
,{call_type} as call_typ
,{call_type_detail} as call_dtl_nm
,{call_type_detail_id} as call_dtl_id
,{interaction_type} as intractn_typ
,{parent_call_id} as prnt_call_id
,{is_parent_call} as is_prnt_cl
,{record_type} as rec_type
,{remote_call_method} as rmt_call_mthd
,{venue_ms} as venu_ms
,{video_call_software} as vid_call_sftwr
,{call_weightage} as wtg
,{field_team_type} as field_team_type 								  
,brd_nm from comm_pro_icf_publish.{regn}_fact_icf_alig_g360_call_dtl where upper({affiliate_code})='{afl_id}'  and upper(coalesce({therapy_area_TA},'UNASSIGNED_TA')) in {ta} 
 ),
--ICF-2381: Added block to fetch samples and discussions
Samples_And_discussion as 
(Select call_id as s_call_id,
src_prod_nm,
objection_gild_e__c,
ms_slide_decks_used_gild__c,
discussion_topics_gild__c,
call_discussion_name,
actions_gild__c,
indication__C,
comments__c,
quantity_vod__c,
Prod_type,
call_sample_name,
sample_name,
afl_id
from comm_icf_atlas.fact_icf_g360_align_calls_discussion_sample
where upper({affiliate_code})='{afl_id}'), 
 
 --Creating a CTE called 'cycle_CTE' to get cycle and segment priority attribute data from different regions
 cycle_sgmnt_CTE as (
 (select distinct 
 {cycle} AS spl_cyc,
 ta,
 afl_id,
 strt_dt,
 end_dt,
 {prioritization} as sgmnt_prty,
 cust_id from comm_pro_icf_publish.{regn}_fact_icf_alig_g360_cycle_plan_dtl where upper({affiliate_code})='{afl_id}'  and upper(coalesce({therapy_area_TA},'UNASSIGNED_TA')) in {ta})
  
 ),
 


-- Fetching the maximum time period start date and maximum time period end date of '4MC' bucket code to handle affiliates with 'CC' cycle not present in cycle plan fact table by creating 4 month cycle
df4mc_dt AS (
    SELECT MAX(TIME_PERD_STRT_DT) AS max_4mc_start_dt , MAX(TIME_PERD_END_DT) as max_4mc_end_dt FROM comm_pro_icf_publish.MAP_ICF_TIME_BCKT WHERE bckt_cd = '4MC' and UPPER(SBJCT_AREA) ='ACTIVITY'
),

-- Fetching the MAX maximum time period start date and maximum time period end date for Italy PC and PPC cycle
dfp4mc_dt AS (
    SELECT MAX(TIME_PERD_STRT_DT) AS max_p4mc_start_dt ,MAX(TIME_PERD_END_DT) AS max_p4mc_end_dt,CAST(MAX(TIME_PERD_STRT_DT) AS DATE) - INTERVAL '4' MONTH AS max_pp4mc_start_dt , CAST(MAX(TIME_PERD_END_DT) AS DATE) - INTERVAL '4' MONTH AS max_pp4mc_end_dt FROM comm_pro_icf_publish.MAP_ICF_TIME_BCKT WHERE bckt_cd = 'P4MC' and UPPER(SBJCT_AREA) ='ACTIVITY'
),



mtb_spl_cyc AS (
-- Fetching all affiliates except 'IT'; for Italy, we will generate PC and PPC from map time bucket and Fetching start and end date for Italy with 'CC' spl cycle
 SELECT DISTINCT afl_id, ta, spl_cyc, strt_dt, end_dt FROM cycle_sgmnt_CTE where afl_id!='IT' or (afl_id='IT' and spl_cyc='CC')
 UNION
-- Handling affiliates with 'CC' cycle not present in cycle plan fact table by creating 4 month cycle
 SELECT 
    A.afl_id, 
    A.ta, 
    'CC' AS spl_cyc, 
    CAST(MAX(df4mc_dt.max_4mc_start_dt) AS TIMESTAMP) AS TIME_PERD_STRT_DT,
    CAST(MAX(df4mc_dt.max_4mc_end_dt) AS TIMESTAMP) AS time_perd_end_dt
FROM 
    (SELECT DISTINCT TA, afl_id FROM call_CTE) A
CROSS JOIN 
    df4mc_dt
LEFT JOIN 
    (SELECT afl_id, ta FROM cycle_sgmnt_CTE WHERE spl_cyc = 'CC' GROUP BY 1, 2) cpt 
    ON A.afl_id = cpt.afl_id AND A.ta = cpt.ta
WHERE 
    cpt.afl_id IS NULL AND cpt.ta IS NULL
GROUP BY 
    A.afl_id, A.ta
 UNION
  -- Handling 'PC' cycle for 'IT' affiliates 

 SELECT  A.afl_id, A.ta, 'PC' AS spl_cyc,
    CAST(MAX(dfp4mc_dt.max_p4mc_start_dt) AS TIMESTAMP) AS TIME_PERD_STRT_DT,
    CAST(MAX(dfp4mc_dt.max_p4mc_end_dt) AS TIMESTAMP) AS time_perd_end_dt
    FROM (SELECT DISTINCT afl_id, ta FROM call_CTE WHERE afl_id = 'IT') A
    CROSS JOIN dfp4mc_dt
    GROUP BY A.afl_id, A.ta


 UNION
   -- Handling 'PPC' cycle for 'IT' affiliates 
 
 SELECT  A.afl_id, A.ta, 'PPC' AS spl_cyc,
    CAST(MAX(dfp4mc_dt.max_pp4mc_start_dt) AS TIMESTAMP) AS TIME_PERD_STRT_DT,
    CAST(MAX(dfp4mc_dt.max_pp4mc_end_dt) AS TIMESTAMP) AS time_perd_end_dt
    FROM (SELECT DISTINCT afl_id, ta FROM call_CTE WHERE afl_id = 'IT') A
    CROSS JOIN dfp4mc_dt
    GROUP BY A.afl_id, A.ta

  ),

 
 --Creating a CTE 'stg_hist_call' to fetch g360 product detailed attribute from staging hist tables
 --ICF-2427: Pulled entity_diplay_name to support coaching data
 stg_hist_call as (
 select  id, {source_g360_product_detailed} as Detailed_products_vod__c, entity_display_name_vod__c,
    CASE WHEN UPPER(clm_vod__c) = 'TRUE' THEN 1 
    WHEN UPPER(clm_vod__c) = 'FALSE' THEN 0 
    END AS is_clm_call from comm_pro_icf_publish.stg_icf_g360_call2_vod__c_hist 
 union all
 select id, {source_g360_product_detailed} as Detailed_products_vod__c, entity_display_name_vod__c,
    CASE WHEN UPPER(clm_vod__c) = 'TRUE' THEN 1
    WHEN UPPER(clm_vod__c) = 'FALSE' THEN 0
    END AS is_clm_call from comm_pro_icf_publish.stg_icf_jp_g360_call2_vod__c_hist 
 )
 ,
 


 --Determining the count of distinct cycle associated with each call_id, ensuring that in cases of overlapping dates in the cycle plan fact table, the call_id is linked to the cycle with the latest end date.
 DistinctSplCycCount AS (
    SELECT
        call_dtl.call_id,
        COUNT(DISTINCT cycle_plan_cyc.spl_cyc) AS distinct_spl_cyc_count
    FROM
        call_CTE call_dtl
    LEFT JOIN (
        SELECT DISTINCT spl_cyc, ta, afl_id, strt_dt, end_dt
        FROM mtb_spl_cyc
    ) cycle_plan_cyc ON cycle_plan_cyc.ta = call_dtl.ta
        AND cycle_plan_cyc.afl_id = call_dtl.afl_id
        AND call_dt BETWEEN cycle_plan_cyc.end_dt AND cycle_plan_cyc.strt_dt
    GROUP BY call_dtl.call_id
), 
 -- Creating a CTE 'CallDetailsCTE' to Select and transform data from various tables 
  CallDetailsCTE AS (
 select  /*+ BROADCAST(promotional_call_flag,cntry,terr,dim_icf_geo,cust,geography_reference,cycle_plan_sgmnt,cycle_plan_cyc,dim_geo) */
 geography_reference.level1_global_region_name as global_region_name,
 geography_reference.level2_sub_region_name as global_subregion_name,
 call_dtl.afl_nm as affiliate_name,
 call_dtl.afl_id as affiliate_code,
 cntry.cntry_nm as country_name,
 call_dtl.src_cntry_id as country_code,
 call_dtl.brik_nm as brick_name,
 call_dtl.brik_id as brick_id,
 call_dtl.regn_nm as affiliate_region_name,
 call_dtl.sf_nm as salesforce_name,
 call_dtl.pri_st_cd as state_name,
 call_dtl.pri_city as city_name,
 call_dtl.pri_zip as zip_post_code,
 call_dtl.pri_addr_ln1 as primary_address_line_1,
 call_dtl.pri_addr_ln2 as primary_address_line_2,
 cust.pri_addr_ln3 as primary_address_line_3,
 cust.pri_addr_ln4 as primary_address_line_4,
 call_dtl.terr_nm as territory_name,
 call_dtl.terr_id as territory_id,
 terr.terr_stat as territory_status,
 dim_geo.sub_terr_nm as affiliate_subterritory_name,
 dim_geo.sub_terr_id as affiliate_subterritory_id,
 call_dtl.unasgn_flg as unassigned_flag,
 dim_geo.subterritory_hiearachy_level_1 as subterritory_hiearachy_level_1,
 dim_geo.subterritory_hiearachy_level_2 as subterritory_hiearachy_level_2,
 dim_geo.subterritory_hiearachy_level_3 as subterritory_hiearachy_level_3,
 dim_geo.brick_hierarchy_level_1 as brick_hierarchy_level_1,
 dim_geo.brick_hierarchy_level_2 as brick_hierarchy_level_2,
 dim_geo.brick_hierarchy_level_3 as brick_hierarchy_level_3,
 call_dtl.ta as therapy_area_TA,
 call_dtl.prod_nm as product_name,
 call_dtl.src_prod_nm as source_product_name,
 '{data_source_name}' as data_source_name,
 call_dtl.src_acnt_id as source_account_id,
 call_dtl.src_acnt_nm as source_account_name,
 call_dtl.src_brik_id as source_brick_id,
 call_dtl.src_brik_nm as source_brick_name,
 call_dtl.src_mastr_acnt_id as source_master_account_id,
 call_dtl.src_mastr_acnt_nm as source_master_account_name,
 call_dtl.src_prnt_brik_id as source_parent_brick_id,
 call_dtl.src_prnt_brik_nm as source_parent_brick_name,
 call_dtl.src_user_id as source_user_id,
 call_dtl.cust_nm as hcp_name,
 call_dtl.cust_id as hcp_eid,
 call_dtl.cegedim as cegedim_id,
 call_dtl.veeva_id as veeva_id,
 call_dtl.pri_phn_num as primary_phone_number,
 call_dtl.prnt_cust_id as master_account_id,
 call_dtl.prnt_cust_nm as master_account_name,
 call_dtl.clsfn as classification,
 call_dtl.src_clsfn as source_classification,
 call_dtl.pri_spec_cd as primary_specialty_code,
 call_dtl.cdtl as credential,
 call_dtl.cdtl_cd as credential_code,
 call_dtl.spec as customer_speciality,
 call_dtl.cust_stat as customer_status,
 call_dtl.ntnl_prfssnal as national_professional_id,
 call_dtl.ety_clas_cd as entity_class_code,
 call_dtl.ety_clas_desc as entity_class_description,
 call_dtl.role as role,
 call_dtl.prnt_cust_nm as parent_customer_name,
 call_dtl.prnt_cust_id as parent_customer_id,
 call_dtl.prnt_cust_stat as parent_customer_status,
 call_dtl.prnt_cust_type as parent_customer_type,
 call_dtl.prnt_ety_clas_cd as parent_entity_class_code,
 call_dtl.prnt_ety_clas_desc as parent_entity_class_description,
 call_dtl.prnt_pracr_type as parent_practicioner_type,
 call_dtl.prnt_pri_addr_ln1 as parent_primary_address_line_1,
 call_dtl.prnt_pri_addr_ln2 as parent_primary_address_line_2,
 call_dtl.prnt_pri_city as parent_primary_city,
 call_dtl.prnt_pri_cntry as parent_primary_country,
 call_dtl.prnt_pri_phn_num as parent_primary_phone_number,
 call_dtl.prnt_pri_spec_cd as parent_primary_specialty_code,
 call_dtl.call_dt as call_date,
 cycle_plan_cyc.spl_cyc as cycle,
 call_dtl.rsm_nm as rsm_name,
 call_dtl.ts_nm as ts_name,
 cast('NULL' as varchar(10)) as ms_name,
 call_dtl.call_id as call_id,
 call_dtl.call_nm as call_name,
 call_dtl.call_stat as call_status,
 call_dtl.call_typ as call_type,
 call_dtl.call_dtl_nm as call_type_detail,
 call_dtl.call_dtl_id as call_type_detail_id,
 call_dtl.field_team_type as field_team_type,
 case when call_dtl.intractn_typ is null OR UPPER(call_dtl.intractn_typ) = 'NULL' THEN 'F2F'
 else call_dtl.intractn_typ end as interaction_type,
 call_dtl.prnt_call_id as parent_call_id,
 call_dtl.is_prnt_cl as is_parent_call,
 call_dtl.rec_type as record_type,
 call_dtl.rmt_call_mthd as remote_call_method,
 call_dtl.venu_ms as venue_ms,
 call_dtl.vid_call_sftwr as video_call_software,
 
 -- to calculate the overall_number_of_actual_calls , number_of_product_details, number_of_therapy_area_actual_calls metric assign a unique sequential number to each row within the partition
--When encountering overlapping dates in the cycle plan fact table, the call_id will be associated with the cycle featuring the latest end date.If there are no overlapping dates, the call_id will be attributed to the cycle specified in the cycle plan fact table.

 case when dsc.distinct_spl_cyc_count =1 then 
 ROW_NUMBER() OVER (PARTITION BY call_dtl.call_id ORDER BY call_dtl.call_id DESC) 
 else 
 ROW_NUMBER() OVER (PARTITION BY call_dtl.call_id ORDER BY cycle_plan_cyc.end_dt DESC) 
 end as RANKING_CALL,
  case when dsc.distinct_spl_cyc_count =1 then 
 ROW_NUMBER() OVER (PARTITION BY call_dtl.call_id, call_dtl.brd_nm ORDER BY call_dtl.call_id DESC)
 else 
 ROW_NUMBER() OVER (PARTITION BY call_dtl.call_id, call_dtl.brd_nm ORDER BY cycle_plan_cyc.end_dt DESC) 
 end as RANKING_PRODUCT,
   case when dsc.distinct_spl_cyc_count =1 then 
 ROW_NUMBER() OVER (PARTITION BY call_dtl.call_id, call_dtl.ta ORDER BY call_dtl.call_id DESC)
 else 
  ROW_NUMBER() OVER (PARTITION BY call_dtl.call_id, call_dtl.ta ORDER BY cycle_plan_cyc.end_dt DESC)
 end as RANKING_TA,
 call_dtl.wtg as call_weightage,
 
 CASE 
 WHEN call_dtl.src_prod_nm IS NULL THEN 'N'
 ELSE COALESCE(promotional_call_flag.promotional_call_flag, 'Y')
 END AS promotional_call_yn,
 stg_call_hist.Detailed_products_vod__c as source_g360_product_detailed,
 objection_gild_e__c, 
    ms_slide_decks_used_gild__c, 
    discussion_topics_gild__c, 
    call_discussion_name, 
    actions_gild__c, 
    indication__C, 
    comments__c, 
    quantity_vod__c, 
    prod_type, 
    call_sample_name,
    sample_name,
 stg_call_hist.entity_display_name_vod__c as entity_display_name_vod__c,
 stg_call_hist.is_clm_call
 
 FROM
 (SELECT * FROM call_CTE) call_dtl
 LEFT JOIN 
  -- Joining with country reference table to get country name
 (SELECT {country_name} as cntry_nm,cntry_id,afl_id FROM comm_cur_icf_staging.stg_icf_dim_cntry
        where pt_batch_id in (select max(pt_batch_id) from comm_cur_icf_staging.stg_icf_dim_cntry) ) cntry
 ON cntry.cntry_id = call_dtl.src_cntry_id
 and cntry.afl_id = call_dtl.afl_id
 LEFT JOIN DistinctSplCycCount dsc ON call_dtl.call_id = dsc.call_id
 LEFT JOIN

 -- Joining with customer information table to get address details. Applied row_number to handle duplicates

(
  SELECT 
    eid,
    {primary_address_line_3} as pri_addr_ln3,
    {primary_address_line_4} as pri_addr_ln4,
    ROW_NUMBER() OVER (PARTITION BY eid ORDER BY eid DESC) AS row_num
  FROM comm_pro_icf_publish.dim_icf_cust 
  WHERE UPPER(edw_actv_flg) = 'Y'
) cust ON cust.eid = call_dtl.cust_id AND cust.row_num = 1 
 
 
 LEFT JOIN 
 stg_hist_call stg_call_hist on stg_call_hist.id = call_dtl.call_id
 
 LEFT JOIN 
 --Joining with Employee Territory table to get status of the territory
 (SELECT distinct terr_id,ta,{territory_status} as terr_stat,afl_id FROM comm_pro_icf_publish.dim_icf_emp_terr where  UPPER(edw_actv_flg) = 'Y' and UPPER(GEO_LVL_CD)='TERRITORY' and upper({affiliate_code})='{afl_id}'  and upper(coalesce({therapy_area_TA},'UNASSIGNED_TA')) in {ta}) terr
 ON call_dtl.terr_id = terr.terr_id AND call_dtl.ta = terr.ta and call_dtl.afl_id = terr.afl_id
 
 --Joining with cycle CTE to get the cycle for the call
 LEFT JOIN 
 (select distinct spl_cyc,ta,afl_id,strt_dt,end_dt from mtb_spl_cyc) cycle_plan_cyc
 ON cycle_plan_cyc.ta = call_dtl.ta  AND cycle_plan_cyc.afl_id = call_dtl.afl_id
 and call_dt between cycle_plan_cyc.strt_dt and cycle_plan_cyc.end_dt
 
 LEFT JOIN Samples_And_discussion sam on call_dtl.call_id = sam.s_call_id 
      and call_dtl.src_prod_nm = sam.src_prod_nm
	  and call_dtl.afl_id='FR'
 
 LEFT JOIN 
 -- Joining with dim_icf_geo : contains brick to subterritory and brick to brick group mapping. 
 -- For affliate id != UKI region : fetching brick hierarchy and sub terr id and name
 (
  Select afl_id,brik_id,brik_nm,ta,{affiliate_subterritory_id} as sub_terr_id,{affiliate_subterritory_name} as sub_terr_nm,null as subterritory_hiearachy_level_1,
 null as subterritory_hiearachy_level_2,
 null as subterritory_hiearachy_level_3,
 {brick_hierarchy_level_1} as brick_hierarchy_level_1,
{brick_hierarchy_level_2} as brick_hierarchy_level_2,
 {brick_hierarchy_level_3} as brick_hierarchy_level_3 from comm_pro_icf_publish.dim_icf_geo  where hier_type ='GEOGRAPHY' and is_actv = 'Y' and afl_id !='UKI'
 
 union all 
 -- Subquery for UKI region with hierarchy mapping
 (Select distinct a.afl_id,a.brik_id,a.brik_nm,a.ta,a.sub_terr_id,a.sub_terr_nm,b.hier_lvl1 as subterritory_hiearachy_level_1,b.hier_lvl2 as subterritory_hiearachy_level_2,b.hier_lvl3 as subterritory_hiearachy_level_3,
 a.hier_lvl1 as brick_hierarchy_level_1,a.hier_lvl2 as brick_hierarchy_level_2,a.hier_lvl3 as brick_hierarchy_level_3 from ((Select  afl_id,brik_id,brik_nm,ta,{affiliate_subterritory_id} as sub_terr_id,{affiliate_subterritory_name} as sub_terr_nm,{brick_hierarchy_level_1} as hier_lvl1,{brick_hierarchy_level_2} as hier_lvl2,{brick_hierarchy_level_3} as hier_lvl3  from comm_pro_icf_publish.dim_icf_geo where 
  hier_type='ANALYSIS GROUP' and is_actv='Y' and afl_id ='UKI') a left join (Select  afl_id,brik_id,brik_nm,ta,{affiliate_subterritory_id} as sub_terr_id,{affiliate_subterritory_name} as sub_terr_nm,{subterritory_hiearachy_level_1} as hier_lvl1,{subterritory_hiearachy_level_2} as hier_lvl2,{subterritory_hiearachy_level_3} as hier_lvl3  from comm_pro_icf_publish.dim_icf_geo where  hier_type='GEOGRAPHY' and is_actv='Y' and afl_id ='UKI')b on   a.sub_terr_nm= b.sub_terr_nm  and a.ta=b.ta))  )dim_geo on dim_geo.brik_id=call_dtl.brik_id and dim_geo.afl_id= call_dtl.afl_id and dim_geo.ta= call_dtl.ta 
 -- Joining with semantic geography reference table for global_region_name, global_subregion_name details
 LEFT JOIN  (select distinct {global_region_name} as level1_global_region_name,{global_subregion_name} as level2_sub_region_name,affiliate_code,country_name from comm_icf_atlas.config_icf_semantic_geography_reference_table)  geography_reference on 
 geography_reference.affiliate_code=cntry.afl_id and
 UPPER(geography_reference.country_name) = UPPER(cntry.cntry_nm)
 -- Joining with configuration table to get promotional call flag for non-promotional products
 LEFT JOIN (select distinct source_product_name,{promotional_call_yn} as promotional_call_flag from comm_icf_atlas.config_icf_semantic_non_promotional_products)  promotional_call_flag on UPPER(promotional_call_flag.source_product_name) =UPPER(call_dtl.src_prod_nm)
 
 ),
 
 
 -- creating a 'fact_ims_cnt_per_plz' CTE to find distinct IMS landscape value for each ZIP for 'DE' affiliate
 fact_ims_cnt_per_plz AS (
 SELECT /*+ BROADCAST(icf_config_de_plz_mapping) */
 global_region_name,
 global_subregion_name,
 affiliate_name,
 affiliate_code,
 country_name,
 country_code,
 brick_name,
 brick_id,
 affiliate_region_name,
 salesforce_name,
 state_name,
 city_name,
 zip_post_code,
 primary_address_line_1,
 primary_address_line_2,
 primary_address_line_3,
 primary_address_line_4,
 territory_name,
 territory_id,
 territory_status,
 affiliate_subterritory_name,
 affiliate_subterritory_id,
 unassigned_flag,
 subterritory_hiearachy_level_1,
 subterritory_hiearachy_level_2,
 subterritory_hiearachy_level_3,
 brick_hierarchy_level_1,
 brick_hierarchy_level_2,
 brick_hierarchy_level_3,
 therapy_area_TA,
 product_name,
 source_product_name,
 data_source_name,
 source_account_id,
 source_account_name,
 source_brick_id,
 source_brick_name,
 source_master_account_id,
 source_master_account_name,
 source_parent_brick_id,
 source_parent_brick_name,
 source_user_id,
 hcp_name,
 hcp_eid,
 cegedim_id,
 veeva_id,
 primary_phone_number,
 master_account_id,
 master_account_name,
 classification,
 source_classification,
 primary_specialty_code,
 credential,
 credential_code,
 customer_speciality,
 customer_status,
 cycle_plan_sgmnt.sgmnt_prty as prioritization,
 national_professional_id,
 entity_class_code,
 entity_class_description,
 role,
 parent_customer_name,
 parent_customer_id,
 parent_customer_status,
 parent_customer_type,
 parent_entity_class_code,
 parent_entity_class_description,
 parent_practicioner_type,
 parent_primary_address_line_1,
 parent_primary_address_line_2,
 parent_primary_city,
 parent_primary_country,
 parent_primary_phone_number,
 parent_primary_specialty_code,
 call_date,
 case when cycle='CC' then 'current cycle' 
 when cycle='PC' then 'previous cycle' 
 when cycle='PPC' then 'previous to previous cycle' 
 else cycle end as cycle,
 rsm_name,
 ts_name,
 ms_name,
 call_id,
 call_name,
 call_status,
 call_type,
 call_type_detail,
 call_type_detail_id,
 field_team_type,
 interaction_type,
 parent_call_id,
 is_parent_call,
 record_type,
 remote_call_method,
 venue_ms,
 video_call_software,
 -- Assigning 1 to the first row within each 'call_id' partition, else 0
 CASE WHEN RANKING_CALL = 1 THEN 1 ELSE 0 END AS overall_number_of_actual_calls,
 -- Assigning 1 to the first row within each 'call_id' and 'brd_nm' partition, else 0
 CASE WHEN RANKING_PRODUCT = 1 THEN 1 ELSE 0 END AS number_of_product_details,
 -- Assigning 1 to the first row within each 'call_id' and 'ta' partition, else 0
 CASE WHEN RANKING_TA = 1 THEN 1 ELSE 0 END AS number_of_therapy_area_actual_calls,
 call_weightage,
 ims_cnt_per_plz.ims_cnt,
 promotional_call_yn,
 source_g360_product_detailed,
 objection_gild_e__c,
ms_slide_decks_used_gild__c,
discussion_topics_gild__c,
call_discussion_name,
actions_gild__c,
indication__C,
comments__c,
quantity_vod__c,
prod_type,
call_sample_name,
sample_name,
entity_display_name_vod__c,
is_clm_call
 FROM CallDetailsCTE  
 
 -- parametrized IMS landscape
 fact left join (select PLZ,count(distinct {ims_landscape_germany_only}) as ims_cnt,'DE' as afl_id from comm_pro_icf_publish.icf_config_de_plz_mapping group by 1) ims_cnt_per_plz
 on fact.zip_post_code=ims_cnt_per_plz.PLZ 
 and fact.affiliate_code=ims_cnt_per_plz.afl_id
  --Joining with cycle CTE to get the segment priority for the customer/prescribers
 LEFT JOIN 
 cycle_sgmnt_CTE cycle_plan_sgmnt
 ON cycle_plan_sgmnt.ta = fact.therapy_area_TA  AND cycle_plan_sgmnt.afl_id = fact.affiliate_code
 and  fact.cycle = cycle_plan_sgmnt.spl_cyc
 and cycle_plan_sgmnt.cust_id = fact.hcp_eid 
 )
 ,
 
 -- Creating a CTE 'call_data_product' to generate the final dataset with IMS landscape information
 
 call_data_product as (
 -- Selecting rows where IMS count is 1 and joining with PLZ configuration data for 'DE' affiliate. Records from the PLZ file which have a 1:1 match between zip and landscape are segregate and landscape value is fetched
 select /*+ BROADCAST(icf_config_de_plz_mapping) */ fact.*, plz_config.ims_landscape from (select * from fact_ims_cnt_per_plz where ims_cnt=1) fact left join (select distinct plz,{ims_landscape_germany_only} as ims_landscape,'DE' as afl_id from comm_pro_icf_publish.icf_config_de_plz_mapping) plz_config
 on trim(fact.zip_post_code)=trim(plz_config.PLZ)
 and trim(fact.affiliate_code)=trim(plz_config.afl_id)
 union all
 
 -- For cases where we do not have a 1:1 map for ZIP, take a combination of ZIP + City is taken and mapped with the combination of ZIP + City from the PLZ file to get the IMS Landscape
 select fact.*, plz_config.ims_landscape from (select * from fact_ims_cnt_per_plz where ims_cnt!=1) fact left join (select distinct plz,{ims_landscape_germany_only} as ims_landscape,ort,'DE' as afl_id from comm_pro_icf_publish.icf_config_de_plz_mapping) plz_config
 on trim(fact.zip_post_code)=trim(plz_config.PLZ) and  upper(trim(REGEXP_REPLACE(fact.city_name, ',.*', '')))=upper(trim(plz_config.ORT)) and trim(fact.affiliate_code)=trim(plz_config.afl_id) where plz_config.ims_landscape is not null 
 union all
 
 --For cases where we still do not get an IMS Landscape, take a combination of Zip + City is looked up against the combination of ZIP + Place in the PLZ file to get the IMS Landscape. Utilizing REGEXP_REPLACE to extract the first city name from comma-separated values in the city_name column
 select outer_fact.*,outer_plz_config.ims_landscape from  (select fact.* from (select * from fact_ims_cnt_per_plz where ims_cnt!=1) fact left join (select distinct plz,{ims_landscape_germany_only} as ims_landscape,ort,'DE' as afl_id from comm_pro_icf_publish.icf_config_de_plz_mapping) plz_config
 on trim(fact.zip_post_code)=trim(plz_config.PLZ) and  upper(trim(REGEXP_REPLACE(fact.city_name, ',.*', '')))=upper(trim(plz_config.ORT)) and trim(fact.affiliate_code)=trim(plz_config.afl_id) where plz_config.ims_landscape is null) outer_fact 
 left join (select distinct plz,{ims_landscape_germany_only} as ims_landscape,Ortkurz,'DE' as afl_id from comm_pro_icf_publish.icf_config_de_plz_mapping) outer_plz_config
 on trim(outer_fact.zip_post_code)=trim(outer_plz_config.PLZ) and upper(trim(REGEXP_REPLACE(outer_fact.city_name, ',.*', '')))=upper(trim(outer_plz_config.Ortkurz)) and trim(outer_fact.affiliate_code)=trim(outer_plz_config.afl_id)
 union all
 -- Combining results where IMS count is null
 select *,null as ims_landscape from fact_ims_cnt_per_plz where ims_cnt is null
 )
 
 --Selecting columns in final order
 select  
 global_region_name,
 global_subregion_name,
 affiliate_name,
 country_name,
 country_code,
 brick_name,
 brick_id,
 affiliate_region_name,
 salesforce_name,
 state_name,
 city_name,
 zip_post_code,
 primary_address_line_1,
 primary_address_line_2,
 primary_address_line_3,
 primary_address_line_4,
 territory_name,
 territory_id,
 territory_status,
 affiliate_subterritory_name,
 affiliate_subterritory_id,
 unassigned_flag,
 subterritory_hiearachy_level_1,
 subterritory_hiearachy_level_2,
 subterritory_hiearachy_level_3,
 brick_hierarchy_level_1,
 brick_hierarchy_level_2,
 brick_hierarchy_level_3,
 therapy_area_TA,
 product_name,
 source_g360_product_detailed,
 source_product_name,
 data_source_name,
 source_account_id,
 source_account_name,
 source_brick_id,
 source_brick_name,
 source_master_account_id,
 source_master_account_name,
 source_parent_brick_id,
 source_parent_brick_name,
 source_user_id,
 hcp_name,
 hcp_eid,
 cegedim_id,
 veeva_id,
 primary_phone_number,
 master_account_id,
 master_account_name,
 classification,
 source_classification,
 primary_specialty_code,
 credential,
 credential_code,
 customer_speciality,
 customer_status,
 prioritization,
 national_professional_id,
 entity_class_code,
 entity_class_description,
 role,
 ims_landscape as ims_landscape_germany_only,
 parent_customer_name,
 parent_customer_id,
 parent_customer_status,
 parent_customer_type,
 parent_entity_class_code,
 parent_entity_class_description,
 parent_practicioner_type,
 parent_primary_address_line_1,
 parent_primary_address_line_2,
 parent_primary_city,
 parent_primary_country,
 parent_primary_phone_number,
 parent_primary_specialty_code,
 call_date,
 cycle,
 rsm_name,
 ts_name,
 ms_name,
 call_id,
 call_name,
 call_status,
 call_type,
 call_type_detail,
 call_type_detail_id,
 field_team_type,
 interaction_type,
 parent_call_id,
 is_parent_call,
 record_type,
 remote_call_method,
 venue_ms,
 video_call_software,
 promotional_call_yn,
 overall_number_of_actual_calls,
 number_of_product_details,
 number_of_therapy_area_actual_calls,
 call_weightage,
 objection_gild_e__c,
	ms_slide_decks_used_gild__c,
	discussion_topics_gild__c,
	call_discussion_name,
	actions_gild__c,
	indication__C,
	comments__c,
	quantity_vod__c,
	prod_type,
	call_sample_name,
	sample_name,
	entity_display_name_vod__c,
 now() as last_data_refresh_timestamp,
 affiliate_code,
 is_clm_call
 from call_data_product
