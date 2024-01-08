/*******************
* Get Samsung Universe
* Estimated runtime: approx 2 mins
* !!! NOTE: THIS LOGIC HAS BEEN DEPRECATED !!!
*******************/

-- Connection settings
USE ROLE UDW_MARKETING_ANALYTICS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_MARKETING_ANALYTICS_DEFAULT_WH_PROD;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;



-- set variables
SET report_start_datetime = '2024-01-01 10:30:00'::TIMESTAMP;
SET report_end_datetime = '2024-01-01 12:30:00'::TIMESTAMP;
SET report_country = 'US';



/*******************
* Samsung Universe
* 
* Samsung Universe (aka. superset) is a collection of Samsung TVs that can be found in any of following 3 data sources:
*    - TV Hardware: profile_tv.fact_psid_hardware_without_pii
*    - App Open: data_tv_smarthub.fact_app_opened_event_without_pii
*    - O&O Samsung Ads Campaign Delivery: data_ad_xdevice.fact_delivery_event_without_pii (for exchange_id = 6 and exchange_seller_id = 86) 
*
* Any data used for attribution reports needs to be intersected with Samsung Universe
*******************/
-- create mapping table for psid/tifa
DROP TABLE IF EXISTS temp_table_psid_tifa_mapping;
CREATE TEMP TABLE temp_table_psid_tifa_mapping AS (
    SELECT DISTINCT
        m.psid
        ,m.vpsid
        ,m.tifa
        ,m.vtifa
    FROM (
        SELECT
            ptm.psid
            ,ptm.vpsid
            ,ptm.tifa
            ,ptm.vtifa
            ,ROW_NUMBER() OVER(PARTITION BY ptm.tifa ORDER BY ptm.psid DESC) AS row_num
        FROM udw_lib.virtual_psid_tifa_mapping_v ptm
    ) m
    WHERE 
        m.row_num = 1
);

-- build universe with mapping
DROP TABLE IF EXISTS temp_table_samsung_ue; 
CREATE TEMP TABLE temp_table_samsung_ue AS (
    -- TV Hardware
    SELECT DISTINCT 
        m.vtifa
    FROM profile_tv.fact_psid_hardware_without_pii p
        LEFT JOIN temp_table_psid_tifa_mapping m ON m.vpsid = p.psid_pii_virtual_id
    WHERE 
        p.udw_partition_datetime BETWEEN $report_start_datetime AND $report_end_datetime 
        AND p.partition_country = $report_country	
    UNION
    -- O&O Samsung Ads Campaign Delivery
    SELECT DISTINCT 
        COALESCE(GET(e.samsung_tvids_pii_virtual_id, 0), m.vtifa) AS vtifa 
    FROM data_ad_xdevice.fact_delivery_event_without_pii e	
        LEFT JOIN temp_table_psid_tifa_mapping m ON SPLIT_PART(e.samsung_tvids, ',', 1) = m.tifa
    WHERE 
        e.udw_partition_datetime BETWEEN $report_start_datetime AND $report_end_datetime 
        AND e.type = 1
        AND (e.dropped != TRUE OR e.dropped IS NULL)
        AND (e.exchange_id = 6 OR e.exchange_seller_id = 86)
        AND e.device_country = $report_country
    UNION 
    -- App Open
    -- app open table has upper case psid. upper case and lower case psid have different vpsid, but have the same vtifa
    SELECT DISTINCT 
        m.vtifa
    FROM data_tv_smarthub.fact_app_opened_event_without_pii a 
        LEFT JOIN udw_lib.virtual_psid_tifa_mapping_v m ON m.vpsid = a.psid_pii_virtual_id
    WHERE 
        a.udw_partition_datetime BETWEEN $report_start_datetime AND $report_end_datetime 
        AND a.partition_country = $report_country
);


SELECT * FROM temp_table_samsung_ue;


