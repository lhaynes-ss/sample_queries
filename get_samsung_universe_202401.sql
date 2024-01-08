/*******************
* Get Samsung Universe 202401
* Estimated runtime: approx 6 mins
* Dependency: Campaign Meta
* Reference: https://adgear.atlassian.net/wiki/spaces/MAST/pages/19673186934/M+E+Analytics+-+A+I+Custom+Report+Methodology
*******************/

-- Connection settings
USE ROLE UDW_MARKETING_ANALYTICS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_MARKETING_ANALYTICS_DEFAULT_WH_PROD;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;



-- set variables
SET report_start_date_time = '2024-01-01 10:30:00'::TIMESTAMP;
SET report_end_date_time = '2024-01-01 12:30:00'::TIMESTAMP;
SET report_country = 'US';
SET reporting_vao = 113693;


/*******************
* DEPENDENCY: Test Campaign Meta Table
* The dependency logic has been truncated to reduce testing time
*******************/
DROP TABLE IF EXISTS campaign_meta;
CREATE TEMP TABLE campaign_meta AS (

    -- build table with CTE
    -- get vao/campaign IDs
    WITH vao_samsung_campaign_id_cte AS (
        SELECT
            sc.vao
            ,sc.samsung_campaign_id
            ,sc.sales_order_id
            ,sc.sales_order_name
        FROM (
            SELECT
                CAST(REPLACE(sf_opp.jira_id__c, 'VAO-', '') AS INT) AS vao
                ,sf_opp.samsung_campaign_id__c AS samsung_campaign_id
                ,sf_opp.operative_order_id__c AS sales_order_id
                ,sf_opp.order_name__c AS sales_order_name
                ,ROW_NUMBER() OVER(PARTITION BY vao ORDER BY sf_opp.lastmodifieddate DESC) AS row_num
            FROM salesforce.opportunity AS sf_opp
            WHERE 
                vao = $reporting_vao 
        ) sc
        WHERE sc.row_num = 1
    )
    
    -- get order information for campaign IDs previously selected
    ,sales_order_cte AS (
        SELECT
            so.sales_order_id
            ,so.sales_order_name
            ,so.order_start_date
            ,so.order_end_date
            ,so.time_zone
        FROM (
            SELECT
                sales_order.sales_order_id
                ,sales_order.sales_order_name
                ,sales_order.order_start_date
                ,sales_order.order_end_date
                ,sales_order.time_zone
                ,ROW_NUMBER() OVER(PARTITION BY sales_order.sales_order_id ORDER BY sales_order.last_modified_on) AS row_num
            FROM operativeone.sales_order AS sales_order
                INNER JOIN vao_samsung_campaign_id_cte AS vao USING (sales_order_id)
        ) AS so
        WHERE so.row_num = 1
    )

    -- get campaign data for campaign IDs previously selected 
    ,campaign_cte AS (
        SELECT DISTINCT
            tc.sales_order_id
            ,tc.sales_order_line_item_id
            ,cmpgn.id AS campaign_id
            ,cmpgn.name AS campaign_name
            ,tc.rate_type
            ,tc.net_unit_cost
            ,cmpgn.start_at_datetime::TIMESTAMP AS cmpgn_start_datetime_utc
            ,cmpgn.end_at_datetime::TIMESTAMP AS cmpgn_end_datetime_utc
        FROM trader.campaigns_latest AS cmpgn
            INNER JOIN ( -- get rate and cost info for campaign IDs previously selected
                SELECT DISTINCT
                    cmpgn_att.campaign_id
                    ,cmpgn_att.rate_type
                    ,cmpgn_att.net_unit_cost
                    ,cmpgn_att.io_external_id AS sales_order_id
                    ,cmpgn_att.li_external_id AS sales_order_line_item_id
                FROM trader.campaign_oms_attrs_latest cmpgn_att
                    INNER JOIN vao_samsung_campaign_id_cte sci ON sci.sales_order_id = cmpgn_att.external_id
            ) AS tc ON tc.campaign_id = cmpgn.id
    )



    -- final query          
    -- (Remember to edit the parts you want to keep in below as well!)
    SELECT DISTINCT
        -- Campaign info
        campaign.campaign_id
    FROM vao_samsung_campaign_id_cte vao
        INNER JOIN sales_order_cte sales USING (sales_order_id)
        INNER JOIN campaign_cte campaign USING (sales_order_id)
);

-- SELECT * FROM campaign_meta;





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
DROP TABLE IF EXISTS qualifier;
CREATE TEMP TABLE qualifier AS (
	-- depending on exchange seller, report date will either be the report date or report date - 30 days (noted as "Superset +30 days")
	-- https://adgear.atlassian.net/wiki/spaces/AGILE/pages/3651895321/Lift+Report+Methodology
	SELECT 
		LISTAGG(DISTINCT '"' || exchange_seller_id || '"', ',') AS exchage_seller_id_list
		,CASE 
			WHEN NOT exchage_seller_id_list LIKE ANY ('%"86"%', '%"88"%', '%"1"%', '%"256027"%', '%"237147"%', '%"escg8-6k2bc"%', '%"amgyk-mxvjr"%' ) 
			THEN 'Superset +30 days'
			ELSE 'Superset' 
		END AS qualifier
		,CASE 
			WHEN qualifier = 'Superset +30 days' 
			THEN DATEADD(DAY, -30, TO_DATE($report_start_date_time))::TIMESTAMP 
			ELSE $report_start_date_time
		END AS report_start_date
	FROM data_ad_xdevice.fact_delivery_event_without_pii a
		INNER JOIN campaign_meta b ON a.campaign_id = b.campaign_id
	WHERE 
		udw_partition_datetime BETWEEN $report_start_date_time AND $report_end_date_time
		AND type = 1
		AND device_country = $report_country
);

SET report_start_date_qual = (SELECT report_start_date FROM qualifier);

DROP TABLE IF EXISTS samsung_ue; 
CREATE TEMP TABLE samsung_ue AS (
	-- generate universe
	SELECT DISTINCT m.vtifa
	FROM profile_tv.fact_psid_hardware_without_pii a
		JOIN udw_lib.virtual_psid_tifa_mapping_v m ON a.psid_pii_virtual_id = m.vpsid
	WHERE 
		udw_partition_datetime BETWEEN $report_start_date_qual AND $report_end_date_time
		AND partition_country = $report_country	
	UNION
	SELECT DISTINCT GET(samsung_tvids_pii_virtual_id , 0) AS vtifa
	FROM data_ad_xdevice.fact_delivery_event_without_pii 	
	WHERE 
		udw_partition_datetime BETWEEN $report_start_date_qual AND $report_end_date_time
		AND type = 1
		AND (dropped != TRUE OR  dropped IS NULL)
		AND (exchange_id = 6 OR exchange_seller_id = 86)
		AND device_country = $report_country
	UNION 
	SELECT DISTINCT m.vtifa
	FROM data_tv_smarthub.fact_app_opened_event_without_pii a 
	JOIN udw_prod.udw_lib.virtual_psid_tifa_mapping_v m ON a.PSID_PII_VIRTUAL_ID = m.vpsid
	WHERE 
		udw_partition_datetime BETWEEN $report_start_date_qual AND $report_end_date_time
		AND partition_country = $report_country
);


SELECT * FROM samsung_ue;

