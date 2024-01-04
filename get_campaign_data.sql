
/*******************
* Campaign Data Template
* Estimated runtime: 20 mins
*******************/

-- Connection settings
USE ROLE UDW_MARKETING_ANALYTICS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_MARKETING_ANALYTICS_DEFAULT_WH_PROD;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;


-- set variables
SET reporting_vao = 113693;


/*******************
* Create Campaign Meta Table
*******************/
DROP TABLE IF EXISTS temp_table_campaign_meta;
CREATE TEMP TABLE temp_table_campaign_meta AS (

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

    -- get flight information for campaign
    ,flight_cte AS (
        SELECT DISTINCT
            c.sales_order_id
            ,flight.id AS flight_id
            ,flight.name AS flight_name
            ,flight.start_at_datetime::TIMESTAMP AS flight_start_datetime_utc
            ,flight.end_at_datetime::TIMESTAMP AS flight_end_datetime_utc
        FROM trader.flights_latest flight
            INNER JOIN campaign_cte c USING (campaign_id)
    )

    -- get campaign/flight/creative mapping
    ,campaign_flight_creative_cte AS (
        SELECT DISTINCT
            c.sales_order_id
            ,c.campaign_id
            ,fact.flight_id
            ,fact.creative_id
        FROM data_ad_xdevice.fact_delivery_event_without_pii fact
            INNER JOIN campaign_cte c USING (campaign_id)
        WHERE 
            fact.udw_partition_datetime BETWEEN (SELECT MIN(cp.cmpgn_start_datetime_utc) FROM campaign_cte cp) AND (SELECT MAX(cp.cmpgn_end_datetime_utc) FROM campaign_cte cp)
    )

    -- get creative data
    ,creative_cte AS (
        SELECT DISTINCT 
            cfc.sales_order_id
            ,c.id AS creative_id
            ,c.name AS creative_name
        FROM trader.creatives_latest c
            INNER JOIN campaign_flight_creative_cte cfc ON cfc.creative_id = c.id
    )


    -- final query          
    -- (Remember to edit the parts you want to keep in below as well!)
    SELECT DISTINCT
        -- VAO info
        vao.vao
        ,vao.samsung_campaign_id
        ,vao.sales_order_id
        ,vao.sales_order_name
        -- Sales Order info
        ,sales.order_start_date
        ,sales.order_end_date
        -- Campaign info
        ,campaign.campaign_id
        ,campaign.campaign_name
        ,campaign.rate_type
        ,campaign.net_unit_cost
        ,campaign.cmpgn_start_datetime_utc
        ,campaign.cmpgn_end_datetime_utc
        -- Flight info
        ,flight.flight_id
        ,flight.flight_name
        ,flight.flight_start_datetime_utc
        ,flight.flight_end_datetime_utc
        -- Creative info
        ,creative.creative_id
        ,creative.creative_name
    FROM vao_samsung_campaign_id_cte vao
        INNER JOIN sales_order_cte sales USING (sales_order_id)
        INNER JOIN campaign_cte campaign USING (sales_order_id)
        INNER JOIN flight_cte flight USING (sales_order_id)
        INNER JOIN campaign_flight_creative_cte USING (sales_order_id)
        INNER JOIN creative_cte creative USING (sales_order_id)
);

SELECT * FROM temp_table_campaign_meta;

