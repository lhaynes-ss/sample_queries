/*******************
* Loading data from s3
* Estimated runtime: < 1 min
*******************/

-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD;
USE DATABASE UDW_PROD;
USE SCHEMA PUBLIC;



-- Note: Te see stage info run: DESC STAGE adbiz_data.SAMSUNG_ADS_DATA_SHARE;


-- create a temp table to store csv data
DROP TABLE IF EXISTS place_mapping;
CREATE TEMP TABLE place_mapping (
    vao INT
    ,line_item_name varchar(556)
    ,camp_start timestamp
    ,camp_end timestamp
    ,campaign_name varchar(556)
    ,campaign_id INT
    ,flight_name varchar(556)
    ,flight_id INT
    ,creative_name varchar(556)
    ,creative_id INT
);

-- load csv data from s3 stage into temp table
COPY INTO place_mapping 
FROM @adbiz_data.SAMSUNG_ADS_DATA_SHARE/analytics/custom/vaughn/documentation/confluence_mapping_test.csv
        file_format = (format_name = adbiz_data.mycsvformat3);

-- select data from temp table
SELECT * 
FROM place_mapping;
