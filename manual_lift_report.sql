/***********************
 DSP LIFT REPORT VALUES
***********************/
SET universe                    = 51125242;     --> Samsung Universe
SET conversion_audience         = 161802;       --> Total Conversion Audience
SET impressions                 = 22041267;     --> Impressions
SET reach                       = 9315124;      --> Reach
SET exposed_conversion_audience = 19020;        --> Exposed Conversion Audience


-- generate lift report
DROP TABLE IF EXISTS lift_report;
CREATE TEMP TABLE lift_report AS (

    -- get reach, frequency, and impressions
    WITH campaign_metrics AS (
        SELECT 
            $reach AS reach
            ,$impressions AS impressions
            ,CAST($impressions AS FLOAT)/reach AS frequency
    ), 

    -- get samsung universe counts
    -- get conversions
    audience_totals AS (
        SELECT 
            $universe AS total_universe 
            ,$conversion_audience AS total_converters
    )	

    SELECT 
        c.impressions
        ,c.reach
        ,c.frequency
        ,$exposed_conversion_audience AS exposed_converters
        ,CAST(exposed_converters AS FLOAT)/reach AS exposed_conversion_rate                     --> exposed_conversion_rate     = exposed_converters/reach
        ,a.total_universe - c.reach AS unexposed_audience                                       --> unexposed_audience          = total_universe - reach
        ,a.total_converters - exposed_converters AS unexposed_converters                        --> unexposed_converters        = total_converters - exposed_converters
        ,CAST(unexposed_converters AS FLOAT)/unexposed_audience AS unexposed_conversion_rate    --> unexposed_conversion_rate   = unexposed_converters/unexposed_audience
        ,exposed_conversion_rate/unexposed_conversion_rate - 1 AS lift                          --> lift                        = exposed_conversion_rate/unexposed_conversion_rate - 1
    FROM campaign_metrics c 
        JOIN audience_totals a ON 1 = 1  --> CROSS JOIN equivalent

);

SELECT * FROM lift_report;