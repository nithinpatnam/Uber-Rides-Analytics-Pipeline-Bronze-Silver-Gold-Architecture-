use silver;
-- (This Silver Schema Contains The Cleansed Data Which is some how Differenrt from the Raw Data which is held in the Bronze Schema)
-- (Here In this Silver Schema We convert the data and we perform certain operations on the Data whuch is in the Bronze Schema)


CREATE TABLE silver.uber_rides_cleansed (
    start_datetime DATETIME,
    end_datetime DATETIME,
    ride_category VARCHAR(20),
    start_location VARCHAR(255),
    stop_location VARCHAR(255),
    miles FLOAT,
    ride_purpose VARCHAR(100),
    duration_minutes INT,
    avg_speed_mph FLOAT,
    ride_date DATE,
    month INT,
    day INT,
    year INT,
    day_of_week INT,
    hour INT
);


INSERT INTO silver.uber_rides_cleansed
SELECT 
    STR_TO_DATE(`START_DATE*`, '%m/%d/%Y %H:%i') AS start_datetime, -- (In the dataset the date is in (-) seperate form and this converts into (/) seperated form)
    STR_TO_DATE(`END_DATE*`, '%m/%d/%Y %H:%i') AS end_datetime,-- (And we perform operations on the (/) seperated form only)
    
    -- (This is the Another Case where it converts the dat in the Category feild into uppercase and trims the data in the category feild)
   --  (The Trim function here is used as rmeove the extra spaces in the starting letter of the data and the ending letetr of the data)
    CASE 
        WHEN UPPER(TRIM(`CATEGORY*`)) IN ('BUSINESS', 'PERSONAL') THEN UPPER(TRIM(`CATEGORY*`))
        ELSE 'UNKNOWN'
    END AS ride_category,
    
    TRIM(`START*`) AS start_location,
    TRIM(`STOP*`) AS stop_location,
    COALESCE(`MILES*`, 0) AS miles,
-- (COALESCE is a function which takes 2 arguments as a input like (x,y) and accepts default x value and if the value of x is null means like 0 or NA then it considers the y argument)
    
-- (This is a Another Case Where it uses the "%" Symbol which is used to select the data by checking the all Stirng before and after the specified stirng)
--     (And there is another Symbol called "*" Which considers only the befor and after letters)
--     (Ex: There are names like patnam nithin and patnam madhusudhan and patnam varalakshmi
-- 		Then if we use like "%nithin%" then it returns patnam nithin only beacause there is only one data containing nithin
--         then if we use like "%patnam%" then it returns all the 3 names like  patnam nithin and patnam madhusudhan and patnam varalakshmi)
    CASE 
        WHEN `PURPOSE*` IS NULL OR TRIM(`PURPOSE*`) = '' THEN 'UNSPECIFIED'
        WHEN UPPER(TRIM(`PURPOSE*`)) LIKE '%MEAL%' THEN 'MEAL/ENTERTAINMENT'
        ELSE UPPER(TRIM(`PURPOSE*`))
    END AS ride_purpose,
    
--     (This is an Another Case Where it uses TIMESTAMPDIFF Method)
--  (This is a Method called TIMESTAMPDIFF which helps in retriving the difference between the 2 standard times by specifying the component like miniutes or hours or seconds)
-- (In the above context we used the start time and end time and specifying the Minutes Component which helps in retriving the difference between 2 times)
    TIMESTAMPDIFF(MINUTE, STR_TO_DATE(`START_DATE*`, '%m/%d/%Y %H:%i'), STR_TO_DATE(`END_DATE*`, '%m/%d/%Y %H:%i')) AS duration_minutes,
   
   
-- (This is an Another case Which helps in Calculating the average speed of the ride from the time and distance between source and destination)
-- (If the TIMESTAMPDIFF id equal to 0 then return 0 else calculate the average speed)
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, STR_TO_DATE(`START_DATE*`, '%m/%d/%Y %H:%i'), STR_TO_DATE(`END_DATE*`, '%m/%d/%Y %H:%i')) = 0 THEN 0
        ELSE (`MILES*` * 60) / NULLIF(TIMESTAMPDIFF(MINUTE, STR_TO_DATE(`START_DATE*`, '%m/%d/%Y %H:%i'), STR_TO_DATE(`END_DATE*`, '%m/%d/%Y %H:%i')), 0)
    END AS avg_speed_mph,
    
-- (These are the Some Specific Functions which performs several Operations)
    DATE(STR_TO_DATE(`START_DATE*`, '%m/%d/%Y %H:%i')) AS ride_date, -- (Returns the Date of the given date like given=06/02/2004 04:30 then it returns "06/02/2004" which is Date)
    MONTH(STR_TO_DATE(`START_DATE*`, '%m/%d/%Y %H:%i')) AS month, -- (Returns the Month of the given date like given=06/02/2004 04:30 then it returns "June" which is Month)
    DAY(STR_TO_DATE(`START_DATE*`, '%m/%d/%Y %H:%i')) AS day, -- (Returns the Day of the given date like given=06/02/2004 04:30 then it returns "02" which is day)
    YEAR(STR_TO_DATE(`START_DATE*`, '%m/%d/%Y %H:%i')) AS year, -- (Returns the Year of the given date like given=06/02/2004 04:30 then it returns "2004" which is year)
    DAYOFWEEK(STR_TO_DATE(`START_DATE*`, '%m/%d/%Y %H:%i')) AS day_of_week, -- (Returns the Code of the given date like given=06/02/2004 04:30 then it returns "1 or 2 or --" which is Code to calculate the day)
    HOUR(STR_TO_DATE(`START_DATE*`, '%m/%d/%Y %H:%i')) AS hour -- (Returns the Hours of the given date like given=06/02/2004 04:30 then it returns "04" which is Hour of the time)

FROM bronze.uber_rides_raw
WHERE `START_DATE*` != 'Totals'
  AND STR_TO_DATE(`START_DATE*`, '%m/%d/%Y %H:%i') IS NOT NULL;
 
-- (Used To Retrive the Data that includes the Cleansed Data by performing the Several operations on the Raw data) 
select * from silver.uber_rides_cleansed;


    